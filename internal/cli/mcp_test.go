package cli

import (
	"bufio"
	"encoding/json"
	"io"
	"strings"
	"testing"
)

func TestMCPServesToolsOverStdio(t *testing.T) {
	e := newFakeEnv(t, true)
	e.f.AddTopic("orders", 1)

	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	cmd := NewRootCmd(withTerminal(false))
	cmd.SetIn(inR)
	cmd.SetOut(outW)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--config", e.path, "mcp"})
	done := make(chan error, 1)
	go func() {
		err := cmd.Execute()
		_ = outW.Close()
		done <- err
	}()

	send := func(line string) {
		t.Helper()
		if _, err := io.WriteString(inW, line+"\n"); err != nil {
			t.Fatal(err)
		}
	}
	// Every stdout line must be a JSON-RPC message: nothing else may write there.
	out := bufio.NewScanner(outR)
	reply := func(id float64) map[string]any {
		t.Helper()
		for out.Scan() {
			var msg map[string]any
			if err := json.Unmarshal(out.Bytes(), &msg); err != nil {
				t.Fatalf("stdout line is not JSON-RPC: %q", out.Text())
			}
			if msg["id"] == id {
				res, _ := msg["result"].(map[string]any)
				if res == nil {
					t.Fatalf("reply %v has no result: %v", id, msg)
				}
				return res
			}
		}
		t.Fatalf("no reply %v: %v", id, out.Err())
		return nil
	}

	send(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"0"}}}`)
	if s, _ := reply(1)["instructions"].(string); !strings.Contains(s, `"fake" (fake, read_only`) {
		t.Fatalf("instructions = %q", s)
	}
	send(`{"jsonrpc":"2.0","method":"notifications/initialized"}`)
	send(`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"list_topics","arguments":{}}}`)
	res := reply(2)
	if body, _ := json.Marshal(res); res["isError"] == true || !strings.Contains(string(body), "orders") {
		t.Fatalf("tools/call result = %s", body)
	}

	// Closing stdin ends the server cleanly.
	_ = inW.Close()
	go func() { _, _ = io.Copy(io.Discard, outR) }()
	if err := <-done; err != nil {
		t.Fatalf("mcp exited with %v", err)
	}
}
