package tui

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Max2535/mqx/internal/config"
)

// withConfigFile saves the harness config, with a leading comment, to a temp
// file that context edits are written to, and returns its path.
func withConfigFile(h *harness) string {
	h.t.Helper()
	path := filepath.Join(h.t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("# my contexts\n"), 0o600); err != nil {
		h.t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		h.t.Fatal(err)
	}
	cfg.CurrentContext, cfg.Contexts = h.a.cfg.CurrentContext, h.a.cfg.Contexts
	if err := cfg.Save(path); err != nil {
		h.t.Fatal(err)
	}
	h.a.cfg, h.a.path, h.a.store = cfg, path, &contextStore{cfg: cfg, path: path}
	return path
}

func loadFile(t *testing.T, path string) (*config.Config, string) {
	t.Helper()
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return cfg, string(data)
}

// focusField tabs through the open form until the field with key has focus.
func focusField(h *harness, key string) {
	h.t.Helper()
	f, ok := h.a.overlay.(*form)
	if !ok {
		h.t.Fatalf("overlay is %T, not a form", h.a.overlay)
	}
	for range f.fields {
		if f.fields[f.focus].key == key {
			return
		}
		h.keys("tab")
	}
	h.t.Fatalf("form has no field %q", key)
}

func TestContextTestConnection(t *testing.T) {
	_, dev := fakeCtx(t, "dev", false, kafkaCaps)
	broken, bad := fakeCtx(t, "broken", false, kafkaCaps)
	broken.OpenErr = errors.New("dial tcp: connection refused")
	h := newHarness(t, Options{}, dev, bad)

	h.keys("c")
	h.wantView("Contexts", "* dev (default)", "broken")
	h.keys("t")
	h.wantView("✓ dev reachable", "15 capabilities")
	h.keys("down", "t")
	h.wantView(`✕ context "broken": connect to fake: dial tcp: connection refused`)
	if h.a.openName() != "dev" {
		t.Errorf("testing must not switch context; open = %q", h.a.openName())
	}
}

func TestContextCreate(t *testing.T) {
	_, dev := fakeCtx(t, "dev", false, kafkaCaps)
	h := newHarness(t, Options{}, dev)
	path := withConfigFile(h)

	h.keys("c", "n")
	h.wantView("New context", "broker")
	h.keys("enter")
	h.wantView("New fake context", "name")
	h.typeText("staging")

	// Validation runs before saving: the fake needs options.instance.
	h.keys("ctrl+s")
	h.wantView("fake needs options.instance")
	if cfg, _ := loadFile(t, path); len(cfg.Contexts) != 1 {
		t.Fatalf("an invalid context was saved: %+v", cfg.Contexts)
	}

	// A secret pasted into an env field is refused.
	focusField(h, "password_env")
	h.typeText("hunter2!")
	h.keys("ctrl+s")
	h.wantView("must be the name of an environment variable")
	for range "hunter2!" {
		h.keys("backspace")
	}
	h.typeText("STAGING_PASS")

	focusField(h, "options")
	h.typeText("instance=" + dev.Options["instance"])
	focusField(h, "read_only")
	h.keys(" ")
	h.keys("ctrl+s")

	h.wantStatus("saved context staging", false)
	h.wantView("Contexts", "staging", "read-only")
	cfg, data := loadFile(t, path)
	got, err := cfg.Find("staging")
	if err != nil {
		t.Fatalf("not saved: %v\n%s", err, data)
	}
	if !got.ReadOnly || got.PasswordEnv != "STAGING_PASS" || got.Broker != "fake" {
		t.Errorf("saved context = %+v", got)
	}
	if !strings.HasPrefix(data, "# my contexts\n") {
		t.Errorf("comment lost:\n%s", data)
	}

	// The new context opens like any other.
	t.Setenv("STAGING_PASS", "x")
	h.keys("enter")
	h.wantView("ctx: staging", "READ-ONLY")
}

func TestContextEditOpenReconnects(t *testing.T) {
	_, dev := fakeCtx(t, "dev", false, kafkaCaps)
	h := newHarness(t, Options{}, dev)
	path := withConfigFile(h)
	h.wantNotView("READ-ONLY")

	h.keys("c", "e")
	h.wantView("Edit context dev (fake)")
	focusField(h, "name")
	for range "dev" {
		h.keys("backspace")
	}
	h.typeText("dev-ro")
	focusField(h, "read_only")
	h.keys("right", "ctrl+s")

	h.wantStatus("connected to dev-ro", false) // reopened with the new settings
	h.keys("esc")
	h.wantView("ctx: dev-ro", "READ-ONLY")
	cfg, _ := loadFile(t, path)
	if cfg.CurrentContext != "dev-ro" || len(cfg.Contexts) != 1 || !cfg.Contexts[0].ReadOnly {
		t.Errorf("saved = %+v", cfg)
	}
}

func TestContextDeleteAndDefault(t *testing.T) {
	_, dev := fakeCtx(t, "dev", false, kafkaCaps)
	_, prod := fakeCtx(t, "prod", true, kafkaCaps)
	h := newHarness(t, Options{}, dev, prod)
	path := withConfigFile(h)

	h.keys("c", "down", "u")
	h.wantStatus("default context is now prod", false)
	h.wantView("prod (default)")
	if cfg, _ := loadFile(t, path); cfg.CurrentContext != "prod" {
		t.Errorf("current-context = %q, want prod", cfg.CurrentContext)
	}

	// Deleting asks first; the default answer is Cancel.
	h.keys("up", "d")
	h.wantView(`Delete context "dev"?`, "It is open now")
	h.keys("enter")
	h.wantStatus("cancelled", false)
	if cfg, _ := loadFile(t, path); len(cfg.Contexts) != 2 {
		t.Fatal("cancel deleted the context")
	}

	h.keys("c", "d", "y")
	h.wantStatus("deleted context dev", false)
	if h.a.e != nil {
		t.Error("the deleted context is still open")
	}
	cfg, _ := loadFile(t, path)
	if len(cfg.Contexts) != 1 || cfg.Contexts[0].Name != "prod" || cfg.CurrentContext != "prod" {
		t.Errorf("saved = %+v", cfg)
	}
	h.keys("esc")
	h.wantView("No context open")
}

func TestContextEditWithoutFile(t *testing.T) {
	_, dev := fakeCtx(t, "dev", false, kafkaCaps)
	h := newHarness(t, Options{}, dev)
	h.keys("c", "u")
	h.wantStatus("no config file to save to", true)
}

func TestContextFirstBecomesDefault(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	cfg, _, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{t: t, a: newApp(appConfig{cfg: cfg, path: path, now: newClock().now})}
	h.run(h.a.Init())
	h.keys("c")
	h.wantView("No contexts yet")
	h.keys("n", "enter")
	h.typeText("only")
	focusField(h, "options")
	h.typeText("instance=x")
	h.keys("ctrl+s")
	h.wantView("only (default)")
	if saved, _ := loadFile(t, path); saved.CurrentContext != "only" {
		t.Errorf("current-context = %q, want only", saved.CurrentContext)
	}
}

func TestLoadConfigMissingFileIsEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "absent.yaml")
	cfg, got, err := loadConfig(path)
	if err != nil || got != path || len(cfg.Contexts) != 0 {
		t.Fatalf("loadConfig() = %+v, %q, %v", cfg, got, err)
	}
}
