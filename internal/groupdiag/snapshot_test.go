package groupdiag

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Max2535/mqx/internal/broker"
	"github.com/Max2535/mqx/internal/testutil/fakebroker"
)

func TestCollect(t *testing.T) {
	f, c := fakebroker.New(t)
	f.Now = func() time.Time { return t0 }
	f.AddTopic("orders", 3)
	f.AddTopic("other", 1)
	f.AddMessages("orders", 0, broker.NewMessage([]byte("a")), broker.NewMessage([]byte("b")))
	f.AddGroup(broker.GroupDescription{Name: "g", State: "Stable", Members: []broker.GroupMember{
		member("m1", []string{"orders"}, parts("orders", 0, 1, 2)),
	}}, map[string]map[int32]int64{"orders": {0: 1}})
	b, err := broker.Open(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	s, err := Collect(context.Background(), b, "g")
	if err != nil {
		t.Fatal(err)
	}
	if !s.At.Equal(t0) || s.Group.Name != "g" || len(s.Lag) != 1 || s.Lag[0].Lag != 1 {
		t.Errorf("snapshot = %+v", s)
	}
	if !reflect.DeepEqual(s.Partitions, map[string]int{"orders": 3}) {
		t.Errorf("partitions = %v", s.Partitions)
	}
	if _, err := Collect(context.Background(), b, "missing"); !errors.Is(err, broker.ErrNotFound) {
		t.Errorf("missing group: err = %v", err)
	}
	f.Caps = []string{broker.CapGroupInspector}
	if _, err := Collect(context.Background(), b, "g"); !errors.Is(err, broker.ErrUnsupported) {
		t.Errorf("without lag capability: err = %v", err)
	}
}
