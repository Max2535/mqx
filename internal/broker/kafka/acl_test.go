package kafka

import (
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/Max2535/mqx/internal/broker"
)

func TestParseACLEnums(t *testing.T) {
	tests := []struct {
		name    string
		acl     broker.ACL
		want    aclEnums
		wantErr string
	}{
		{name: "empty filter is any", want: aclEnums{kmsg.ACLResourceTypeAny, kmsg.ACLResourcePatternTypeAny, kmsg.ACLOperationAny, kmsg.ACLPermissionTypeAny}},
		{
			name: "case insensitive with separators",
			acl:  broker.ACL{ResourceType: "Transactional-ID", PatternType: "PREFIXED", Operation: "describe_configs", Permission: "Deny"},
			want: aclEnums{kmsg.ACLResourceTypeTransactionalId, kmsg.ACLResourcePatternTypePrefixed, kmsg.ACLOperationDescribeConfigs, kmsg.ACLPermissionTypeDeny},
		},
		{name: "bad resource", acl: broker.ACL{ResourceType: "queue"}, wantErr: "use topic, group, cluster"},
		{name: "bad operation", acl: broker.ACL{Operation: "fly"}, wantErr: `operation "fly"`},
		{name: "bad permission", acl: broker.ACL{Permission: "maybe"}, wantErr: "use allow or deny"},
		{name: "bad pattern", acl: broker.ACL{PatternType: "regex"}, wantErr: "use literal or prefixed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseACLEnums(tt.acl)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("parseACLEnums() = %+v, %v; want %+v", got, err, tt.want)
			}
		})
	}
}

func TestPartitioner(t *testing.T) {
	tp := newPartitioner().ForTopic("t")
	explicit := &kgo.Record{Partition: 2, Key: []byte("k")}
	if got := tp.Partition(explicit, 3); got != 2 || !tp.RequiresConsistency(explicit) {
		t.Errorf("explicit partition = %d (consistent %v), want 2", got, tp.RequiresConsistency(explicit))
	}
	keyed := &kgo.Record{Partition: -1, Key: []byte("same")}
	first := tp.Partition(keyed, 6)
	for range 5 {
		if got := tp.Partition(keyed, 6); got != first {
			t.Fatalf("keyed record moved from %d to %d", first, got)
		}
	}
}
