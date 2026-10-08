package kafkatest

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Member is a group consumer running in the background. It consumes
// everything from the start and commits after every poll.
type Member struct {
	Client *kgo.Client
	cancel context.CancelFunc
	done   chan struct{}
	dialer *killableDialer

	mu       sync.Mutex
	consumed int
}

// MemberOption configures StartMember.
type MemberOption func(*memberConfig)

type memberConfig struct {
	opts   []kgo.Opt
	commit bool
}

// WithOpts adds franz-go options, e.g. kgo.InstanceID for a static member.
func WithOpts(opts ...kgo.Opt) MemberOption {
	return func(c *memberConfig) { c.opts = append(c.opts, opts...) }
}

// NoCommit makes the member consume without committing.
func NoCommit() MemberOption { return func(c *memberConfig) { c.commit = false } }

// ConsumerProtocol makes the member use the KIP-848 consumer protocol.
func ConsumerProtocol() MemberOption {
	return WithOpts(kgo.ServerSideBalancer(), kgo.Balancers(kgo.RangeBalancer()))
}

// StartMember joins group, subscribed to topics, and polls until Stop or Kill.
func StartMember(brokers, group string, topics []string, options ...MemberOption) (*Member, error) {
	cfg := memberConfig{commit: true}
	for _, o := range options {
		o(&cfg)
	}
	dialer := &killableDialer{}
	opts := append([]kgo.Opt{
		kgo.SeedBrokers(brokers),
		kgo.Dialer(dialer.dial),
		kgo.ClientID("mqx-test-" + group),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.SessionTimeout(6 * time.Second),
		kgo.HeartbeatInterval(500 * time.Millisecond),
		kgo.FetchMaxWait(200 * time.Millisecond),
	}, cfg.opts...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("start member of %s: %w", group, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &Member{Client: cl, cancel: cancel, done: make(chan struct{}), dialer: dialer}
	go func() {
		defer close(m.done)
		for ctx.Err() == nil {
			fs := cl.PollFetches(ctx)
			m.mu.Lock()
			m.consumed += fs.NumRecords()
			m.mu.Unlock()
			if cfg.commit && fs.NumRecords() > 0 {
				_ = cl.CommitUncommittedOffsets(ctx)
			}
		}
	}()
	return m, nil
}

// Consumed returns how many records the member has polled.
func (m *Member) Consumed() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.consumed
}

// Stop leaves the group (dynamic members send LeaveGroup; static members
// do not, as with the Java client) and closes the client.
func (m *Member) Stop() {
	m.cancel()
	<-m.done
	m.Client.Close()
}

// Kill simulates an unclean shutdown such as a crashed process: every
// connection is cut and no new one can be dialed, so the member neither
// heartbeats nor leaves. The coordinator keeps it until its session times
// out (a static member stays until it is removed or rejoins).
func (m *Member) Kill() {
	m.dialer.kill()
	m.cancel()
	<-m.done
	go m.Client.Close() // its LeaveGroup cannot reach the broker any more
}

// killableDialer tracks connections so Kill can sever them all.
type killableDialer struct {
	mu     sync.Mutex
	conns  []net.Conn
	killed bool
}

func (d *killableDialer) dial(ctx context.Context, network, host string) (net.Conn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.killed {
		return nil, errors.New("member killed")
	}
	c, err := (&net.Dialer{Timeout: 10 * time.Second}).DialContext(ctx, network, host)
	if err != nil {
		return nil, err
	}
	d.conns = append(d.conns, c)
	return c, nil
}

func (d *killableDialer) kill() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.killed = true
	for _, c := range d.conns {
		_ = c.Close()
	}
}
