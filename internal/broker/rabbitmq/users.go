package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/Max2535/mqx/internal/broker"
)

// tagList decodes user tags, a comma-separated string before RabbitMQ 3.9 and a list since.
type tagList []string

func (t *tagList) UnmarshalJSON(data []byte) error {
	var list []string
	if err := json.Unmarshal(data, &list); err == nil {
		*t = list
		return nil
	}
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("decode user tags: %w", err)
	}
	*t = nil
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			*t = append(*t, part)
		}
	}
	return nil
}

// Users lists users and their tags; password hashes are never returned.
func (r *RabbitMQ) Users(ctx context.Context) ([]broker.User, error) {
	var us []struct {
		Name string  `json:"name"`
		Tags tagList `json:"tags"`
	}
	if err := r.mgmt.get(ctx, apiPath("users"), &us); err != nil {
		return nil, fmt.Errorf("list users: %w", err)
	}
	out := make([]broker.User, len(us))
	for i, u := range us {
		tags := []string(u.Tags)
		if tags == nil {
			tags = []string{}
		}
		out[i] = broker.User{Name: u.Name, Tags: tags}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// PutUser creates or updates a user. An empty password keeps an existing
// user's password (only the tags change); a new user without a password
// cannot log in with one.
func (r *RabbitMQ) PutUser(ctx context.Context, u broker.User, password string) error {
	if u.Name == "" {
		return errors.New("user name is required")
	}
	body := map[string]any{"tags": strings.Join(u.Tags, ",")}
	if password != "" {
		body["password"] = password
	} else {
		// A PUT without a password clears it, so carry the current hash over.
		hash, err := r.passwordHash(ctx, u.Name)
		if err != nil {
			return fmt.Errorf("put user %q: %w", u.Name, err)
		}
		if hash != nil {
			body["password_hash"] = hash.PasswordHash
			if hash.HashingAlgorithm != "" {
				body["hashing_algorithm"] = hash.HashingAlgorithm
			}
		}
	}
	if err := r.mgmt.put(ctx, apiPath("users", u.Name), body); err != nil {
		return fmt.Errorf("put user %q: %w", u.Name, err)
	}
	return nil
}

type passwordHash struct {
	PasswordHash     string `json:"password_hash"`
	HashingAlgorithm string `json:"hashing_algorithm"`
}

// passwordHash returns an existing user's password hash, or nil when the user
// does not exist or has no password. RabbitMQ before 4.4 returns the hash from
// /api/users/{name}; 4.4 only reports has_password there, so the hash is read
// from the definitions export instead. It fails rather than return nil for a
// user whose password it cannot read, so a tag change never clears a password.
func (r *RabbitMQ) passwordHash(ctx context.Context, name string) (*passwordHash, error) {
	var cur struct {
		passwordHash
		HasPassword *bool `json:"has_password"`
	}
	err := r.mgmt.get(ctx, apiPath("users", name), &cur)
	switch {
	case errors.Is(err, errNotFound):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("read current password hash: %w", err)
	case cur.PasswordHash != "":
		return &cur.passwordHash, nil
	case cur.HasPassword != nil && !*cur.HasPassword:
		return nil, nil
	}
	var defs struct {
		Users []struct {
			Name string `json:"name"`
			passwordHash
		} `json:"users"`
	}
	if err := r.mgmt.get(ctx, apiPath("definitions"), &defs); err != nil {
		return nil, fmt.Errorf("read current password hash from definitions: %w", err)
	}
	for _, u := range defs.Users {
		if u.Name == name && u.PasswordHash != "" {
			return &u.passwordHash, nil
		}
	}
	return nil, fmt.Errorf("cannot read the current password of %q, so changing only its tags would clear it; pass a password", name)
}

// DeleteUser deletes a user.
func (r *RabbitMQ) DeleteUser(ctx context.Context, name string) error {
	if err := r.mgmt.delete(ctx, apiPath("users", name), nil); err != nil {
		return fmt.Errorf("delete user %q: %w", name, err)
	}
	return nil
}

// PutVHost creates or updates a virtual host.
func (r *RabbitMQ) PutVHost(ctx context.Context, v broker.VHost) error {
	if v.Name == "" {
		return errors.New("vhost name is required")
	}
	body := map[string]any{"description": v.Description, "tracing": v.Tracing}
	if err := r.mgmt.put(ctx, apiPath("vhosts", v.Name), body); err != nil {
		return fmt.Errorf("put vhost %q: %w", v.Name, err)
	}
	return nil
}

// DeleteVHost deletes a virtual host with everything in it.
func (r *RabbitMQ) DeleteVHost(ctx context.Context, name string) error {
	if err := r.mgmt.delete(ctx, apiPath("vhosts", name), nil); err != nil {
		return fmt.Errorf("delete vhost %q: %w", name, err)
	}
	return nil
}

// Permissions lists every user's permissions on every vhost.
func (r *RabbitMQ) Permissions(ctx context.Context) ([]broker.Permission, error) {
	var ps []broker.Permission
	if err := r.mgmt.get(ctx, apiPath("permissions"), &ps); err != nil {
		return nil, fmt.Errorf("list permissions: %w", err)
	}
	sort.Slice(ps, func(i, j int) bool {
		if ps[i].VHost != ps[j].VHost {
			return ps[i].VHost < ps[j].VHost
		}
		return ps[i].User < ps[j].User
	})
	return ps, nil
}

// SetPermission sets a user's permissions on a vhost (the context's vhost when empty).
func (r *RabbitMQ) SetPermission(ctx context.Context, p broker.Permission) error {
	if p.User == "" {
		return errors.New("permission user is required")
	}
	vhost := r.vhostOr(p.VHost)
	body := map[string]string{"configure": p.Configure, "write": p.Write, "read": p.Read}
	if err := r.mgmt.put(ctx, apiPath("permissions", vhost, p.User), body); err != nil {
		return fmt.Errorf("set permissions of %q on vhost %q: %w", p.User, vhost, err)
	}
	return nil
}

// ClearPermission removes a user's permissions on a vhost (the context's vhost when empty).
func (r *RabbitMQ) ClearPermission(ctx context.Context, user, vhost string) error {
	vhost = r.vhostOr(vhost)
	if err := r.mgmt.delete(ctx, apiPath("permissions", vhost, user), nil); err != nil {
		return fmt.Errorf("clear permissions of %q on vhost %q: %w", user, vhost, err)
	}
	return nil
}

func (r *RabbitMQ) vhostOr(v string) string {
	if v == "" {
		return r.settings.vhost
	}
	return v
}
