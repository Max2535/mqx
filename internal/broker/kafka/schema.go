package kafka

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/sr"

	"github.com/Max2535/mqx/internal/broker"
	"github.com/Max2535/mqx/internal/config"
)

// registryClient wraps the Schema Registry client and implements schemaLookup.
type registryClient struct{ cl *sr.Client }

func newRegistryClient(url string, creds config.Credentials) (*registryClient, error) {
	opts := []sr.ClientOpt{sr.URLs(url), sr.UserAgent("mqx")}
	if creds.Username != "" || creds.Password != "" {
		opts = append(opts, sr.BasicAuth(creds.Username, creds.Password))
	}
	cl, err := sr.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("schema registry client: %w", err)
	}
	return &registryClient{cl: cl}, nil
}

func (k *Kafka) registryClient() (*registryClient, error) {
	if k.registry == nil {
		return nil, fmt.Errorf("schema registry is not configured; add schema_registry.url to the context: %w", broker.ErrUnsupported)
	}
	return k.registry, nil
}

// srErr maps registry errors: missing subjects, versions and schemas become broker.ErrNotFound.
func srErr(what string, err error) error {
	if err == nil {
		return nil
	}
	var re *sr.ResponseError
	if errors.As(err, &re) {
		if re.StatusCode == 404 {
			return fmt.Errorf("%s: %w (%s); run `mqx schema subjects` to list subjects", what, broker.ErrNotFound, re.Error())
		}
		if re.StatusCode == 401 || re.StatusCode == 403 {
			return fmt.Errorf("%s: %w; check username_env / password_env of schema_registry", what, err)
		}
		return fmt.Errorf("%s: schema registry HTTP %d: %w", what, re.StatusCode, err)
	}
	return fmt.Errorf("%s: %w", what, err)
}

// Subjects implements broker.SchemaRegistry.
func (k *Kafka) Subjects(ctx context.Context) ([]string, error) {
	r, err := k.registryClient()
	if err != nil {
		return nil, err
	}
	subjects, err := r.cl.Subjects(ctx)
	return subjects, srErr("list subjects", err)
}

// SchemaVersions implements broker.SchemaRegistry.
func (k *Kafka) SchemaVersions(ctx context.Context, subject string) ([]int, error) {
	r, err := k.registryClient()
	if err != nil {
		return nil, err
	}
	versions, err := r.cl.SubjectVersions(ctx, subject)
	return versions, srErr(fmt.Sprintf("versions of subject %q", subject), err)
}

// Schema implements broker.SchemaRegistry; version -1 is the latest.
func (k *Kafka) Schema(ctx context.Context, subject string, version int) (*broker.Schema, error) {
	r, err := k.registryClient()
	if err != nil {
		return nil, err
	}
	s, err := r.bySubject(ctx, subject, version)
	if err != nil {
		return nil, err
	}
	out := s.public()
	return &out, nil
}

// SubjectCompatibility implements broker.SchemaRegistry, falling back to the global level.
func (k *Kafka) SubjectCompatibility(ctx context.Context, subject string) (string, error) {
	r, err := k.registryClient()
	if err != nil {
		return "", err
	}
	res := r.cl.Compatibility(sr.WithParams(ctx, sr.DefaultToGlobal), subject)
	if len(res) == 0 {
		return "", fmt.Errorf("compatibility of %q: empty response", subject)
	}
	if res[0].Err != nil {
		return "", srErr(fmt.Sprintf("compatibility of %q", subject), res[0].Err)
	}
	return res[0].Level.String(), nil
}

// RegisterSchema implements broker.SchemaRegistry and returns the schema id.
func (k *Kafka) RegisterSchema(ctx context.Context, subject string, s broker.Schema) (int, error) {
	r, err := k.registryClient()
	if err != nil {
		return 0, err
	}
	typ, err := schemaType(s.Type)
	if err != nil {
		return 0, err
	}
	in := sr.Schema{Schema: s.Schema, Type: typ}
	for _, ref := range s.References {
		name := cmp.Or(ref.Name, ref.Subject)
		in.References = append(in.References, sr.SchemaReference{Name: name, Subject: ref.Subject, Version: ref.Version})
	}
	id, err := r.cl.RegisterSchema(ctx, subject, in, -1, -1)
	return id, srErr(fmt.Sprintf("register schema under %q", subject), err)
}

// DeleteSubject implements broker.SchemaRegistry. A permanent delete soft
// deletes first when needed, as the registry requires.
func (k *Kafka) DeleteSubject(ctx context.Context, subject string, permanent bool) ([]int, error) {
	r, err := k.registryClient()
	if err != nil {
		return nil, err
	}
	versions, err := r.cl.DeleteSubject(ctx, subject, sr.SoftDelete)
	var re *sr.ResponseError
	softDeleted := errors.As(err, &re) && re.ErrorCode == sr.ErrSubjectSoftDeleted.Code
	if err != nil && (!permanent || !softDeleted) {
		return nil, srErr(fmt.Sprintf("delete subject %q", subject), err)
	}
	if !permanent {
		return versions, nil
	}
	versions, err = r.cl.DeleteSubject(ctx, subject, sr.HardDelete)
	return versions, srErr(fmt.Sprintf("permanently delete subject %q", subject), err)
}

func schemaType(s string) (sr.SchemaType, error) {
	switch strings.ToUpper(s) {
	case "", "AVRO":
		return sr.TypeAvro, nil
	case "PROTOBUF", "PROTO":
		return sr.TypeProtobuf, nil
	case "JSON", "JSONSCHEMA":
		return sr.TypeJSON, nil
	}
	return 0, fmt.Errorf("schema type %q: use AVRO, PROTOBUF or JSON", s)
}

// regSchema is a registered schema with its references, as the serde needs it.
type regSchema struct {
	ID      int
	Subject string
	Version int
	Type    string // AVRO, PROTOBUF, JSON
	Text    string
	Refs    []sr.SchemaReference
}

func (s regSchema) public() broker.Schema {
	out := broker.Schema{Subject: s.Subject, Version: s.Version, ID: s.ID, Type: s.Type, Schema: s.Text}
	for _, r := range s.Refs {
		out.References = append(out.References, broker.SchemaRef{Name: r.Name, Subject: r.Subject, Version: r.Version})
	}
	return out
}

func typeName(t sr.SchemaType) string {
	switch t {
	case sr.TypeProtobuf:
		return "PROTOBUF"
	case sr.TypeJSON:
		return "JSON"
	}
	return "AVRO"
}

// byID implements schemaLookup. The subject is the first one using the id.
func (r *registryClient) byID(ctx context.Context, id int) (regSchema, error) {
	s, err := r.cl.SchemaByID(ctx, id)
	if err != nil {
		return regSchema{}, srErr(fmt.Sprintf("schema id %d", id), err)
	}
	out := regSchema{ID: id, Type: typeName(s.Type), Text: s.Schema, Refs: s.References}
	if usages, err := r.cl.SchemaUsagesByID(ctx, id); err == nil && len(usages) > 0 {
		out.Subject, out.Version = usages[0].Subject, usages[0].Version
	}
	return out, nil
}

// bySubject implements schemaLookup; version -1 is the latest.
func (r *registryClient) bySubject(ctx context.Context, subject string, version int) (regSchema, error) {
	s, err := r.cl.SchemaByVersion(ctx, subject, version)
	if err != nil {
		return regSchema{}, srErr(fmt.Sprintf("schema %q version %s", subject, versionLabel(version)), err)
	}
	return regSchema{ID: s.ID, Subject: s.Subject, Version: s.Version, Type: typeName(s.Type), Text: s.Schema.Schema, Refs: s.References}, nil
}

func versionLabel(v int) string {
	if v < 0 {
		return "latest"
	}
	return fmt.Sprint(v)
}
