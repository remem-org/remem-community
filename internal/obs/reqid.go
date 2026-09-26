package obs

import (
	"context"
	"log/slog"

	"github.com/remem-org/remem-go/internal/id"
)

// The identifiers spec §50 requires structured logs to carry. Each is a
// distinct unexported key type, so nothing outside this package can collide
// with them or read them by guessing a string.
type (
	tenantKey      struct{}
	requestIDKey   struct{}
	nodeKey        struct{}
	shardKey       struct{}
	jobIDKey       struct{}
	migrationIDKey struct{}
)

// Log field names. They are part of the operator-facing contract — dashboards
// and log queries are written against them — so they change only deliberately.
const (
	FieldTenant      = "tenant"
	FieldRequestID   = "request_id"
	FieldNode        = "node"
	FieldShard       = "shard"
	FieldJobID       = "job_id"
	FieldMigrationID = "migration_id"
)

// NewRequestID returns an identifier for one inbound request. It is a UUIDv7,
// so request ids sort by arrival time in a log store that sorts them as
// strings.
func NewRequestID() string { return id.New().String() }

// WithTenant records the tenant every subsequent log line belongs to.
//
// The value is a tenant id — an opaque, operator-chosen name, not user
// content — which is why spec §50's "tenant where safe" is satisfied by
// logging it.
func WithTenant(ctx context.Context, tenant string) context.Context {
	return context.WithValue(ctx, tenantKey{}, tenant)
}

// WithRequestID records the request id for ctx.
func WithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, requestID)
}

// WithNode records the node identity for ctx. Phase 14+ populates it; it is
// defined here because the log schema should not change when the cluster
// arrives.
func WithNode(ctx context.Context, node string) context.Context {
	return context.WithValue(ctx, nodeKey{}, node)
}

// WithShard records the shard for ctx. Phase 16+ populates it.
func WithShard(ctx context.Context, shard string) context.Context {
	return context.WithValue(ctx, shardKey{}, shard)
}

// WithJobID records the background job whose execution ctx belongs to.
func WithJobID(ctx context.Context, jobID string) context.Context {
	return context.WithValue(ctx, jobIDKey{}, jobID)
}

// WithMigrationID records the migration ctx belongs to.
func WithMigrationID(ctx context.Context, migrationID string) context.Context {
	return context.WithValue(ctx, migrationIDKey{}, migrationID)
}

// Tenant returns the tenant ctx carries, or "".
func Tenant(ctx context.Context) string { return stringValue(ctx, tenantKey{}) }

// RequestID returns the request id ctx carries, or "".
func RequestID(ctx context.Context) string { return stringValue(ctx, requestIDKey{}) }

// Node returns the node identity ctx carries, or "".
func Node(ctx context.Context) string { return stringValue(ctx, nodeKey{}) }

// Shard returns the shard ctx carries, or "".
func Shard(ctx context.Context) string { return stringValue(ctx, shardKey{}) }

// JobID returns the job id ctx carries, or "".
func JobID(ctx context.Context) string { return stringValue(ctx, jobIDKey{}) }

// MigrationID returns the migration id ctx carries, or "".
func MigrationID(ctx context.Context) string { return stringValue(ctx, migrationIDKey{}) }

func stringValue(ctx context.Context, key any) string {
	v, _ := ctx.Value(key).(string)
	return v
}

// contextAttrs renders the identifiers present in ctx as log attributes.
// Absent ones are omitted rather than logged empty: a field that is always
// present carries no information about whether it was known.
func contextAttrs(ctx context.Context) []slog.Attr {
	if ctx == nil {
		return nil
	}
	pairs := [...]struct {
		name  string
		value string
	}{
		{FieldTenant, Tenant(ctx)},
		{FieldRequestID, RequestID(ctx)},
		{FieldNode, Node(ctx)},
		{FieldShard, Shard(ctx)},
		{FieldJobID, JobID(ctx)},
		{FieldMigrationID, MigrationID(ctx)},
	}
	attrs := make([]slog.Attr, 0, len(pairs))
	for _, p := range pairs {
		if p.value != "" {
			attrs = append(attrs, slog.String(p.name, p.value))
		}
	}
	return attrs
}
