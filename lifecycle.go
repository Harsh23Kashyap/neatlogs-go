package neatlogs

import (
	"context"
	"sort"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const (
	interruptedAttribute       = "neatlogs.trace.interrupted"
	terminationReasonAttribute = "neatlogs.trace.termination.reason"
)

// activeSpanRegistry owns the live spans created on Neatlogs' private tracer
// provider. The mutex protects framework-created spans that can start and end
// from different goroutines.
type activeSpanRegistry struct {
	mu    sync.Mutex
	spans map[trace.SpanID]sdktrace.ReadWriteSpan
}

var _ sdktrace.SpanProcessor = (*activeSpanRegistry)(nil)

func newActiveSpanRegistry() *activeSpanRegistry {
	return &activeSpanRegistry{spans: make(map[trace.SpanID]sdktrace.ReadWriteSpan)}
}

func (p *activeSpanRegistry) OnStart(_ context.Context, span sdktrace.ReadWriteSpan) {
	p.mu.Lock()
	p.spans[span.SpanContext().SpanID()] = span
	p.mu.Unlock()
}

func (p *activeSpanRegistry) OnEnd(span sdktrace.ReadOnlySpan) {
	p.mu.Lock()
	delete(p.spans, span.SpanContext().SpanID())
	p.mu.Unlock()
}

func (p *activeSpanRegistry) Shutdown(context.Context) error   { return nil }
func (p *activeSpanRegistry) ForceFlush(context.Context) error { return nil }

// endActiveSpans atomically claims the current registry and ends it child-first.
// Claiming before calling End makes repeated and concurrent shutdown calls
// idempotent. A normal application End that wins the race is skipped through
// IsRecording. Existing span status is deliberately left untouched.
func (p *activeSpanRegistry) endActiveSpans(reason string) int {
	p.mu.Lock()
	active := p.spans
	p.spans = make(map[trace.SpanID]sdktrace.ReadWriteSpan)
	p.mu.Unlock()

	if len(active) == 0 {
		return 0
	}

	spans := make([]sdktrace.ReadWriteSpan, 0, len(active))
	for _, span := range active {
		spans = append(spans, span)
	}
	sort.SliceStable(spans, func(i, j int) bool {
		return activeSpanDepth(spans[i], active) > activeSpanDepth(spans[j], active)
	})

	cleanReason := truncateRunes(reason, 256)
	if cleanReason == "" {
		cleanReason = "shutdown"
	}
	ended := 0
	for _, span := range spans {
		if !span.IsRecording() {
			continue
		}
		span.SetAttributes(
			attribute.Bool(interruptedAttribute, true),
			attribute.String(terminationReasonAttribute, cleanReason),
		)
		span.End()
		ended++
	}
	return ended
}

func activeSpanDepth(span sdktrace.ReadWriteSpan, active map[trace.SpanID]sdktrace.ReadWriteSpan) int {
	parent := span.Parent()
	seen := make(map[trace.SpanID]struct{})
	depth := 0
	for parent.IsValid() {
		parentID := parent.SpanID()
		if _, duplicate := seen[parentID]; duplicate {
			break
		}
		parentSpan, ok := active[parentID]
		if !ok {
			break
		}
		seen[parentID] = struct{}{}
		depth++
		parent = parentSpan.Parent()
	}
	return depth
}

func truncateRunes(value string, limit int) string {
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return string(runes[:limit])
}
