package neatlogs

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// Every span must carry the SDK version as `service.version` (parity with the
// Python and TypeScript SDKs) and on its instrumentation scope.
func TestVersion_StampedOnResourceAndScope(t *testing.T) {
	ctx := context.Background()
	sink := tracetest.NewInMemoryExporter()
	sd, err := Init(ctx, Config{WorkflowName: "wf-test"}, WithExporter(sink))
	if err != nil {
		t.Fatal(err)
	}
	defer sd(ctx)

	_, _, end := Trace(ctx, "root")
	end()

	Flush(ctx)

	spans := sink.GetSpans()
	if len(spans) == 0 {
		t.Fatal("no spans exported")
	}
	for _, s := range spans {
		var got string
		for _, kv := range s.Resource.Attributes() {
			if string(kv.Key) == "service.version" {
				got = kv.Value.AsString()
			}
		}
		if got != Version {
			t.Errorf("span %q: service.version = %q, want %q", s.Name, got, Version)
		}
		if s.InstrumentationScope.Version != Version {
			t.Errorf("span %q: scope version = %q, want %q", s.Name, s.InstrumentationScope.Version, Version)
		}
	}
}
