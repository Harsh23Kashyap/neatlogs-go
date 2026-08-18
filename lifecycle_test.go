package neatlogs

import (
	"context"
	"os"
	"syscall"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestShutdownEndsActiveSpansChildFirstWithoutChangingStatus(t *testing.T) {
	ctx := context.Background()
	sink := &retainingExporter{InMemoryExporter: tracetest.NewInMemoryExporter()}
	shutdown, err := Init(ctx, Config{
		WorkflowName:          "graceful-shutdown",
		DisableSignalHandlers: true,
	}, WithExporter(sink))
	if err != nil {
		t.Fatal(err)
	}

	childCtx, root, _ := Trace(ctx, "workflow")
	root.SetStatus(codes.Ok, "successful so far")
	_, _, _ = StartSpan(childCtx, "agent", "agent")

	if err := shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := shutdown(ctx); err != nil {
		t.Fatalf("second shutdown: %v", err)
	}

	spans := sink.GetSpans()
	rootIndex, childIndex := -1, -1
	var rootSpan, childSpan tracetest.SpanStub
	for i, span := range spans {
		switch span.Name {
		case "workflow":
			rootIndex, rootSpan = i, span
		case "agent":
			childIndex, childSpan = i, span
		}
	}
	if childIndex < 0 || rootIndex < 0 || childIndex >= rootIndex {
		t.Fatalf("span order = child %d, root %d; want child before root", childIndex, rootIndex)
	}
	if rootSpan.Status.Code != codes.Ok {
		t.Fatalf("root status = %v, want OK", rootSpan.Status.Code)
	}
	if childSpan.Status.Code != codes.Unset {
		t.Fatalf("child status = %v, want UNSET", childSpan.Status.Code)
	}
	for _, span := range []tracetest.SpanStub{childSpan, rootSpan} {
		if interrupted, ok := attrBool(span.Attributes, interruptedAttribute); !ok || !interrupted {
			t.Errorf("%s interrupted = %v (present %v), want true", span.Name, interrupted, ok)
		}
		if reason, ok := attrString(span.Attributes, terminationReasonAttribute); !ok || reason != "shutdown" {
			t.Errorf("%s reason = %q (present %v), want shutdown", span.Name, reason, ok)
		}
		if len(span.Events) != 0 {
			t.Errorf("%s events = %d, want none", span.Name, len(span.Events))
		}
	}
	if byName(sink.InMemoryExporter, completionMarkerName).Name != completionMarkerName {
		t.Fatal("missing completion marker")
	}
}

// tracetest.InMemoryExporter clears its buffer from Shutdown; retain it so this
// test can inspect what the SDK flushed as part of its shutdown call.
type retainingExporter struct {
	*tracetest.InMemoryExporter
}

func (e *retainingExporter) Shutdown(context.Context) error { return nil }

func TestShutdownSignalControllerFlushesBeforeRedelivery(t *testing.T) {
	controller := newShutdownSignalController()
	shutdownCalled := make(chan os.Signal, 1)
	redelivered := make(chan os.Signal, 1)
	go controller.run(
		func(sig os.Signal) { shutdownCalled <- sig },
		func(sig os.Signal) { redelivered <- sig },
	)

	controller.signals <- syscall.SIGTERM

	select {
	case sig := <-shutdownCalled:
		if reason := signalTerminationReason(sig); reason != "SIGTERM" {
			t.Fatalf("reason = %q, want SIGTERM", reason)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown was not called")
	}
	select {
	case sig := <-redelivered:
		if sig != syscall.SIGTERM {
			t.Fatalf("re-delivered %v, want SIGTERM", sig)
		}
	case <-time.After(time.Second):
		t.Fatal("signal was not re-delivered")
	}
}

func attrBool(kvs []attribute.KeyValue, key string) (bool, bool) {
	for _, kv := range kvs {
		if string(kv.Key) == key {
			return kv.Value.AsBool(), true
		}
	}
	return false, false
}
