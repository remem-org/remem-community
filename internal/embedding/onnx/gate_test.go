package onnx

import (
	"testing"
	"time"
)

func waitForWaiters(t *testing.T, g *passGate, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for g.waiters() != n {
		if time.Now().After(deadline) {
			t.Fatalf("%d passes waiting, want %d", g.waiters(), n)
		}
		time.Sleep(time.Millisecond)
	}
}

// A document pass is running and fifty more are waiting when a query arrives.
// When the running pass ends, the query goes next: a search waits for at most
// the pass already in flight, never for the rest of a long write.
//
// The fifty, and the query's place among them, are what make this a test. With
// one document waiter queued before the query, a gate with no priority at all
// passed it in five runs of five: Broadcast wakes both and the scheduler
// favours the last one readied. With the priority, documents cannot be admitted
// while a query waits, so the outcome is certain.
func TestAWaitingQueryIsAdmittedBeforeWaitingDocumentPasses(t *testing.T) {
	const documents = 50
	g := newPassGate()
	g.acquire(false) // a document pass is running

	order := make(chan string, documents+1)
	document := func() {
		g.acquire(false)
		order <- "document"
		g.release()
	}
	// The query queues in the middle. Broadcast readies waiters in the order
	// they waited and the scheduler tends to run the last one readied first, so
	// a query queued last would win without any priority at all.
	for range documents / 2 {
		go document()
	}
	waitForWaiters(t, g, documents/2)
	go func() {
		g.acquire(true)
		order <- "query"
		g.release()
	}()
	waitForWaiters(t, g, documents/2+1)
	for range documents / 2 {
		go document()
	}
	waitForWaiters(t, g, documents+1)

	g.release()
	if first := <-order; first != "query" {
		t.Fatalf("a %s pass was admitted first; the waiting query must go before every waiting document pass", first)
	}
	for range documents {
		if got := <-order; got != "document" {
			t.Fatalf("then %s", got)
		}
	}
}

// Only one pass runs at a time, whatever its kind.
func TestOnePassRunsAtATime(t *testing.T) {
	g := newPassGate()
	g.acquire(true)
	admitted := make(chan struct{})
	go func() {
		g.acquire(true)
		close(admitted)
		g.release()
	}()
	waitForWaiters(t, g, 1)
	select {
	case <-admitted:
		t.Fatal("a second pass ran while the first held the gate")
	case <-time.After(20 * time.Millisecond):
	}
	g.release()
	select {
	case <-admitted:
	case <-time.After(5 * time.Second):
		t.Fatal("the waiting pass was never admitted")
	}
}
