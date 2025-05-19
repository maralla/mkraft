package mkraft

import (
	"testing"
	"time"
)

func TestReadMultipleFromChannel_ExactCount(t *testing.T) {
	ch := make(chan int, 3)
	ch <- 1
	ch <- 2
	ch <- 3
	close(ch)

	result := readMultipleFromChannel(ch, 3)
	expected := []int{1, 2, 3}
	if len(result) != 3 {
		t.Fatalf("expected 3 items, got %d", len(result))
	}
	for i, v := range expected {
		if result[i] != v {
			t.Errorf("expected %d at index %d, got %d", v, i, result[i])
		}
	}
}

func TestReadMultipleFromChannel_LessThanCount(t *testing.T) {
	ch := make(chan string, 2)
	ch <- "a"
	ch <- "b"
	close(ch)

	result := readMultipleFromChannel(ch, 5)
	expected := []string{"a", "b"}
	if len(result) != 2 {
		t.Fatalf("expected 2 items, got %d", len(result))
	}
	for i, v := range expected {
		if result[i] != v {
			t.Errorf("expected %s at index %d, got %s", v, i, result[i])
		}
	}
}

func TestReadMultipleFromChannel_EmptyChannel(t *testing.T) {
	ch := make(chan float64)
	close(ch)

	result := readMultipleFromChannel(ch, 3)
	if len(result) != 0 {
		t.Fatalf("expected 0 items, got %d", len(result))
	}
}

func TestReadMultipleFromChannel_NonBlocking(t *testing.T) {
	ch := make(chan int)
	// Do not send anything to ch

	result := readMultipleFromChannel(ch, 2)
	if len(result) != 0 {
		t.Fatalf("expected 0 items, got %d", len(result))
	}
}

func TestReadMultipleFromChannel_PartialAvailable(t *testing.T) {
	ch := make(chan int, 2)
	ch <- 10
	close(ch)

	result := readMultipleFromChannel(ch, 3)
	if len(result) != 1 {
		t.Fatalf("expected 1 item, got %d", len(result))
	}
	if result[0] != 10 {
		t.Errorf("expected 10, got %d", result[0])
	}
}

func TestReadMultipleFromChannel_SlowSender(t *testing.T) {
	ch := make(chan int, 1)
	go func() {
		time.Sleep(10 * time.Millisecond)
		ch <- 42
	}()
	// Should return immediately with nothing, since channel is empty at call time
	result := readMultipleFromChannel(ch, 1)
	if len(result) != 0 {
		t.Fatalf("expected 0 items, got %d", len(result))
	}
}
