package log

import (
	"fmt"
	"testing"
)

func TestAppend(t *testing.T) {
	list := LinkedList{nil, nil, 0}

	list.append("./test.txt")

	if list.Head == nil {
		t.Error("Incorrect Invariant: HEAD not nil on appended LL")
	}
	if list.Tail == nil {
		t.Error("Incorrect Invariant: TAIL not nil on appended LL")
	}
	if list.Head != list.Tail {
		t.Error("Incorrect Invariant: HEAD == TAIL on LL with length 1")
	}
	fmt.Print(list)
	if list.Length != 1 {
		t.Error("Incorrect Invariant: LL length = number of nodes")
	}
}
