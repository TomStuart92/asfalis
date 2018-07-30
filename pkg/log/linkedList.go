package log

import (
	"fmt"
	"os"
	"sync"
	"time"
)

// filePointer wraps a ref to a specific file
type filePointer struct {
	Name      string
	Mutex     *sync.Mutex
	File      *os.File
	CreatedAt time.Time
}

type LinkedListNode struct {
	Next *LinkedListNode
	Prev *LinkedListNode
	Data *filePointer
}

// LinkedList holds a pointer to the latest logfile.
type LinkedList struct {
	Head   *LinkedListNode
	Tail   *LinkedListNode
	Length int
}

// Initialize creates first logfile
func newFilePointer(filename string) (*filePointer, error) {
	var mutex = &sync.Mutex{}

	file, err := os.Create(filename)
	if err != nil {
		fmt.Printf("%s\n", err)
		file, err = os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return nil, err
		}
	}

	filePointer := filePointer{filename, mutex, file, time.Now()}
	return &filePointer, nil
}

func (list *LinkedList) append(filename string) (*LinkedList, error) {
	filePointer, err := newFilePointer(filename)
	if err != nil {
		return nil, err
	}
	currentTail := list.Tail
	newTail := LinkedListNode{Next: nil, Prev: currentTail, Data: filePointer}
	if list.Head == nil {
		list.Head = &newTail
	}
	if currentTail != nil {
		currentTail.Next = &newTail
	}
	list.Tail = &newTail
	list.Length++
	return list, nil
}
