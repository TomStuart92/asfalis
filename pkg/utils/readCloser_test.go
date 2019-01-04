package utils

import (
	"strings"
	"testing"
)

const data = "test"

var dataByte = []byte(data)
var dataLength = len(dataByte)

type CloserStub struct{}

func (CloserStub) Close() error {
	return nil

}

func TestReadCloserSuccess(t *testing.T) {
	rc := ReaderAndCloser{
		Reader: strings.NewReader(data),
		Closer: CloserStub{},
	}
	erc := NewExactReadCloser(rc, int64(dataLength))
	length, err := erc.Read(dataByte)
	if err != nil {
		t.Error("Unexpected Error Reading Bytes")
	}
	if length != dataLength {
		t.Errorf("Unexpected Data Length, expected %d, got %d", dataLength, length)
	}
}

func TestReadCloserShortFail(t *testing.T) {
	rc := ReaderAndCloser{
		Reader: strings.NewReader(""),
		Closer: CloserStub{},
	}
	erc := NewExactReadCloser(rc, int64(dataLength+1))
	_, err := erc.Read(dataByte)
	if err != ErrShortRead {
		t.Error("Expected Short Read Error")
	}
}

func TestReadCloserLongFail(t *testing.T) {
	rc := ReaderAndCloser{
		Reader: strings.NewReader(data),
		Closer: CloserStub{},
	}
	erc := NewExactReadCloser(rc, int64(dataLength-1))
	_, err := erc.Read(dataByte)
	if err != ErrExpectEOF {
		t.Error("Expected Long Read Error")
	}
}
