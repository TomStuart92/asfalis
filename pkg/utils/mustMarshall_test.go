package utils

import (
	"errors"
	"reflect"
	"testing"
)

const testString = "test"

var testBytes = []byte(testString)

type MarshalerStubSuccess struct{}
type MarshalerStubFail struct{}

func (MarshalerStubSuccess) Marshal() (data []byte, err error) {
	return testBytes, nil
}

func (MarshalerStubFail) Marshal() (data []byte, err error) {
	return nil, errors.New("Failed to Marshall")
}

func TestMustMarshallSuccess(t *testing.T) {
	data := MustMarshal(MarshalerStubSuccess{})
	if !reflect.DeepEqual(data, testBytes) {
		t.Error("Expected Marshaller To Return Bytes")
	}
}

func TestMustMarshallFail(t *testing.T) {
	data := MustMarshal(MarshalerStubFail{})
	if data != nil {
		t.Error("Expected Marshaller To Return Nil")
	}
}
