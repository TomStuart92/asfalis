package rpc

import (
	"context"
	"testing"

	pb "github.com/TomStuart92/asfalis/pkg/api"
)

func TestSet(t *testing.T) {
	test := Server{}
	req := &pb.KeyValueRequest{Key: "key", Value: "value"}
	resp, err := test.SetValue(context.Background(), req)
	if err != nil {
		t.Errorf("TestSet got unexpected error")
	}
	if resp.Key != req.Key {
		t.Errorf("TestSet, Got %v, wanted %v", resp.Key, req.Key)
	}
	if resp.Value != req.Value {
		t.Errorf("TestSet, Got %v, wanted %v", resp.Value, req.Value)
	}
}

func TestGet(t *testing.T) {
	test := Server{}
	setReq := &pb.KeyValueRequest{Key: "key", Value: "value"}
	getReq := &pb.KeyRequest{Key: "key"}
	resp, err := test.SetValue(context.Background(), setReq)
	resp, err = test.GetValue(context.Background(), getReq)
	if err != nil {
		t.Errorf("TestGet got unexpected error")
	}
	if resp.Key != setReq.Key {
		t.Errorf("TestSet, Got %v, wanted %v", resp.Key, setReq.Key)
	}
	if resp.Value != setReq.Value {
		t.Errorf("TestSet, Got %v, wanted %v", resp.Value, setReq.Value)
	}
}

func TestDelete(t *testing.T) {
	test := Server{}
	setReq := &pb.KeyValueRequest{Key: "key", Value: "value"}
	getReq := &pb.KeyRequest{Key: "key"}
	test.SetValue(context.Background(), setReq)
	test.GetValue(context.Background(), getReq)
	_, err := test.DeleteValue(context.Background(), getReq)
	if err != nil {
		t.Errorf("TestDelete got unexpected error")
	}
}
