package server

import (
	pb "github.com/TomStuart92/asfalis/pkg/api"
	store "github.com/TomStuart92/asfalis/pkg/store"
	"golang.org/x/net/context"
)

// Server is used to implement api.Store.
type Server struct{}

var cache = store.New()

// SetValue implements Store.SetValue
func (s *Server) SetValue(ctx context.Context, in *pb.KeyValueRequest) (*pb.Record, error) {
	record, err := cache.Set(in.Key, in.Value)
	if err != nil {
		return &pb.Record{}, err
	}
	return &pb.Record{Key: record.Key, Value: record.Value}, nil
}

// GetValue implements Store.GetValue
func (s *Server) GetValue(ctx context.Context, in *pb.KeyRequest) (*pb.Record, error) {
	record, err := cache.Get(in.Key)
	if err != nil {
		return &pb.Record{}, err
	}
	return &pb.Record{Key: record.Key, Value: record.Value}, nil
}

// DeleteValue implements Store.DeleteValue
func (s *Server) DeleteValue(ctx context.Context, in *pb.KeyRequest) (*pb.DeletedRecord, error) {
	_, err := cache.Delete(in.Key)
	if err != nil {
		return &pb.DeletedRecord{}, err
	}
	return &pb.DeletedRecord{}, nil
}
