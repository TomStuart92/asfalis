package server

import (
	"log"
	"net"

	pb "github.com/TomStuart92/asfalis/pkg/api"
	store "github.com/TomStuart92/asfalis/pkg/store"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	port = ":50051"
)

// server is used to implement api.Store.
type server struct{}

var cache = store.New()

// SetValue implements Store.SetValue
func (s *server) SetValue(ctx context.Context, in *pb.KeyValueRequest) (*pb.Record, error) {
	record, err := cache.Set(in.Key, in.Value)
	if err != nil {
		return &pb.Record{}, err
	}
	return &pb.Record{Key: record.Key, Value: record.Value}, nil
}

// GetValue implements Store.GetValue
func (s *server) GetValue(ctx context.Context, in *pb.KeyRequest) (*pb.Record, error) {
	record, err := cache.Get(in.Key)
	if err != nil {
		return &pb.Record{}, err
	}
	return &pb.Record{Key: record.Key, Value: record.Value}, nil
}

// DeleteValue implements Store.DeleteValue
func (s *server) DeleteValue(ctx context.Context, in *pb.KeyRequest) (*pb.DeletedRecord, error) {
	_, err := cache.Delete(in.Key)
	if err != nil {
		return &pb.DeletedRecord{}, err
	}
	return &pb.DeletedRecord{}, nil
}

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterStoreServer(s, &server{})
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
