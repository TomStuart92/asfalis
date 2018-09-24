package rpc

import (
	"errors"
	"fmt"
	"log"
	"net"

	store "github.com/TomStuart92/asfalis/pkg/store"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// Server is used to implement api.Store.
type Server struct {
	cache *store.Store
}

// SetValue implements Store.SetValue
func (s Server) SetValue(ctx context.Context, in *KeyValueRequest) (*Record, error) {
	record, err := s.cache.Set(in.Key, in.Value)
	if err != nil {
		return &Record{}, err
	}
	return &Record{Key: record.Key, Value: record.Value}, nil
}

// GetValue implements Store.GetValue
func (s Server) GetValue(ctx context.Context, in *KeyRequest) (*Record, error) {

	if value, ok := s.cache.Get(in.Key); ok {
		return &Record{Key: in.Key, Value: value}, nil
	}
	return &Record{}, errors.New("Failed To Get Record")
}

// DeleteValue implements Store.DeleteValue
func (s Server) DeleteValue(ctx context.Context, in *KeyRequest) (*DeletedRecord, error) {
	_, err := s.cache.Delete(in.Key)
	if err != nil {
		return &DeletedRecord{}, err
	}
	return &DeletedRecord{}, nil
}

// ServeRPC starts RPC server
func ServeRPC(port string, store *store.Store) {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	server := Server{store}
	RegisterStoreServer(s, server)
	reflection.Register(s)
	fmt.Printf("Server Listening on %s", port)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}