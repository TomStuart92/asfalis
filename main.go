package main

import (
	"fmt"
	"log"
	"net"

	pb "github.com/TomStuart92/asfalis/pkg/api"
	"github.com/TomStuart92/asfalis/pkg/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	port = ":50051"
)

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterStoreServer(s, &server.Server{})
	reflection.Register(s)
	fmt.Printf("Server Listening on %s", port)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
