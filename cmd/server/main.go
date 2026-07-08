package main

import (
	"fmt"
	"log/slog"
	"net"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	pb "github.com/example/file-service/gen/fileservice/v1"
	"github.com/example/file-service/internal/cache"
	"github.com/example/file-service/internal/config"
	minioclient "github.com/example/file-service/internal/minio"
	"github.com/example/file-service/internal/scanner"
	"github.com/example/file-service/internal/server"
)

func main() {
	cfg := config.Load()

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	minioClient, err := minioclient.NewClient(cfg)
	if err != nil {
		log.Error("failed to initialise MinIO client", "err", err)
		os.Exit(1)
	}

	cacheClient := cache.NewClient(cfg)
	scannerClient := scanner.NewClient(cfg)

	svc := server.New(minioClient, cacheClient, scannerClient, log)

	// Raise the message size limits to handle the largest allowed file
	// (200 MiB) transmitted as raw chunks in a single stream without
	// requiring stream splitting above the application layer.
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(256*1024*1024),
		grpc.MaxSendMsgSize(256*1024*1024),
	)

	pb.RegisterFileServiceServer(grpcServer, svc)

	// Implement the gRPC Health Checking Protocol so grpc_health_probe and
	// Docker Compose healthchecks can verify the service is ready.
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthSrv)
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	// Enable server reflection so grpcurl and grpc-ui can introspect the
	// service in development and staging environments.
	reflection.Register(grpcServer)

	addr := fmt.Sprintf(":%s", cfg.GRPCPort)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Error("failed to listen", "addr", addr, "err", err)
		os.Exit(1)
	}

	log.Info("file-service started", "addr", addr,
		"minio", fmt.Sprintf("%s:%s", cfg.MinIOEndpoint, cfg.MinIOPort),
		"clamav_configured", scannerClient.Available(),
		"redis_configured", cacheClient.Available(),
	)

	if err := grpcServer.Serve(lis); err != nil {
		log.Error("gRPC server exited with error", "err", err)
		os.Exit(1)
	}
}
