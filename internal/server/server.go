// Package server implements the gRPC FileService handlers. Every file
// transfer - upload, download, and scan - is fully streamed in 64 KiB chunks
// so the service never holds an entire file in heap memory.
package server

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"math"
	"time"

	"github.com/minio/minio-go/v7"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/example/file-service/gen/fileservice/v1"
	minioclient "github.com/example/file-service/internal/minio"
	"github.com/example/file-service/internal/scanner"
)

const (
	chunkSize            = minioclient.ChunkSize
	defaultPresignExpiry = time.Hour
	presignCacheFraction = 0.8
	statCacheTTL         = 60 * time.Second
)

type cachedStat struct {
	ContentType          string `json:"ct"`
	SizeBytes            int64  `json:"size"`
	LastModifiedUnixNano int64  `json:"lm"`
}

type minioAPI interface {
	GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, string, error)
	PutObject(ctx context.Context, bucket, key, contentType string, r io.Reader, size int64) (int64, error)
	DeleteObject(ctx context.Context, bucket, key string) error
	StatObject(ctx context.Context, bucket, key string) (minio.ObjectInfo, error)
	ListObjects(ctx context.Context, bucket, prefix string) ([]minio.ObjectInfo, error)
	PresignedGetURL(ctx context.Context, bucket, key string, expiry time.Duration) (string, error)
}

type cacheAPI interface {
	GetPresignedURL(ctx context.Context, bucket, key string) (string, error)
	SetPresignedURL(ctx context.Context, bucket, key, url string, ttl time.Duration) error
	InvalidatePresignedURL(ctx context.Context, bucket, key string) error
	GetStat(ctx context.Context, bucket, key string) (string, error)
	SetStat(ctx context.Context, bucket, key, data string, ttl time.Duration) error
	InvalidateStat(ctx context.Context, bucket, key string) error
}

type scannerAPI interface {
	Available() bool
	ScanReader(r io.Reader, chunkSize int) (scanner.ScanResult, string, error)
}

// Server is the gRPC service implementation. It is safe for concurrent use.
type Server struct {
	pb.UnimplementedFileServiceServer
	minio   minioAPI
	cache   cacheAPI
	scanner scannerAPI
	log     *slog.Logger
}

// New constructs a Server with the given dependencies.
func New(minio minioAPI, cache cacheAPI, scanner scannerAPI, log *slog.Logger) *Server {
	return &Server{minio: minio, cache: cache, scanner: scanner, log: log}
}

// ── GetFile ───────────────────────────────────────────────────────────────────

func (s *Server) GetFile(req *pb.GetFileRequest, stream pb.FileService_GetFileServer) error {
	ctx := stream.Context()
	rc, contentType, err := s.minio.GetObject(ctx, req.Bucket, req.Key)
	if err != nil {
		return status.Errorf(codes.Internal, "get object: %v", err)
	}
	defer rc.Close()

	buf := make([]byte, chunkSize)
	first := true
	for {
		n, readErr := rc.Read(buf)
		if n > 0 {
			chunk := &pb.GetFileChunk{Data: buf[:n]}
			if first {
				chunk.ContentType = contentType
				first = false
			}
			if err := stream.Send(chunk); err != nil {
				return status.Errorf(codes.Internal, "send chunk: %v", err)
			}
		}
		if readErr == io.EOF {
			return nil
		}
		if readErr != nil {
			return status.Errorf(codes.Internal, "read object: %v", readErr)
		}
	}
}

// ── PutFile ───────────────────────────────────────────────────────────────────

func (s *Server) PutFile(stream pb.FileService_PutFileServer) error {
	ctx := stream.Context()
	first, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "expected first message with metadata: %v", err)
	}
	meta := first.GetMeta()
	if meta == nil {
		return status.Error(codes.InvalidArgument, "first message must contain PutFileMeta")
	}

	pr, pw := io.Pipe()
	type putResult struct {
		size int64
		err  error
	}
	resultCh := make(chan putResult, 1)
	go func() {
		size, err := s.minio.PutObject(ctx, meta.Bucket, meta.Key, meta.ContentType, pr, -1)
		resultCh <- putResult{size: size, err: err}
	}()

	var pipeErr error
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			pw.Close()
			break
		}
		if err != nil {
			pw.CloseWithError(err)
			pipeErr = err
			break
		}
		chunk := msg.GetChunk()
		if chunk == nil {
			continue
		}
		if _, err := pw.Write(chunk); err != nil {
			pipeErr = err
			pw.CloseWithError(err)
			break
		}
	}

	res := <-resultCh
	if res.err != nil {
		return status.Errorf(codes.Internal, "put object: %v", res.err)
	}
	if pipeErr != nil {
		return status.Errorf(codes.Internal, "receive stream: %v", pipeErr)
	}
	_ = s.cache.InvalidateStat(ctx, meta.Bucket, meta.Key)
	_ = s.cache.InvalidatePresignedURL(ctx, meta.Bucket, meta.Key)
	return stream.SendAndClose(&pb.PutFileResponse{Key: meta.Key, SizeBytes: res.size})
}

// ── DeleteFile ────────────────────────────────────────────────────────────────

func (s *Server) DeleteFile(ctx context.Context, req *pb.DeleteFileRequest) (*pb.DeleteFileResponse, error) {
	if err := s.minio.DeleteObject(ctx, req.Bucket, req.Key); err != nil {
		return nil, status.Errorf(codes.Internal, "delete object: %v", err)
	}
	_ = s.cache.InvalidateStat(ctx, req.Bucket, req.Key)
	_ = s.cache.InvalidatePresignedURL(ctx, req.Bucket, req.Key)
	return &pb.DeleteFileResponse{}, nil
}

// ── StatFile ──────────────────────────────────────────────────────────────────

func (s *Server) StatFile(ctx context.Context, req *pb.StatFileRequest) (*pb.StatFileResponse, error) {
	if cached, _ := s.cache.GetStat(ctx, req.Bucket, req.Key); cached != "" {
		var cs cachedStat
		if err := json.Unmarshal([]byte(cached), &cs); err == nil {
			return &pb.StatFileResponse{
				ContentType:  cs.ContentType,
				SizeBytes:    cs.SizeBytes,
				LastModified: timestamppb.New(time.Unix(0, cs.LastModifiedUnixNano)),
			}, nil
		}
	}
	info, err := s.minio.StatObject(ctx, req.Bucket, req.Key)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "stat object: %v", err)
	}
	resp := &pb.StatFileResponse{
		ContentType:  info.ContentType,
		SizeBytes:    info.Size,
		LastModified: timestamppb.New(info.LastModified),
	}
	if data, err := json.Marshal(cachedStat{
		ContentType:          info.ContentType,
		SizeBytes:            info.Size,
		LastModifiedUnixNano: info.LastModified.UnixNano(),
	}); err == nil {
		_ = s.cache.SetStat(ctx, req.Bucket, req.Key, string(data), statCacheTTL)
	}
	return resp, nil
}

// ── ListFiles ─────────────────────────────────────────────────────────────────

func (s *Server) ListFiles(ctx context.Context, req *pb.ListFilesRequest) (*pb.ListFilesResponse, error) {
	objects, err := s.minio.ListObjects(ctx, req.Bucket, req.Prefix)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list objects: %v", err)
	}
	files := make([]*pb.FileInfo, 0, len(objects))
	for _, o := range objects {
		files = append(files, &pb.FileInfo{
			Key:          o.Key,
			SizeBytes:    o.Size,
			LastModified: timestamppb.New(o.LastModified),
		})
	}
	return &pb.ListFilesResponse{Files: files}, nil
}

// ── GetPresignedUrl ───────────────────────────────────────────────────────────

func (s *Server) GetPresignedUrl(ctx context.Context, req *pb.PresignedUrlRequest) (*pb.PresignedUrlResponse, error) {
	if cached, _ := s.cache.GetPresignedURL(ctx, req.Bucket, req.Key); cached != "" {
		return &pb.PresignedUrlResponse{Url: cached}, nil
	}
	expirySecs := req.ExpirySeconds
	if expirySecs <= 0 {
		expirySecs = int32(defaultPresignExpiry.Seconds())
	}
	expiry := time.Duration(expirySecs) * time.Second
	url, err := s.minio.PresignedGetURL(ctx, req.Bucket, req.Key, expiry)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "presign url: %v", err)
	}
	cacheTTL := time.Duration(math.Round(float64(expiry) * presignCacheFraction))
	_ = s.cache.SetPresignedURL(ctx, req.Bucket, req.Key, url, cacheTTL)
	return &pb.PresignedUrlResponse{Url: url}, nil
}

// ── ScanFile ──────────────────────────────────────────────────────────────────

type scanOutcome struct {
	result    scanner.ScanResult
	virusName string
	err       error
}

func (s *Server) ScanFile(stream pb.FileService_ScanFileServer) error {
	pr, pw := io.Pipe()
	outCh := make(chan scanOutcome, 1)
	go func() {
		result, virusName, err := s.scanner.ScanReader(pr, chunkSize)
		outCh <- scanOutcome{result: result, virusName: virusName, err: err}
	}()

	var pipeErr error
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			pw.Close()
			break
		}
		if err != nil {
			pw.CloseWithError(err)
			pipeErr = err
			break
		}
		if _, err := pw.Write(msg.Chunk); err != nil {
			pipeErr = err
			pw.CloseWithError(err)
			break
		}
	}

	out := <-outCh
	if pipeErr != nil && out.result == scanner.ResultUnavailable {
		return status.Errorf(codes.Internal, "receive stream: %v", pipeErr)
	}
	if out.err != nil {
		s.log.Warn("clamd scan error", "err", out.err)
	}

	var pbResult pb.ScanResult
	switch out.result {
	case scanner.ResultClean:
		pbResult = pb.ScanResult_SCAN_RESULT_CLEAN
	case scanner.ResultInfected:
		pbResult = pb.ScanResult_SCAN_RESULT_INFECTED
	default:
		pbResult = pb.ScanResult_SCAN_RESULT_UNAVAILABLE
	}
	return stream.SendAndClose(&pb.ScanFileResponse{Result: pbResult, VirusName: out.virusName})
}
