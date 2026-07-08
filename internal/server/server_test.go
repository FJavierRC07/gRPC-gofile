package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "github.com/example/file-service/gen/fileservice/v1"
	"github.com/example/file-service/internal/scanner"
)

type fakeMinio struct {
	getObjectErr  error
	statObjectErr error
}

func (f fakeMinio) GetObject(context.Context, string, string) (io.ReadCloser, string, error) {
	return nil, "", f.getObjectErr
}

func (f fakeMinio) PutObject(context.Context, string, string, string, io.Reader, int64) (int64, error) {
	return 0, nil
}

func (f fakeMinio) DeleteObject(context.Context, string, string) error { return nil }

func (f fakeMinio) StatObject(context.Context, string, string) (minio.ObjectInfo, error) {
	return minio.ObjectInfo{}, f.statObjectErr
}

func (f fakeMinio) ListObjects(context.Context, string, string) ([]minio.ObjectInfo, error) {
	return nil, nil
}

func (f fakeMinio) PresignedGetURL(context.Context, string, string, time.Duration) (string, error) {
	return "", nil
}

type fakeCache struct{}

func (fakeCache) GetPresignedURL(context.Context, string, string) (string, error) { return "", nil }
func (fakeCache) SetPresignedURL(context.Context, string, string, string, time.Duration) error {
	return nil
}
func (fakeCache) InvalidatePresignedURL(context.Context, string, string) error         { return nil }
func (fakeCache) GetStat(context.Context, string, string) (string, error)              { return "", nil }
func (fakeCache) SetStat(context.Context, string, string, string, time.Duration) error { return nil }
func (fakeCache) InvalidateStat(context.Context, string, string) error                 { return nil }

type fakeScanner struct{}

func (fakeScanner) Available() bool { return false }
func (fakeScanner) ScanReader(io.Reader, int) (scanner.ScanResult, string, error) {
	return scanner.ResultUnavailable, "", nil
}

type fakeGetFileStream struct{ ctx context.Context }

func (f fakeGetFileStream) SetHeader(metadata.MD) error  { return nil }
func (f fakeGetFileStream) SendHeader(metadata.MD) error { return nil }
func (f fakeGetFileStream) SetTrailer(metadata.MD)       {}
func (f fakeGetFileStream) Context() context.Context     { return f.ctx }
func (f fakeGetFileStream) SendMsg(any) error            { return nil }
func (f fakeGetFileStream) RecvMsg(any) error            { return nil }
func (f fakeGetFileStream) Send(*pb.GetFileChunk) error  { return nil }

func TestGetFileReturnsInternalOnMinIOFailure(t *testing.T) {
	svc := New(
		fakeMinio{getObjectErr: errors.New("boom")},
		fakeCache{},
		fakeScanner{},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)

	err := svc.GetFile(&pb.GetFileRequest{Bucket: "b", Key: "k"}, fakeGetFileStream{ctx: context.Background()})
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected Internal, got %v (%v)", status.Code(err), err)
	}
}

func TestStatFileReturnsInternalOnMinIOFailure(t *testing.T) {
	svc := New(
		fakeMinio{statObjectErr: errors.New("boom")},
		fakeCache{},
		fakeScanner{},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)

	_, err := svc.StatFile(context.Background(), &pb.StatFileRequest{Bucket: "b", Key: "k"})
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected Internal, got %v (%v)", status.Code(err), err)
	}
}
