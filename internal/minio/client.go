// Package minioclient wraps the minio-go SDK with the narrow set of operations
// needed by the file service. ensureBucket results are memoised per-process to
// avoid redundant round-trips; production buckets are pre-created by the
// minio-init container so the memo almost always hits after the first call.
package minioclient

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/ctf-platform/file-service/internal/config"
)

// ChunkSize is the canonical read/write unit used for streaming. 64 KiB keeps
// individual gRPC messages well below the default 4 MiB message size limit
// while being large enough to keep syscall overhead low.
const ChunkSize = 64 * 1024

// partSize is the multipart upload part size used when the total file size is
// not known in advance (size = -1). 16 MiB gives at most 13 parts for a
// 200 MiB file and keeps per-part memory pressure reasonable.
const partSize = 16 * 1024 * 1024

// Client is a thin, stateful wrapper around *minio.Client.
type Client struct {
	mc             *minio.Client
	ensuredBuckets sync.Map
}

// NewClient initialises and returns a Client. It does NOT verify connectivity
// at construction time; the first real operation will surface connection errors.
func NewClient(cfg *config.Config) (*Client, error) {
	endpoint := fmt.Sprintf("%s:%s", cfg.MinIOEndpoint, cfg.MinIOPort)
	mc, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.MinIOAccessKey, cfg.MinIOSecretKey, ""),
		Secure: cfg.MinIOUseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("minio.New: %w", err)
	}
	return &Client{mc: mc}, nil
}

// EnsureBucket guarantees that the named bucket exists, creating it if
// necessary. Results are memoised: once verified the check is skipped for the
// lifetime of the process.
func (c *Client) EnsureBucket(ctx context.Context, bucket string) error {
	if _, loaded := c.ensuredBuckets.Load(bucket); loaded {
		return nil
	}
	exists, err := c.mc.BucketExists(ctx, bucket)
	if err != nil {
		return fmt.Errorf("BucketExists(%s): %w", bucket, err)
	}
	if !exists {
		if err := c.mc.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
			return fmt.Errorf("MakeBucket(%s): %w", bucket, err)
		}
	}
	c.ensuredBuckets.Store(bucket, struct{}{})
	return nil
}

// GetObject opens a streaming download for the given object and returns an
// io.ReadCloser together with the stored Content-Type. The caller is
// responsible for closing the reader.
func (c *Client) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, string, error) {
	obj, err := c.mc.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, "", fmt.Errorf("GetObject: %w", err)
	}
	stat, err := obj.Stat()
	if err != nil {
		obj.Close()
		return nil, "", fmt.Errorf("object Stat: %w", err)
	}
	return obj, stat.ContentType, nil
}

// PutObject streams data from r into the bucket/key object. Passing size = -1
// triggers minio-go multipart upload with 16 MiB parts; the actual byte count
// written is returned. EnsureBucket is called before every put.
func (c *Client) PutObject(ctx context.Context, bucket, key, contentType string, r io.Reader, size int64) (int64, error) {
	if err := c.EnsureBucket(ctx, bucket); err != nil {
		return 0, err
	}
	info, err := c.mc.PutObject(ctx, bucket, key, r, size, minio.PutObjectOptions{
		ContentType: contentType,
		PartSize:    partSize,
	})
	if err != nil {
		return 0, fmt.Errorf("PutObject: %w", err)
	}
	return info.Size, nil
}

// DeleteObject removes an object. Errors from removing a non-existent object
// are silently ignored by the minio-go client.
func (c *Client) DeleteObject(ctx context.Context, bucket, key string) error {
	return c.mc.RemoveObject(ctx, bucket, key, minio.RemoveObjectOptions{})
}

// StatObject returns stored object metadata (content-type, size, last-modified).
func (c *Client) StatObject(ctx context.Context, bucket, key string) (minio.ObjectInfo, error) {
	return c.mc.StatObject(ctx, bucket, key, minio.StatObjectOptions{})
}

// ListObjects returns all objects in bucket whose key starts with prefix.
// An empty prefix lists everything.
func (c *Client) ListObjects(ctx context.Context, bucket, prefix string) ([]minio.ObjectInfo, error) {
	var items []minio.ObjectInfo
	for obj := range c.mc.ListObjects(ctx, bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	}) {
		if obj.Err != nil {
			return nil, fmt.Errorf("ListObjects: %w", obj.Err)
		}
		items = append(items, obj)
	}
	return items, nil
}

// PresignedGetURL generates a time-limited presigned download URL.
func (c *Client) PresignedGetURL(ctx context.Context, bucket, key string, expiry time.Duration) (string, error) {
	u, err := c.mc.PresignedGetObject(ctx, bucket, key, expiry, nil)
	if err != nil {
		return "", fmt.Errorf("PresignedGetObject: %w", err)
	}
	return u.String(), nil
}
