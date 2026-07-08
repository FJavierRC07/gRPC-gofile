// Package cache provides a thin Redis wrapper for two caching purposes:
//
//   - Presigned URL cache: avoids a MinIO round-trip on every file-serve
//     request. The cached URL is stored for 80 % of its expiry so it never
//     expires in the hands of a client before it was supposed to.
//
//   - StatFile cache: avoids a MinIO StatObject call for metadata that rarely
//     changes. TTL is 60 seconds and is invalidated explicitly on put/delete.
//
// All methods degrade silently when Redis is not configured or is temporarily
// unavailable: the service continues to function, just without caching.
package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ctf-platform/file-service/internal/config"
)

// Client wraps a redis.Client. A nil inner client means Redis is not
// configured; all operations become no-ops returning nil.
type Client struct {
	rdb *redis.Client
}

// NewClient initialises a Client from cfg. Returns a no-op Client when
// RedisURL is empty or cannot be parsed.
func NewClient(cfg *config.Config) *Client {
	if cfg.RedisURL == "" {
		return &Client{}
	}
	opt, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return &Client{}
	}
	return &Client{rdb: redis.NewClient(opt)}
}

// Available reports whether Redis is configured and the cache is active.
func (c *Client) Available() bool { return c.rdb != nil }

func (c *Client) available() bool { return c.Available() }

// ── Presigned URL cache ───────────────────────────────────────────────────────

func (c *Client) GetPresignedURL(ctx context.Context, bucket, key string) (string, error) {
	if !c.available() {
		return "", nil
	}
	val, err := c.rdb.Get(ctx, presignedKey(bucket, key)).Result()
	if err == redis.Nil {
		return "", nil
	}
	return val, err
}

func (c *Client) SetPresignedURL(ctx context.Context, bucket, key, url string, ttl time.Duration) error {
	if !c.available() {
		return nil
	}
	return c.rdb.Set(ctx, presignedKey(bucket, key), url, ttl).Err()
}

func (c *Client) InvalidatePresignedURL(ctx context.Context, bucket, key string) error {
	if !c.available() {
		return nil
	}
	return c.rdb.Del(ctx, presignedKey(bucket, key)).Err()
}

// ── StatFile cache ────────────────────────────────────────────────────────────

func (c *Client) GetStat(ctx context.Context, bucket, key string) (string, error) {
	if !c.available() {
		return "", nil
	}
	val, err := c.rdb.Get(ctx, statKey(bucket, key)).Result()
	if err == redis.Nil {
		return "", nil
	}
	return val, err
}

func (c *Client) SetStat(ctx context.Context, bucket, key, data string, ttl time.Duration) error {
	if !c.available() {
		return nil
	}
	return c.rdb.Set(ctx, statKey(bucket, key), data, ttl).Err()
}

func (c *Client) InvalidateStat(ctx context.Context, bucket, key string) error {
	if !c.available() {
		return nil
	}
	return c.rdb.Del(ctx, statKey(bucket, key)).Err()
}

func presignedKey(bucket, key string) string {
	return fmt.Sprintf("fileservice:presigned:%s:%s", bucket, key)
}

func statKey(bucket, key string) string {
	return fmt.Sprintf("fileservice:stat:%s:%s", bucket, key)
}
