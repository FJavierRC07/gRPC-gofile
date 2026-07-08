package config

import "os"

// Config holds all runtime configuration for the file service, sourced
// exclusively from environment variables.
type Config struct {
	// MinIO connection
	MinIOEndpoint  string
	MinIOPort      string
	MinIOAccessKey string
	MinIOSecretKey string
	MinIOUseSSL    bool

	// Redis (optional - caching is silently disabled when empty)
	RedisURL string

	// ClamAV clamd (optional - scanning returns UNAVAILABLE when empty)
	ClamAVHost string
	ClamAVPort string

	// gRPC listener
	GRPCPort string
}

func Load() *Config {
	return &Config{
		MinIOEndpoint:  getenv("MINIO_ENDPOINT", "minio"),
		MinIOPort:      getenv("MINIO_PORT", "9000"),
		MinIOAccessKey: os.Getenv("MINIO_ACCESS_KEY"),
		MinIOSecretKey: os.Getenv("MINIO_SECRET_KEY"),
		MinIOUseSSL:    os.Getenv("MINIO_USE_SSL") == "true",

		RedisURL: os.Getenv("REDIS_URL"),

		ClamAVHost: getenv("CLAMAV_HOST", "clamav"),
		ClamAVPort: os.Getenv("CLAMAV_PORT"),

		GRPCPort: getenv("GRPC_PORT", "50051"),
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
