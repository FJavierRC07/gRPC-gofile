// Package scanner implements the ClamAV clamd INSTREAM protocol over TCP.
// It intentionally avoids third-party clamd libraries to keep the dependency
// tree minimal; the INSTREAM wire format is stable and simple enough to
// implement directly.
//
// Protocol summary (from the clamd(8) man page):
//  1. Connect to clamd over TCP.
//  2. Send the null-terminated command string "zINSTREAM\x00".
//  3. For each data chunk: write a 4-byte big-endian uint32 (chunk length)
//     followed by the chunk bytes.
//  4. Signal end-of-stream by writing 4 zero bytes.
//  5. Read the response line: "stream: OK\n", "stream: <name> FOUND\n", or
//     "stream: <msg> ERROR\n".
package scanner

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/ctf-platform/file-service/internal/config"
)

// ScanResult is the outcome of a file scan.
type ScanResult int

const (
	// ResultUnavailable means clamd is not configured or is unreachable.
	// Callers should treat this as fail-open.
	ResultUnavailable ScanResult = iota
	// ResultClean means no malware was detected.
	ResultClean
	// ResultInfected means clamd identified a known threat.
	ResultInfected
)

// connectTimeout is the TCP dial timeout when reaching clamd.
const connectTimeout = 10 * time.Second

// scanDeadline is the wall-clock deadline applied to the whole INSTREAM
// exchange once the TCP connection is established. Sized to cover the
// ClamAV scan of a 200 MiB file under reasonable load.
const scanDeadline = 5 * time.Minute

// Client holds the clamd address. A zero-value Client (ClamAVPort == "")
// reports Available() == false so callers can skip the scan path cheaply.
type Client struct {
	host string
	port string
}

// NewClient returns a Client configured from cfg.
func NewClient(cfg *config.Config) *Client {
	return &Client{host: cfg.ClamAVHost, port: cfg.ClamAVPort}
}

// Available reports whether clamd is configured. When false, ScanReader
// always returns ResultUnavailable without dialling.
func (c *Client) Available() bool {
	return c.port != ""
}

// ScanReader sends the content of r to clamd via the INSTREAM protocol and
// returns the scan result. chunkSize controls how many bytes are read from r
// per write call; pass minioclient.ChunkSize (64 KiB) for consistency.
//
// Network errors and unexpected clamd responses are treated as
// ResultUnavailable (fail-open). The second return value is the virus name
// when result == ResultInfected; it is empty otherwise.
func (c *Client) ScanReader(r io.Reader, chunkSize int) (ScanResult, string, error) {
	if !c.Available() {
		return ResultUnavailable, "", nil
	}

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%s", c.host, c.port), connectTimeout)
	if err != nil {
		return ResultUnavailable, "", nil
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(scanDeadline)); err != nil {
		return ResultUnavailable, "", nil
	}

	if _, err := fmt.Fprint(conn, "zINSTREAM\x00"); err != nil {
		return ResultUnavailable, "", nil
	}

	buf := make([]byte, chunkSize)
	var lenBuf [4]byte

	for {
		n, readErr := r.Read(buf)
		if n > 0 {
			binary.BigEndian.PutUint32(lenBuf[:], uint32(n))
			if _, err := conn.Write(lenBuf[:]); err != nil {
				return ResultUnavailable, "", nil
			}
			if _, err := conn.Write(buf[:n]); err != nil {
				return ResultUnavailable, "", nil
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return ResultUnavailable, "", fmt.Errorf("reading input: %w", readErr)
		}
	}

	if _, err := conn.Write([]byte{0, 0, 0, 0}); err != nil {
		return ResultUnavailable, "", nil
	}

	raw, err := io.ReadAll(conn)
	if err != nil {
		return ResultUnavailable, "", nil
	}

	response := strings.Trim(string(raw), " \t\r\n\x00")

	switch {
	case strings.HasSuffix(response, " OK"):
		return ResultClean, "", nil
	case strings.HasSuffix(response, " FOUND"):
		virusName := ""
		if parts := strings.SplitN(response, ": ", 2); len(parts) == 2 {
			virusName = strings.TrimSuffix(parts[1], " FOUND")
		}
		return ResultInfected, virusName, nil
	default:
		return ResultUnavailable, "", fmt.Errorf("unexpected clamd response: %q", response)
	}
}
