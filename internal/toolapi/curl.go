// SPDX-License-Identifier: Apache-2.0

package toolapi

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/MihhailSokolov/BloodTrail/internal/dockerx"
)

// CurlImage is pinned so behaviour is reproducible.
const CurlImage = "docker.io/curlimages/curl:8.10.1"

// Every request is bounded on curl's own side. The tool API answers these
// calls in milliseconds -- PUT /pg-migration/neo-to-pg starts the migration
// and returns, it does not run it -- so a request still outstanding after
// these limits means the server is wedged, not busy.
//
// Without them a hung server hangs the whole install indefinitely: the
// installer's context carries no deadline, and WaitUntilIdle only compares
// against --migration-timeout between polls, never during one, so a poll
// that never returns is never timed out by anything.
const (
	connectTimeout = 15 * time.Second
	requestTimeout = 120 * time.Second
)

// CurlContainerTransport reaches a port that is not published on the host by
// running curl in a throwaway container attached to the compose network. The
// upstream BloodHound image is distroless, so exec-ing into it is not an option.
type CurlContainerTransport struct {
	Runner  dockerx.Runner
	Network string
	Image   string
}

func (s CurlContainerTransport) Do(ctx context.Context, method, url string) (int, []byte, error) {
	image := s.Image
	if image == "" {
		image = CurlImage
	}
	out, err := s.Runner.Run(ctx, nil, "docker", "run", "--rm", "--network", s.Network, image,
		"-sS", "-o", "-", "-w", "\n%{http_code}",
		"--connect-timeout", strconv.Itoa(int(connectTimeout.Seconds())),
		"--max-time", strconv.Itoa(int(requestTimeout.Seconds())),
		"-X", method, url)
	if err != nil {
		return 0, nil, fmt.Errorf("curl container: %w", err)
	}
	text := string(out)
	cut := strings.LastIndex(text, "\n")
	if cut < 0 {
		return 0, nil, fmt.Errorf("curl container: no status code in output %q", text)
	}
	status, err := strconv.Atoi(strings.TrimSpace(text[cut+1:]))
	if err != nil {
		return 0, nil, fmt.Errorf("curl container: bad status code in output %q", text)
	}
	return status, []byte(text[:cut]), nil
}
