// SPDX-License-Identifier: Apache-2.0

package toolapi

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/MihhailSokolov/BloodTrail/internal/dockerx"
)

// CurlImage is pinned so behaviour is reproducible.
const CurlImage = "docker.io/curlimages/curl:8.10.1"

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
		"-sS", "-o", "-", "-w", "\n%{http_code}", "-X", method, url)
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
