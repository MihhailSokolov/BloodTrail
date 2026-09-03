// SPDX-License-Identifier: Apache-2.0

package verify

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Fixture holds a small SharpHound v6 collection of the fictional TESTLAB.LOCAL domain.
//
//go:embed fixture/*.json
var Fixture embed.FS

const (
	jobStatusComplete          = 2
	jobStatusFailed            = 5
	jobStatusPartiallyComplete = 8
	fixtureDomain              = "TESTLAB.LOCAL"
)

// Smoke ingests the fixture through the public API and checks it can be found.
type Smoke struct {
	Client   *http.Client
	BaseURL  string
	User     string
	Password string
	Poll     time.Duration // defaults to 5s
	token    string
}

// Run performs login, upload, wait for ingest and analysis, and search.
func (s *Smoke) Run(ctx context.Context, timeout time.Duration) error {
	if s.Poll == 0 {
		s.Poll = 5 * time.Second
	}
	if err := s.login(ctx); err != nil {
		return err
	}
	jobID, err := s.startJob(ctx)
	if err != nil {
		return err
	}
	if err := s.uploadFixture(ctx, jobID); err != nil {
		return err
	}
	if _, err := s.call(ctx, http.MethodPost, fmt.Sprintf("/api/v2/file-upload/%d/end", jobID), "", nil); err != nil {
		return err
	}
	if err := s.waitForJob(ctx, jobID, timeout); err != nil {
		return err
	}
	if err := s.waitForDatapipeIdle(ctx, timeout); err != nil {
		return err
	}
	return s.search(ctx)
}

func (s *Smoke) call(ctx context.Context, method, path, contentType string, body io.Reader, headers ...string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(s.BaseURL, "/")+path, body)
	if err != nil {
		return nil, err
	}
	if s.token != "" {
		req.Header.Set("Authorization", "Bearer "+s.token)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := s.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%s %s: HTTP %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(data)))
	}
	return data, nil
}

func (s *Smoke) login(ctx context.Context) error {
	body, _ := json.Marshal(map[string]string{"login_method": "secret", "username": s.User, "secret": s.Password})
	data, err := s.call(ctx, http.MethodPost, "/api/v2/login", "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("login: %w", err)
	}
	var resp struct {
		Data struct {
			SessionToken string `json:"session_token"`
		} `json:"data"`
	}
	if err := json.Unmarshal(data, &resp); err != nil || resp.Data.SessionToken == "" {
		return fmt.Errorf("login: no session token in response %q", data)
	}
	s.token = resp.Data.SessionToken
	return nil
}

func (s *Smoke) startJob(ctx context.Context) (int64, error) {
	data, err := s.call(ctx, http.MethodPost, "/api/v2/file-upload/start", "", nil)
	if err != nil {
		return 0, err
	}
	var resp struct {
		Data struct {
			ID int64 `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(data, &resp); err != nil || resp.Data.ID == 0 {
		return 0, fmt.Errorf("start ingest job: unexpected response %q", data)
	}
	return resp.Data.ID, nil
}

func (s *Smoke) uploadFixture(ctx context.Context, jobID int64) error {
	entries, err := Fixture.ReadDir("fixture")
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		content, err := Fixture.ReadFile("fixture/" + e.Name())
		if err != nil {
			return err
		}
		if _, err := s.call(ctx, http.MethodPost, fmt.Sprintf("/api/v2/file-upload/%d", jobID), "application/json",
			bytes.NewReader(content), "X-File-Upload-Name", e.Name()); err != nil {
			return fmt.Errorf("upload %s: %w", e.Name(), err)
		}
	}
	return nil
}

func (s *Smoke) waitForJob(ctx context.Context, jobID int64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		data, err := s.call(ctx, http.MethodGet, "/api/v2/file-upload", "", nil)
		if err != nil {
			return err
		}
		var resp struct {
			Data []struct {
				ID            int64  `json:"id"`
				Status        int    `json:"status"`
				StatusMessage string `json:"status_message"`
				FailedFiles   int    `json:"failed_files"`
			} `json:"data"`
		}
		if err := json.Unmarshal(data, &resp); err != nil {
			return fmt.Errorf("decoding ingest jobs: %w", err)
		}
		for _, job := range resp.Data {
			if job.ID != jobID {
				continue
			}
			switch job.Status {
			case jobStatusComplete, jobStatusPartiallyComplete:
				if job.FailedFiles > 0 {
					return fmt.Errorf("ingest job %d finished with %d failed files: %s", jobID, job.FailedFiles, job.StatusMessage)
				}
				return nil
			case jobStatusFailed:
				return fmt.Errorf("ingest job %d failed: %s", jobID, job.StatusMessage)
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("ingest job %d not complete after %s", jobID, timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.Poll):
		}
	}
}

func (s *Smoke) waitForDatapipeIdle(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		data, err := s.call(ctx, http.MethodGet, "/api/v2/datapipe/status", "", nil)
		if err != nil {
			return err
		}
		var resp struct {
			Data struct {
				Status string `json:"status"`
			} `json:"data"`
		}
		if err := json.Unmarshal(data, &resp); err != nil {
			return fmt.Errorf("decoding datapipe status: %w", err)
		}
		if resp.Data.Status == "idle" {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("datapipe still %q after %s", resp.Data.Status, timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.Poll):
		}
	}
}

func (s *Smoke) search(ctx context.Context) error {
	data, err := s.call(ctx, http.MethodGet, "/api/v2/search?q="+url.QueryEscape(fixtureDomain), "", nil)
	if err != nil {
		return err
	}
	var resp struct {
		Data []struct {
			Name string `json:"name"`
		} `json:"data"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return fmt.Errorf("decoding search: %w", err)
	}
	for _, r := range resp.Data {
		if strings.EqualFold(r.Name, fixtureDomain) {
			return nil
		}
	}
	return fmt.Errorf("search for %s returned no matching node; ingest did not reach the graph", fixtureDomain)
}
