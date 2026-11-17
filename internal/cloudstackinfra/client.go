// Package cloudstackinfra discovers backup-target candidates from
// cloud-stack's infrastructure registry (GET /v1/infra). It only suggests
// candidates — nothing here creates a Target, so pointing backup-stack at a
// cloud-stack instance never silently starts backing up a host nobody
// asked for.
//
// It mirrors tickets/internal/cloudstack's client shape (New(baseURL,
// token), ErrNotConfigured when baseURL is empty) since both are separate
// repositories calling cloud-stack's public HTTP API rather than importing
// its internals.
package cloudstackinfra

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// ErrNotConfigured is returned when the client has no base URL.
var ErrNotConfigured = errors.New("cloudstackinfra: cloud-stack is not configured (set CLOUDSTACK_URL)")

// Candidate is one backup target backup-stack could create from an
// inventory host: a host with an ssh: field suggests an ssh-path target,
// and one with kubernetes.api suggests a k8s-namespace target. A host with
// both yields two candidates.
type Candidate struct {
	// Type is "ssh-path" or "k8s-namespace".
	Type string `json:"type"`
	// Host is the inventory host's name (e.g. "spark").
	Host string `json:"host"`
	// Address is the inventory host's address (e.g. "spark.local").
	Address string `json:"address"`
	// SSH is the host's ssh: field (set when Type is ssh-path).
	SSH string `json:"ssh,omitempty"`
	// K8sAPI is the host's kubernetes.api field (set when Type is
	// k8s-namespace).
	K8sAPI  string `json:"k8sApi,omitempty"`
	Purpose string `json:"purpose,omitempty"`
}

// Client discovers candidates from a cloud-stack instance's infra registry.
type Client struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
}

// New returns a client for baseURL; an empty baseURL yields a client whose
// every call fails with ErrNotConfigured.
func New(baseURL, token string) *Client {
	return &Client{BaseURL: strings.TrimRight(baseURL, "/"), Token: token, HTTP: &http.Client{Timeout: 15 * time.Second}}
}

// infraResponse is the subset of cloud-stack's GET /v1/infra this client
// reads.
type infraResponse struct {
	Inventory *struct {
		Hosts []struct {
			Name       string `json:"name"`
			Address    string `json:"address"`
			Purpose    string `json:"purpose,omitempty"`
			SSH        string `json:"ssh,omitempty"`
			Kubernetes *struct {
				Distro string `json:"distro,omitempty"`
				API    string `json:"api,omitempty"`
			} `json:"kubernetes,omitempty"`
		} `json:"hosts"`
	} `json:"inventory,omitempty"`
}

// ListCandidates fetches cloud-stack's infra inventory and returns the
// ssh-path and k8s-namespace candidates it implies.
func (c *Client) ListCandidates(ctx context.Context) ([]Candidate, error) {
	if c.BaseURL == "" {
		return nil, ErrNotConfigured
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/v1/infra", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("cloudstackinfra: unreachable: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, fmt.Errorf("cloudstackinfra: GET /v1/infra: %s", resp.Status)
	}
	var v infraResponse
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		return nil, fmt.Errorf("cloudstackinfra: decode: %w", err)
	}
	if v.Inventory == nil {
		return nil, nil
	}
	var out []Candidate
	for _, h := range v.Inventory.Hosts {
		if h.SSH != "" {
			out = append(out, Candidate{Type: "ssh-path", Host: h.Name, Address: h.Address, SSH: h.SSH, Purpose: h.Purpose})
		}
		if h.Kubernetes != nil && h.Kubernetes.API != "" {
			out = append(out, Candidate{Type: "k8s-namespace", Host: h.Name, Address: h.Address, K8sAPI: h.Kubernetes.API, Purpose: h.Purpose})
		}
	}
	return out, nil
}
