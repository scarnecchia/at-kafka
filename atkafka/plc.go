package atkafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/bluesky-social/indigo/atproto/identity"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/bluesky-social/indigo/pkg/robusthttp"
	lru "github.com/hashicorp/golang-lru/v2/expirable"
	"golang.org/x/time/rate"
)

type PlcClient struct {
	client     *http.Client
	dir        *identity.CacheDirectory
	auditCache *lru.LRU[string, *DidAuditEntry]
	plcHost    string
}

type PlcClientArgs struct {
	PlcHost string
}

func NewPlcClient(args *PlcClientArgs) *PlcClient {
	client := robusthttp.NewClient(robusthttp.WithMaxRetries(2))
	client.Timeout = 3 * time.Second

	baseDirectory := identity.BaseDirectory{
		PLCURL: "https://plc.directory",
		HTTPClient: http.Client{
			Timeout: time.Second * 5,
		},
		PLCLimiter:            rate.NewLimiter(rate.Limit(200), 100),
		TryAuthoritativeDNS:   true,
		SkipDNSDomainSuffixes: []string{".bsky.social", ".staging.bsky.dev"},
	}
	directory := identity.NewCacheDirectory(&baseDirectory, 100_000, time.Hour*48, time.Minute*15, time.Minute*15)

	auditCache := lru.NewLRU(100_000, func(_ string, _ *DidAuditEntry) {
		cacheSize.WithLabelValues("audit_log").Dec()
	}, 1*time.Hour)

	return &PlcClient{
		client:     client,
		dir:        &directory,
		auditCache: auditCache,
		plcHost:    args.PlcHost,
	}
}

type OperationService struct {
	Type     string `json:"type"`
	Endpoint string `json:"endpoint"`
}

type DidLogEntry struct {
	Sig                 string                      `json:"sig"`
	Prev                *string                     `json:"prev"`
	Type                string                      `json:"type"`
	Services            map[string]OperationService `json:"services"`
	AlsoKnownAs         []string                    `json:"alsoKnownAs"`
	RotationKeys        []string                    `json:"rotationKeys"`
	VerificationMethods map[string]string           `json:"verificationMethods"`
}

type DidAuditEntry struct {
	Did       string      `json:"did"`
	Operation DidLogEntry `json:"operation"`
	Cid       string      `json:"cid"`
	Nullified bool        `json:"nullified"`
	CreatedAt string      `json:"createdAt"`
}

type DidAuditLog []DidAuditEntry

func (c *PlcClient) GetIdentity(ctx context.Context, did syntax.DID) (*identity.Identity, error) {
	status := "error"

	defer func() {
		plcRequests.WithLabelValues("did_doc", status, "unknown").Inc()
	}()

	identity, err := c.dir.LookupDID(ctx, did)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup DID: %w", err)
	}

	status = "ok"

	return identity, nil
}

var ErrAuditLogNotFound = errors.New("audit log not found for DID")

func (c *PlcClient) GetDIDAuditLog(ctx context.Context, did syntax.DID) (*DidAuditEntry, error) {
	status := "error"
	cached := false

	if did.Method() != "plc" {
		return nil, ErrAuditLogNotFound
	}

	didString := did.String()

	defer func() {
		plcRequests.WithLabelValues("audit_log", status, fmt.Sprintf("%t", cached)).Inc()
	}()

	if val, ok := c.auditCache.Get(didString); ok {
		status = "ok"
		cached = true
		return val, nil
	}

	ustr := fmt.Sprintf("%s/%s/log/audit", c.plcHost, didString)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ustr, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create http request for DID audit log: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch DID audit log: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		if resp.StatusCode == http.StatusNotFound {
			return nil, ErrAuditLogNotFound
		}
		return nil, fmt.Errorf("DID audit log fetch returned unexpected status: %s", resp.Status)
	}

	var entries []DidAuditEntry
	if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
		return nil, fmt.Errorf("failed to read DID audit log response bytes into DidAuditLog: %w", err)
	}

	if len(entries) == 0 {
		return nil, fmt.Errorf("empty did audit log for did %s", didString)
	}

	entry := entries[0]

	if c.auditCache != nil {
		c.auditCache.Add(didString, &entry)
	}

	cacheSize.WithLabelValues("audit_log").Inc()
	status = "ok"

	return &entry, nil
}
