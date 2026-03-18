package cluster

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"
)

func RunStatus(args []string) error {
	fs := flag.NewFlagSet("cluster status", flag.ContinueOnError)
	addr := fs.String("addr", "http://127.0.0.1:8090", "node or control-plane gateway address")
	if err := fs.Parse(args); err != nil {
		return err
	}

	var resp StatusResponse
	if err := requestJSON(http.MethodGet, joinURL(*addr, "/api/v1/status"), nil, &resp); err != nil {
		return err
	}
	return printJSON(resp)
}

func RunPut(args []string) error {
	fs := flag.NewFlagSet("put", flag.ContinueOnError)
	addr := fs.String("addr", "http://127.0.0.1:8090", "node or control-plane gateway address")
	key := fs.String("key", "", "key to write")
	value := fs.String("value", "", "value to write")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *key == "" {
		return fmt.Errorf("put requires --key")
	}

	var resp WriteResponse
	if err := requestJSON(http.MethodPut, joinURL(*addr, "/api/v1/kv/"+url.PathEscape(*key)), WriteRequest{
		Value: *value,
	}, &resp); err != nil {
		return err
	}
	return printJSON(resp)
}

func RunGet(args []string) error {
	fs := flag.NewFlagSet("get", flag.ContinueOnError)
	addr := fs.String("addr", "http://127.0.0.1:8090", "node or control-plane gateway address")
	key := fs.String("key", "", "key to read")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *key == "" {
		return fmt.Errorf("get requires --key")
	}

	var resp ReadResponse
	if err := requestJSON(http.MethodGet, joinURL(*addr, "/api/v1/kv/"+url.PathEscape(*key)), nil, &resp); err != nil {
		return err
	}
	return printJSON(resp)
}

func RunLockAcquire(args []string) error {
	fs := flag.NewFlagSet("lock acquire", flag.ContinueOnError)
	addr := fs.String("addr", "http://127.0.0.1:8090", "node or control-plane gateway address")
	name := fs.String("name", "maintenance", "lease name")
	owner := fs.String("owner", "operator", "lease owner label")
	ttl := fs.Duration("ttl", 30*time.Second, "lease TTL")
	if err := fs.Parse(args); err != nil {
		return err
	}

	var resp LeaseResponse
	if err := requestJSON(http.MethodPost, joinURL(*addr, "/api/v1/lock/acquire"), LeaseRequest{
		Name:       *name,
		Owner:      *owner,
		TTLSeconds: int64((*ttl) / time.Second),
	}, &resp); err != nil {
		return err
	}
	return printJSON(resp)
}

func RunLockRelease(args []string) error {
	fs := flag.NewFlagSet("lock release", flag.ContinueOnError)
	addr := fs.String("addr", "http://127.0.0.1:8090", "node or control-plane gateway address")
	name := fs.String("name", "maintenance", "lease name")
	owner := fs.String("owner", "operator", "lease owner label")
	if err := fs.Parse(args); err != nil {
		return err
	}

	var resp LeaseResponse
	if err := requestJSON(http.MethodPost, joinURL(*addr, "/api/v1/lock/release"), LeaseRequest{
		Name:  *name,
		Owner: *owner,
	}, &resp); err != nil {
		return err
	}
	return printJSON(resp)
}

func requestJSON(method, target string, body any, out any) error {
	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		reader = bytes.NewReader(payload)
	}

	req, err := http.NewRequest(method, target, reader)
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("request %s %s: %w", method, target, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 8*1024))
		return fmt.Errorf("%s returned %s: %s", target, resp.Status, string(bodyBytes))
	}

	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func printJSON(value any) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}
