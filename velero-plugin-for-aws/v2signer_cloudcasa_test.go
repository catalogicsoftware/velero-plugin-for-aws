package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordedRequest struct {
	method string
	path   string
	header http.Header
	body   string
}

// fakeS3 answers the calls the object store makes and records each request.
type fakeS3 struct {
	mu       sync.Mutex
	requests []recordedRequest
	objects  map[string]string
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	f.requests = append(f.requests, recordedRequest{r.Method, r.URL.Path, r.Header.Clone(), string(body)})
	f.mu.Unlock()

	switch {
	case r.Method == http.MethodPut:
		f.mu.Lock()
		f.objects[r.URL.Path] = string(body)
		f.mu.Unlock()
		w.Header().Set("ETag", `"etag"`)
	case r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2":
		w.Header().Set("Content-Type", "application/xml")
		_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult><Name>bucket</Name><Prefix>backups/</Prefix><KeyCount>1</KeyCount><IsTruncated>false</IsTruncated>
<CommonPrefixes><Prefix>backups/b1/</Prefix></CommonPrefixes></ListBucketResult>`)
	case r.Method == http.MethodGet:
		f.mu.Lock()
		content, ok := f.objects[r.URL.Path]
		f.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = io.WriteString(w, content)
	case r.Method == http.MethodDelete:
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusOK)
	}
}

// A store with signatureVersion v2 signs every request with Signature Version 2
// and sends plain bodies; the SDK's v4 signing and chunked uploads must not leak in.
func TestObjectStoreSignsRequestsWithSignatureV2(t *testing.T) {
	fake := &fakeS3{objects: map[string]string{}}
	server := httptest.NewTLSServer(fake)
	defer server.Close()

	credsFile := filepath.Join(t.TempDir(), "cloud")
	require.NoError(t, os.WriteFile(credsFile,
		[]byte("[default]\naws_access_key_id=AKIDCLOUDCASA\naws_secret_access_key=secret\n"), 0600))

	store := newObjectStore(logrus.New())
	require.NoError(t, store.Init(map[string]string{
		regionKey:                "us-east-1",
		s3URLKey:                 server.URL,
		signatureVersionKey:      "v2",
		credentialsFileKey:       credsFile,
		insecureSkipTLSVerifyKey: "true",
		bucketKey:                "bucket",
		// kubeagent sends an empty checksum algorithm for every store that is not AWS.
		checksumAlgKey: "",
	}))

	// Velero hands the plugin a reader that cannot seek.
	require.NoError(t, store.PutObject("bucket", "backups/b1/b1.tar.gz", io.MultiReader(strings.NewReader("backup contents"))))

	rc, err := store.GetObject("bucket", "backups/b1/b1.tar.gz")
	require.NoError(t, err)
	content, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, "backup contents", string(content))

	prefixes, err := store.ListCommonPrefixes("bucket", "backups/", "/")
	require.NoError(t, err)
	assert.Equal(t, []string{"backups/b1/"}, prefixes)

	require.NoError(t, store.DeleteObject("bucket", "backups/b1/b1.tar.gz"))

	v2Auth := regexp.MustCompile(`^AWS AKIDCLOUDCASA:[A-Za-z0-9+/]+=*$`)
	fake.mu.Lock()
	defer fake.mu.Unlock()
	require.Len(t, fake.requests, 4)
	for _, r := range fake.requests {
		assert.Regexp(t, v2Auth, r.header.Get("Authorization"), "%s %s", r.method, r.path)
		assert.Empty(t, r.header.Get("X-Amz-Date"), "%s %s", r.method, r.path)
		assert.Empty(t, r.header.Get("X-Amz-Content-Sha256"), "%s %s", r.method, r.path)
		assert.NotEmpty(t, r.header.Get("Date"), "%s %s", r.method, r.path)
		assert.True(t, strings.HasPrefix(r.path, "/bucket"), "path-style request expected, got %s", r.path)
	}
	put := fake.requests[0]
	assert.Equal(t, http.MethodPut, put.method)
	assert.Equal(t, "backup contents", put.body)
	assert.NotContains(t, put.header.Get("Content-Encoding"), "aws-chunked")
}
