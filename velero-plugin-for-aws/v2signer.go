package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyendpoints "github.com/aws/smithy-go/endpoints"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// SignatureV2Signer implements AWS Signature Version 2
type SignatureV2Signer struct{}

// SignHTTP signs an HTTP request using Signature Version 2
func (s *SignatureV2Signer) SignHTTP(req *http.Request, accessKey, secretKey string) error {
	// Add date header if not present
	if req.Header.Get("Date") == "" {
		req.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	}

	// Build the string to sign
	stringToSign := s.buildStringToSign(req)

	// Calculate signature
	mac := hmac.New(sha1.New, []byte(secretKey))
	mac.Write([]byte(stringToSign))
	signature := base64.StdEncoding.EncodeToString(mac.Sum(nil))

	// Add authorization header
	authHeader := fmt.Sprintf("AWS %s:%s", accessKey, signature)
	req.Header.Set("Authorization", authHeader)

	return nil
}

func (s *SignatureV2Signer) buildStringToSign(req *http.Request) string {
	// Format: HTTP-Verb + "\n" +
	//         Content-MD5 + "\n" +
	//         Content-Type + "\n" +
	//         Date + "\n" +
	//         CanonicalizedAmzHeaders +
	//         CanonicalizedResource

	var parts []string

	// HTTP Verb
	parts = append(parts, req.Method)

	// Content-MD5
	parts = append(parts, req.Header.Get("Content-MD5"))

	// Content-Type
	parts = append(parts, req.Header.Get("Content-Type"))

	// Date
	parts = append(parts, req.Header.Get("Date"))

	// Canonicalized AMZ Headers
	amzHeaders := s.canonicalizeAmzHeaders(req.Header)
	if amzHeaders != "" {
		parts = append(parts, amzHeaders)
	}

	// Canonicalized Resource
	resource := s.canonicalizeResource(req)

	stringToSign := strings.Join(parts, "\n")
	if amzHeaders == "" {
		stringToSign += "\n"
	}
	stringToSign += resource

	return stringToSign
}

func (s *SignatureV2Signer) canonicalizeAmzHeaders(headers http.Header) string {
	var amzHeaders []string
	headerMap := make(map[string][]string)

	// Headers to exclude for Google Cloud Storage compatibility
	excludeHeaders := map[string]bool{
		"x-amz-decoded-content-length": true,
		"x-amz-trailer":                true,
	}

	for key, values := range headers {
		lowerKey := strings.ToLower(key)
		if strings.HasPrefix(lowerKey, "x-amz-") && !excludeHeaders[lowerKey] {
			amzHeaders = append(amzHeaders, lowerKey)
			headerMap[lowerKey] = values
		}
	}

	if len(amzHeaders) == 0 {
		return ""
	}

	sort.Strings(amzHeaders)

	var canonicalized []string
	for _, key := range amzHeaders {
		values := headerMap[key]
		// Trim whitespace from values and join with comma
		var trimmedValues []string
		for _, v := range values {
			trimmedValues = append(trimmedValues, strings.TrimSpace(v))
		}
		canonicalized = append(canonicalized, fmt.Sprintf("%s:%s", key, strings.Join(trimmedValues, ",")))
	}

	return strings.Join(canonicalized, "\n")
}

func (s *SignatureV2Signer) canonicalizeResource(req *http.Request) string {
	path := req.URL.Path
	if path == "" {
		path = "/"
	}

	// Only include standard AWS subresources in the canonicalized resource
	// For GCS, we should only include the actual subresources, not query parameters
	query := req.URL.Query()
	if len(query) > 0 {
		// List of standard AWS subresources (not including query parameters like list-type, max-keys, etc.)
		subresources := []string{"acl", "lifecycle", "location", "logging", "notification",
			"policy", "requestPayment", "torrent", "uploadId", "uploads", "versionId",
			"versioning", "versions", "website", "delete", "cors", "restore", "tagging"}

		var subresourceParams []string
		for _, sr := range subresources {
			if query.Has(sr) {
				val := query.Get(sr)
				if val != "" {
					subresourceParams = append(subresourceParams, fmt.Sprintf("%s=%s", sr, val))
				} else {
					subresourceParams = append(subresourceParams, sr)
				}
			}
		}

		if len(subresourceParams) > 0 {
			sort.Strings(subresourceParams)
			path += "?" + strings.Join(subresourceParams, "&")
		}
	}

	return path
}

// CustomEndpointResolverV2 implements EndpointResolverV2 for custom endpoints
type CustomEndpointResolverV2 struct {
	URL string
}

func (r *CustomEndpointResolverV2) ResolveEndpoint(ctx context.Context, params s3.EndpointParameters) (smithyendpoints.Endpoint, error) {
	u, err := url.Parse(r.URL)
	if err != nil {
		return smithyendpoints.Endpoint{}, fmt.Errorf("failed to parse endpoint URL: %w", err)
	}

	// For path-style addressing, prepend the bucket to the path
	if params.Bucket != nil {
		u.Path = "/" + *params.Bucket + u.Path
	}

	return smithyendpoints.Endpoint{
		URI: *u,
	}, nil
}

// SigningMiddleware creates a middleware that signs requests with Signature V2
// This is necessary because AWS SDK v2 doesn't support Signature V2 natively
func SigningMiddleware(signer *SignatureV2Signer, credsProvider aws.CredentialsProvider) func(*middleware.Stack) error {
	return func(stack *middleware.Stack) error {
		// Remove only the AWS SigV4 signing middleware by ID
		// This is more surgical than clearing the entire stack
		stack.Finalize.Remove("Signing")

		// Add our custom Signature V2 signing middleware at the end of Finalize
		return stack.Finalize.Add(
			middleware.FinalizeMiddlewareFunc(
				"SignatureV2SigningMiddleware",
				func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
					// Get the HTTP request
					req, ok := in.Request.(*smithyhttp.Request)
					if !ok {
						return next.HandleFinalize(ctx, in)
					}

					// Retrieve credentials from the provider
					creds, err := credsProvider.Retrieve(ctx)
					if err != nil {
						return middleware.FinalizeOutput{}, middleware.Metadata{}, fmt.Errorf("failed to retrieve credentials: %w", err)
					}

					// Remove any SigV4 and AWS-specific headers that might have been added
					req.Header.Del("Authorization")
					req.Header.Del("X-Amz-Date")
					req.Header.Del("X-Amz-Security-Token")
					req.Header.Del("X-Amz-Content-Sha256")
					req.Header.Del("X-Amz-Decoded-Content-Length")
					req.Header.Del("X-Amz-Trailer")

					// Sign the request with Signature V2 using the retrieved credentials
					if err := signer.SignHTTP(req.Request, creds.AccessKeyID, creds.SecretAccessKey); err != nil {
						return middleware.FinalizeOutput{}, middleware.Metadata{}, fmt.Errorf("failed to sign request: %w", err)
					}

					// Continue with the request
					return next.HandleFinalize(ctx, in)
				},
			),
			middleware.After,
		)
	}
}
