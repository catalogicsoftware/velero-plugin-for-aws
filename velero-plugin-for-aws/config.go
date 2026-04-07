package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type configBuilder struct {
	log       logrus.FieldLogger
	opts      []func(*config.LoadOptions) error
	credsFlag bool
}

func newConfigBuilder(logger logrus.FieldLogger) *configBuilder {
	return &configBuilder{
		log: logger,
	}
}

func (cb *configBuilder) WithRegion(region string) *configBuilder {
	cb.opts = append(cb.opts, config.WithRegion(region))
	return cb
}

func (cb *configBuilder) WithProfile(profile string) *configBuilder {
	if profile == "" {
		cb.log.Info("WithProfile: no profile specified, AWS SDK will use default profile")
	} else {
		cb.log.Infof("WithProfile: configuring AWS SDK to use profile: [%s]", profile)
	}
	cb.opts = append(cb.opts, config.WithSharedConfigProfile(profile))
	return cb
}

func (cb *configBuilder) WithCredentialsFile(credentialsFile string) *configBuilder {
	// If no credentialsFile was passed explicitly, check the environment variable
	if credentialsFile == "" && os.Getenv("AWS_SHARED_CREDENTIALS_FILE") != "" {
		credentialsFile = os.Getenv("AWS_SHARED_CREDENTIALS_FILE")
		cb.log.Infof("WithCredentialsFile: no explicit credentials file provided, "+
			"falling back to AWS_SHARED_CREDENTIALS_FILE: %s", credentialsFile)
	}

	if credentialsFile == "" {
		cb.log.Info("WithCredentialsFile: no credentials file specified and " +
			"AWS_SHARED_CREDENTIALS_FILE is not set. " +
			"AWS SDK will use its default credential chain.")
		return cb
	}

	// Check file existence and accessibility before passing to AWS SDK
	fileInfo, statErr := os.Stat(credentialsFile)
	if statErr != nil {
		cb.log.Errorf("WithCredentialsFile: credentials file is not accessible: %s, "+
			"error: %v", credentialsFile, statErr)
	} else {
		cb.log.Infof("WithCredentialsFile: credentials file found: %s, "+
			"size: %d bytes, modTime: %v",
			credentialsFile, fileInfo.Size(), fileInfo.ModTime())

		// Read the file and log all section headers (never log key values
		// for security reasons)
		if data, readErr := os.ReadFile(credentialsFile); readErr != nil {
			cb.log.Errorf("WithCredentialsFile: could not read credentials "+
				"file for inspection: %s, error: %v", credentialsFile, readErr)
		} else {
			var sections []string
			for _, line := range strings.Split(string(data), "\n") {
				line = strings.TrimSpace(line)
				if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
					sections = append(sections, line)
				}
			}
			if len(sections) == 0 {
				cb.log.Infof("WithCredentialsFile: credentials file exists but "+
					"contains NO sections. File may be empty or malformed: %s",
					credentialsFile)
			} else {
				cb.log.Infof("WithCredentialsFile: credentials file sections "+
					"found: %v in file: %s", sections, credentialsFile)
			}
		}
	}

	cb.log.Infof("WithCredentialsFile: configuring AWS SDK with "+
		"SharedCredentialsFiles=[%s] and SharedConfigFiles=[%s]",
		credentialsFile, credentialsFile)

	cb.opts = append(cb.opts,
		config.WithSharedCredentialsFiles([]string{credentialsFile}),
		// SharedConfigFiles is set to support the use case where a config
		// file is passed as BSL credentials. Note: config file format
		// requires profiles to be prefixed with "profile " keyword,
		// e.g. [profile my-profile], whereas credentials file format
		// uses [my-profile] directly.
		config.WithSharedConfigFiles([]string{credentialsFile}))

	// Unset IRSA-related env vars to prevent role assumption
	// when an explicit credentials file is provided
	os.Setenv("AWS_WEB_IDENTITY_TOKEN_FILE", "")
	os.Setenv("AWS_ROLE_SESSION_NAME", "")
	os.Setenv("AWS_ROLE_ARN", "")
	cb.credsFlag = true

	return cb
}

func (cb *configBuilder) WithTLSSettings(insecureSkipTLSVerify bool, caCert string) *configBuilder {
	cb.opts = append(cb.opts, config.WithHTTPClient(awshttp.NewBuildableClient().WithTransportOptions(func(tr *http.Transport) {
		if tr.TLSClientConfig == nil {
			tr.TLSClientConfig = &tls.Config{}
		}
		if len(caCert) > 0 {
			var caCertPool *x509.CertPool
			caCertPool, err := x509.SystemCertPool()
			if err != nil {
				cb.log.Warnf("Failed to load system cert pool, using empty cert pool, err: %v", err)
				caCertPool = x509.NewCertPool()
			}
			caCertPool.AppendCertsFromPEM([]byte(caCert))
			tr.TLSClientConfig.RootCAs = caCertPool
		}
		tr.TLSClientConfig.InsecureSkipVerify = insecureSkipTLSVerify
	})))
	return cb
}

func (cb *configBuilder) Build() (aws.Config, error) {
	const maxAttempts = 5
	retryIntervals := []time.Duration{
		500 * time.Millisecond,
		1 * time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
	}

	cb.log.Infof("Build: starting config.LoadDefaultConfig with %d max attempts",
		maxAttempts)

	var conf aws.Config
	var lastErr error

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			cb.log.Infof("Build: retrying config.LoadDefaultConfig, "+
				"attempt %d/%d after %v",
				attempt+1, maxAttempts, retryIntervals[attempt-1])
			time.Sleep(retryIntervals[attempt-1])
		}

		conf, lastErr = config.LoadDefaultConfig(context.Background(), cb.opts...)
		if lastErr == nil {
			if attempt > 0 {
				cb.log.Infof("Build: config.LoadDefaultConfig succeeded "+
					"on attempt %d", attempt+1)
			} else {
				cb.log.Info("Build: config.LoadDefaultConfig succeeded " +
					"on first attempt")
			}
			break
		}

		cb.log.Errorf("Build: config.LoadDefaultConfig failed on attempt "+
			"%d/%d: %v", attempt+1, maxAttempts, lastErr)

		// If the error is not profile-related or credential-related,
		// do not retry — it will not recover with retries
		errStr := lastErr.Error()
		isRetryable := strings.Contains(errStr, "failed to get shared config profile") ||
			strings.Contains(errStr, "failed to load shared config") ||
			strings.Contains(errStr, "no such file or directory") ||
			strings.Contains(errStr, "failed to refresh cached credentials")

		if !isRetryable {
			cb.log.Errorf("Build: error is not retryable, aborting: %v", lastErr)
			return aws.Config{}, errors.WithStack(lastErr)
		}

		cb.log.Infof("Build: error is retryable (likely transient filesystem "+
			"or credentials visibility issue): %v", lastErr)
	}

	if lastErr != nil {
		cb.log.Errorf("Build: config.LoadDefaultConfig failed after all %d "+
			"attempts. Last error: %v", maxAttempts, lastErr)
		return aws.Config{}, errors.WithStack(lastErr)
	}

	if cb.credsFlag {
		cb.log.Info("Build: verifying credentials can be retrieved from config")
		if _, err := conf.Credentials.Retrieve(context.Background()); err != nil {
			cb.log.Errorf("Build: credential retrieval failed after "+
				"successful LoadDefaultConfig: %v", err)
			return aws.Config{}, errors.WithStack(err)
		}
		cb.log.Info("Build: credentials retrieved successfully")
	}

	return conf, nil
}

func newS3Client(cfg aws.Config, url string, forcePathStyle bool, signatureVersion string) (*s3.Client, error) {
	opts := []func(*s3.Options){
		func(o *s3.Options) {
			o.UsePathStyle = forcePathStyle
		},
	}
	if signatureVersion == "v2" {
		if !IsValidS3URLScheme(url) {
			return nil, errors.Errorf("Invalid s3 url %s, URL must be valid according to https://golang.org/pkg/net/url/#Parse and start with http:// or https://", url)
		}
		sigV2Signer := &SignatureV2Signer{}
		opts = append(opts, func(o *s3.Options) {
			o.EndpointResolver = s3.EndpointResolverFromURL(url)
			o.UsePathStyle = true
			o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
				return SigningMiddleware(sigV2Signer, cfg.Credentials)(stack)
			})
		})
	} else if url != "" {
		if !IsValidS3URLScheme(url) {
			return nil, errors.Errorf("Invalid s3 url %s, URL must be valid according to https://golang.org/pkg/net/url/#Parse and start with http:// or https://", url)
		}
		opts = append(opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(url)
		})
	}

	return s3.NewFromConfig(cfg, opts...), nil
}
