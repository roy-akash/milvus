package storage

import (
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"
)

// Option for setting params used by chunk manager client.
type Config struct {
	address              string
	BucketName           string
	AccessKeyID          string
	SecretAccessKeyID    string
	useSSL               bool
	sslCACert            string
	createBucket         bool
	rootPath             string
	useIAM               bool
	cloudProvider        string
	iamEndpoint          string
	useVirtualHost       bool
	region               string
	requestTimeoutMs     int64
	gcpCredentialJSON    string
	gcpNativeWithoutAuth bool // used for Unit Testing
	SseKms               string
	SessionToken         string
}

func NewDefaultConfig() *Config {
	return &Config{}
}

func (c *Config) Clone() *Config {
	return &Config{
		address:              c.address,
		BucketName:           c.BucketName,
		AccessKeyID:          c.AccessKeyID,
		SecretAccessKeyID:    c.SecretAccessKeyID,
		useSSL:               c.useSSL,
		sslCACert:            c.sslCACert,
		createBucket:         c.createBucket,
		rootPath:             c.rootPath,
		useIAM:               c.useIAM,
		cloudProvider:        c.cloudProvider,
		iamEndpoint:          c.iamEndpoint,
		useVirtualHost:       c.useVirtualHost,
		region:               c.region,
		requestTimeoutMs:     c.requestTimeoutMs,
		gcpCredentialJSON:    c.gcpCredentialJSON,
		gcpNativeWithoutAuth: c.gcpNativeWithoutAuth, // used for Unit Testing
		SseKms:               c.SseKms,
		SessionToken:         c.SessionToken,
	}
}

// Option is used to config the retry function.
type Option func(*Config)

func Address(addr string) Option {
	return func(c *Config) {
		c.address = addr
	}
}

func BucketName(bucketName string) Option {
	return func(c *Config) {
		c.BucketName = bucketName
	}
}

func AccessKeyID(accessKeyID string) Option {
	return func(c *Config) {
		c.AccessKeyID = accessKeyID
	}
}

func SecretAccessKeyID(secretAccessKeyID string) Option {
	return func(c *Config) {
		c.SecretAccessKeyID = secretAccessKeyID
	}
}

func UseSSL(useSSL bool) Option {
	return func(c *Config) {
		c.useSSL = useSSL
	}
}

func SslCACert(sslCACert string) Option {
	return func(c *Config) {
		c.sslCACert = sslCACert
	}
}

func CreateBucket(createBucket bool) Option {
	return func(c *Config) {
		c.createBucket = createBucket
	}
}

func RootPath(rootPath string) Option {
	log.Info("rootPath", zap.String("rootPath", rootPath))
	return func(c *Config) {
		c.rootPath = rootPath
	}
}

func UseIAM(useIAM bool) Option {
	return func(c *Config) {
		c.useIAM = useIAM
	}
}

func CloudProvider(cloudProvider string) Option {
	return func(c *Config) {
		c.cloudProvider = cloudProvider
	}
}

func IAMEndpoint(iamEndpoint string) Option {
	return func(c *Config) {
		c.iamEndpoint = iamEndpoint
	}
}

func UseVirtualHost(useVirtualHost bool) Option {
	return func(c *Config) {
		c.useVirtualHost = useVirtualHost
	}
}

func Region(region string) Option {
	return func(c *Config) {
		c.region = region
	}
}

func RequestTimeout(requestTimeoutMs int64) Option {
	return func(c *Config) {
		c.requestTimeoutMs = requestTimeoutMs
	}
}

func GcpCredentialJSON(gcpCredentialJSON string) Option {
	return func(c *Config) {
		c.gcpCredentialJSON = gcpCredentialJSON
	}
}
