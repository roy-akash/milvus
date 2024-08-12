package indexnode

import (
	"context"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/storage"
)

type FabricChunkMgrFactory struct {
}

func (m *FabricChunkMgrFactory) NewChunkManager(ctx context.Context, config *indexpb.StorageConfig) (storage.ChunkManager, error) {
	chunkManagerFactory := storage.NewFabricChunkManagerFactory(config.GetStorageType(),
		storage.RootPath(config.GetRootPath()),
		storage.Address(config.GetAddress()),
		storage.AccessKeyID(config.GetAccessKeyID()),
		storage.SecretAccessKeyID(config.GetSecretAccessKey()),
		storage.UseSSL(config.GetUseSSL()),
		storage.SslCACert(config.GetSslCACert()),
		storage.BucketName(config.GetBucketName()),
		storage.UseIAM(config.GetUseIAM()),
		storage.CloudProvider(config.GetCloudProvider()),
		storage.IAMEndpoint(config.GetIAMEndpoint()),
		storage.UseVirtualHost(config.GetUseVirtualHost()),
		storage.RequestTimeout(config.GetRequestTimeoutMs()),
		storage.Region(config.GetRegion()),
		storage.CreateBucket(false),
	)
	return chunkManagerFactory.NewPersistentStorageChunkManager(ctx)
}
