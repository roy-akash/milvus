package fabric

import (
	"context"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type FabricChunkManagerFactory struct {
	chunkManagerFactory *storage.ChunkManagerFactory
}

func NewFabricChunkManagerFactory(persistentStorage string, opts ...storage.Option) *FabricChunkManagerFactory {
	chunkMgrFactory := storage.NewChunkManagerFactory(persistentStorage, opts...)
	c := storage.NewDefaultConfig()
	for _, opt := range opts {
		opt(c)
	}
	return &FabricChunkManagerFactory{
		chunkManagerFactory: chunkMgrFactory,
	}
}

func (f *FabricChunkManagerFactory) newChunkManager(ctx context.Context, engine string) (storage.ChunkManager, error) {
	factory := f.chunkManagerFactory
	c := factory.Config
	return NewFabricRemoteChunkManager(ctx, c)
}

func (f *FabricChunkManagerFactory) NewPersistentStorageChunkManager(ctx context.Context) (storage.ChunkManager, error) {
	return f.newChunkManager(ctx, f.chunkManagerFactory.PersistentStorage)
}

func NewFabricChunkManagerFactoryWithParam(params *paramtable.ComponentParam) *FabricChunkManagerFactory {
	chunkManagerFactory := storage.NewChunkManagerFactoryWithParam(params)
	f := &FabricChunkManagerFactory{chunkManagerFactory: chunkManagerFactory}
	return f
}
