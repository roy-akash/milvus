package rootcoord

import (
	"context"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/accessmanager"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"go.uber.org/zap"
)

type FabricCore struct {
	*Core
}

func NewFabricCore(c context.Context, factory dependency.Factory) (*FabricCore, error) {
	core, err := NewCore(c, factory)
	if err != nil {
		return nil, err
	}
	return &FabricCore{Core: core}, nil
}

func (c *FabricCore) DropCollection(ctx context.Context, in *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
	log.Info("Force refresh credentials for the collection.", zap.String("collection ", in.CollectionName))
	_, err := accessmanager.GetCredentialsForCollection(
		ctx,
		in.CollectionName,
		"",
		paramtable.Get().MinioCfg.BucketName.GetValue(),
		true,
	)
	if err != nil {
		log.Warn("Unable to refresh access credentials", zap.Error(err))
	}
	return c.Core.DropCollection(ctx, in)
}
