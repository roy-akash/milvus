package datacoord

import (
	"context"
	"github.com/milvus-io/milvus/internal/accessmanager"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"go.uber.org/zap"
	"strconv"
)

type fabricCompactionTrigger struct {
	*compactionTrigger
}

func newFabricCompactionTrigger(compactionTrigger *compactionTrigger) *fabricCompactionTrigger {
	return &fabricCompactionTrigger{compactionTrigger}
}

func (t *fabricCompactionTrigger) forceTriggerCompaction(collectionID int64) (UniqueID, error) {
	log.Info("Force refresh credentials for the collection.", zap.String("collection id ", strconv.FormatInt(collectionID, 10)))
	_, err := accessmanager.GetCredentialsForCollection(
		context.Background(),
		"",
		strconv.FormatInt(collectionID, 10),
		paramtable.Get().MinioCfg.BucketName.GetValue(),
		true,
	)
	if err != nil {
		log.Warn("Unable to refresh access credentials", zap.Error(err))
	}
	return t.compactionTrigger.forceTriggerCompaction(collectionID)
}
