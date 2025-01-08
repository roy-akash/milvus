package fabric

import (
	"context"
	"fmt"
	"github.com/aliyun/credentials-go/credentials/utils"
	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/accessmanager"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"go.uber.org/zap"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type TransientFabricRemoteChunkManager struct {
	chunkManager        *storage.RemoteChunkManager
	expirationTimestamp string
}

type FabricRemoteChunkManager struct {
	*storage.RemoteChunkManager
	globalTransientRemoteChunkManager *TransientFabricRemoteChunkManager
	chunkManagers                     map[int64]*TransientFabricRemoteChunkManager
	config                            *storage.Config
	chunkManagerMutex                 sync.Mutex
}

var (
	fabricRemoteChunkManager      *FabricRemoteChunkManager
	fabricRemoteChunkManagerMutex sync.Mutex
)

var params *paramtable.ComponentParam = paramtable.Get()

func NewFabricRemoteChunkManager(ctx context.Context, c *storage.Config) (*FabricRemoteChunkManager, error) {
	if fabricRemoteChunkManager == nil {
		log.Debug("Thread waiting to get the NewFabricRemoteChunkManager lock")
		fabricRemoteChunkManagerMutex.Lock()
		defer fabricRemoteChunkManagerMutex.Unlock()
		if fabricRemoteChunkManager == nil {
			log.Debug("Thread entered lock to init NewFabricRemoteChunkManager")
			globalTransientRemoteChunkManager, err := upsertGlobalChunkManager(ctx, c)
			if err != nil {
				log.Error("Error while initializing NewFabricRemoteChunkManager", zap.Error(err))
				return nil, err
			}
			fabricRemoteChunkManager = &FabricRemoteChunkManager{
				RemoteChunkManager:                globalTransientRemoteChunkManager.chunkManager,
				globalTransientRemoteChunkManager: globalTransientRemoteChunkManager,
				chunkManagers:                     map[int64]*TransientFabricRemoteChunkManager{},
				config:                            c,
			}
		}
	}
	log.Debug("Returning Fabric remote chunk manager")
	return fabricRemoteChunkManager, nil
}

func (mcm *FabricRemoteChunkManager) Path(ctx context.Context, filePath string) (string, error) {
	log.Debug("Path function called for path ", zap.String("filePath", filePath))
	rcm, err := mcm.getChunkManager(ctx, filePath)
	if err != nil {
		return "", err
	}
	return rcm.Path(ctx, filePath)
}

func (mcm *FabricRemoteChunkManager) Size(ctx context.Context, filePath string) (int64, error) {
	log.Debug("Size function called for path ", zap.String("filePath", filePath))
	rcm, err := mcm.getChunkManager(ctx, filePath)
	if err != nil {
		return 0, err
	}
	return rcm.Size(ctx, filePath)
}

func (mcm *FabricRemoteChunkManager) Exist(ctx context.Context, filePath string) (bool, error) {
	log.Debug("Exist function called for path ", zap.String("filePath", filePath))
	rcm, err := mcm.getChunkManager(ctx, filePath)
	if err != nil {
		return false, err
	}
	return rcm.Exist(ctx, filePath)
}

func (mcm *FabricRemoteChunkManager) ReadAt(ctx context.Context, filePath string, off int64, length int64) (p []byte, err error) {
	log.Debug("ReadAt function called for filePath ", zap.String("filePath", filePath))
	rcm, err := mcm.getChunkManager(ctx, filePath)
	if err != nil {
		return nil, err
	}
	return rcm.ReadAt(ctx, filePath, off, length)
}

func (mcm *FabricRemoteChunkManager) MultiRemove(ctx context.Context, keys []string) error {
	log.Debug("MultiRemove function called for filePath(s) ", zap.Strings("filePath", keys))
	var el error
	for _, filePath := range keys {
		err := mcm.Remove(ctx, filePath)
		if err != nil {
			el = merr.Combine(el, errors.Wrapf(err, "failed to remove %s", filePath))
		}
	}
	return el
}

func (mcm *FabricRemoteChunkManager) MultiWrite(ctx context.Context, kvs map[string][]byte) error {
	log.Debug("MultiWrite called ")
	var el error
	for filePath, value := range kvs {
		err := mcm.Write(ctx, filePath, value)
		if err != nil {
			el = merr.Combine(el, errors.Wrapf(err, "failed to write %s", filePath))
		}
	}
	return el
}

func (mcm *FabricRemoteChunkManager) Write(ctx context.Context, filePath string, content []byte) error {

	uuid := utils.GetUUID()

	log.Debug("Write called for path ", zap.String("filePath", filePath), zap.String("uuid", uuid))
	requestStartTime := time.Now().UTC()
	collID, err := mcm.retrieveCollectionIDFromFilepath(filePath)
	trcm, err := mcm.getNewChunkManager(ctx, collID)

	chunkManagerCreationTime := time.Now().UTC()
	timeTakenToCreateNewChunkManager := chunkManagerCreationTime.Sub(requestStartTime).Milliseconds()
	log.Info("Time taken to get new chunkManager", zap.Int64("timeTakenToCreateNewChunkManager",
		timeTakenToCreateNewChunkManager), zap.String("uuid", uuid))

	if err != nil {
		return err
	}
	writeErr := trcm.chunkManager.Write(ctx, filePath, content)
	if writeErr == nil {
		timeTakenInMilliSecondsToJustWrite := time.Now().UTC().Sub(chunkManagerCreationTime).Milliseconds()
		timeTakenInMilliSecondsForCompleteWrite := time.Now().UTC().Sub(requestStartTime).Milliseconds()
		log.Info("Time taken to write",
			zap.Int64("timeTakenInMilliSecondsToJustWrite", timeTakenInMilliSecondsToJustWrite),
			zap.Int64("timeTakenInMilliSecondsForCompleteWrite", timeTakenInMilliSecondsForCompleteWrite),
			zap.Int64("timeTakenToCreateNewChunkManager", timeTakenToCreateNewChunkManager),
			zap.String("uuid", uuid))
	} else {
		log.Error("Failed to write path ", zap.String("filePath", filePath), zap.String("uuid", uuid))
	}

	return writeErr
}

func (mcm *FabricRemoteChunkManager) Read(ctx context.Context, filePath string) ([]byte, error) {
	log.Debug("Read called for path ", zap.String("filePath", filePath))
	rcm, err := mcm.getChunkManager(ctx, filePath)
	if err != nil {
		return nil, err
	}
	return rcm.Read(ctx, filePath)
}

func (mcm *FabricRemoteChunkManager) MultiRead(ctx context.Context, keys []string) ([][]byte, error) {
	log.Debug("MultiRead called ")
	var el error
	objectsValues := make([][]byte, 0, len(keys))
	for _, filePath := range keys {
		log.Info("Data to be written to ", zap.String("filePath", filePath))
		var objectValue []byte
		objectValue, err := mcm.Read(ctx, filePath)
		if err != nil {
			el = merr.Combine(el, errors.Wrapf(err, "failed to read filePath:%s", filePath))
		} else {
			objectsValues = append(objectsValues, objectValue)
		}
	}

	return objectsValues, el
}

// Remove deletes an object with @key.
func (mcm *FabricRemoteChunkManager) Remove(ctx context.Context, filePath string) error {
	log.Debug("Remove called for path ", zap.String("filePath", filePath))
	rcm, err := mcm.getChunkManager(ctx, filePath)
	if err != nil {
		return err
	}
	return rcm.Remove(ctx, filePath)
}

// RemoveWithPrefix removes all objects with the same prefix @prefix from minio.
func (mcm *FabricRemoteChunkManager) RemoveWithPrefix(ctx context.Context, prefix string) error {
	log.Debug("RemoveWithPrefix called for path ", zap.String("prefix", prefix))
	rcm, err := mcm.getChunkManager(ctx, prefix)
	if err != nil {
		return err
	}
	return rcm.RemoveWithPrefix(ctx, prefix)
}

func (mcm *FabricRemoteChunkManager) ListWithPrefix(ctx context.Context, prefix string, recursive bool) ([]string, []time.Time, error) {
	log.Debug("ListWithPrefix called for path ", zap.String("prefix", prefix))

	//always use global chunk manager
	gcm, err := mcm.getGlobalChunkManager(ctx)
	if err != nil {
		return nil, nil, err
	}
	return gcm.chunkManager.ListWithPrefix(ctx, prefix, recursive)
}

func (mcm *FabricRemoteChunkManager) retrieveCollectionIDFromFilepath(filePath string) (int64, error) {
	log.Info("Retrieving collection id from filePath.", zap.String("filePath", "filePath"))
	collectionIdIndex := strings.Count(params.MinioCfg.RootPath.GetValue(), "/") + 2
	log.Info("collection id index", zap.Int("collectionIdIndex", collectionIdIndex))
	collId, err := strconv.ParseInt(strings.Split(filePath, "/")[collectionIdIndex], 10, 64)
	if err != nil {
		log.Error("error occurred while trying to derive collection id", zap.String("filePath", filePath), zap.Error(err))
	}
	return collId, err
}

func upsertGlobalChunkManager(ctx context.Context, c *storage.Config) (*TransientFabricRemoteChunkManager, error) {

	log.Debug("Initializing global chunk manager")

	//TODO add retries
	accessCredentials, err := accessmanager.GetGlobalCredentials(
		ctx,
		c.BucketName,
	)

	if err != nil {
		return nil, err
	}

	// cloned the config to be used for this new chunk manager object
	newConfig := c.Clone()

	newConfig.AccessKeyID = accessCredentials.AccessKeyID
	newConfig.SecretAccessKeyID = accessCredentials.SecretAccessKey
	newConfig.SessionToken = accessCredentials.SessionToken

	remoteChunkManager, _ := storage.NewRemoteChunkManager(ctx, newConfig)

	transientChunkManager := &TransientFabricRemoteChunkManager{
		remoteChunkManager,
		accessCredentials.ExpirationTimestamp,
	}

	return transientChunkManager, nil
}

func (mcm *FabricRemoteChunkManager) getGlobalChunkManager(ctx context.Context) (*TransientFabricRemoteChunkManager, error) {
	if mcm.globalTransientRemoteChunkManager.chunkManager == nil || mcm.isChunkManagerExpired(mcm.globalTransientRemoteChunkManager, 0) {
		fabricRemoteChunkManagerMutex.Lock()
		defer fabricRemoteChunkManagerMutex.Unlock()

		if mcm.globalTransientRemoteChunkManager.chunkManager == nil || mcm.isChunkManagerExpired(mcm.globalTransientRemoteChunkManager, 0) {
			transientChunkManager, err := upsertGlobalChunkManager(ctx, mcm.config)
			if err != nil {
				log.Error("Error occurred while trying to initialize global chunk manager")
				return nil, err
			}
			mcm.globalTransientRemoteChunkManager = transientChunkManager
		}

	}
	return mcm.globalTransientRemoteChunkManager, nil
}

func (mcm *FabricRemoteChunkManager) getChunkManager(ctx context.Context, filePath string) (*storage.RemoteChunkManager, error) {
	collID, err := mcm.retrieveCollectionIDFromFilepath(filePath)
	if err != nil {
		return nil, err
	}
	log.Info("getting chunk manager for collection id", zap.Int64("collID", collID))
	if !mcm.isValidChunkManagerPresent(collID) {
		err = mcm.upsertChunkManager(ctx, collID)
	}
	if err != nil {
		log.Error("Error while loading chunk manager for collection id", zap.Int64("collectionId", collID))
		return nil, errors.Wrapf(err, "Error while loading chunk manager for collection id %s", collID)
	}
	return mcm.chunkManagers[collID].chunkManager, nil
}

/*
This check returns true only if there is a chunk manager in the map and the credentials for that chunk manager
don't need to be refreshed yet
*/
func (mcm *FabricRemoteChunkManager) isValidChunkManagerPresent(collID int64) bool {
	transientChunkManager, ok := mcm.chunkManagers[collID]
	if ok {
		// check if the chunk manager needs to be refreshed
		log.Debug("Checking if the chunk manager expired for collection id", zap.Int64("collectionId", collID))
		validChunkManagerPresent := !mcm.isChunkManagerExpired(transientChunkManager, collID)
		log.Debug("Chunk manager state for collection id", zap.Bool("validChunkManagerPresent", validChunkManagerPresent))

		return validChunkManagerPresent
	}
	return false
}

func (mcm *FabricRemoteChunkManager) isChunkManagerExpired(transientChunkManager *TransientFabricRemoteChunkManager, collectionId int64) bool {
	// check if the chunk manager needs to be refreshed
	currentTime := time.Now().UTC()
	expirationTime, err := time.Parse(time.RFC3339, transientChunkManager.expirationTimestamp)
	if err != nil {
		log.Error("Error parsing time string for the current chunk manager", zap.String("expirationTimeStamp", transientChunkManager.expirationTimestamp),
			zap.Int64("collectionId", collectionId))
		return true
	}

	credentialsRefreshThresholdStr := os.Getenv("CREDENTIALS_REFRESH_THRESHOLD_MINUTES")

	credentialsRefreshThreshold, err := strconv.ParseFloat(credentialsRefreshThresholdStr, 64)
	if err != nil {
		log.Error("Error parsing CREDENTIALS_REFRESH_THRESHOLD_MINUTES", zap.Int64("collectionId", collectionId), zap.Error(err))
	}

	remainingValidity := expirationTime.Sub(currentTime)

	log.Debug("ChunkManager expiration details",
		zap.Float64("credentialsRefreshThreshold", credentialsRefreshThreshold),
		zap.Time("currentTime", currentTime),
		zap.Time("expirationTime", expirationTime),
		zap.Float64("remainingValidity", remainingValidity.Minutes()),
		zap.Int64("collectionId", collectionId),
	)

	if remainingValidity.Minutes() < credentialsRefreshThreshold {
		log.Debug("Chunk Manager needs to be refreshed", zap.Int64("collectionId", collectionId))
		return true
	} else {
		log.Debug("Chunk Manager does not need to be refreshed", zap.Int64("collectionId", collectionId))
		return false
	}
}

/*
This method adds a new entry in the chunk manager map if there is none for the given collection id
If the collection id chunk manager exists then it updates the chunk manager with latest credentials from access manager
*/
func (mcm *FabricRemoteChunkManager) upsertChunkManager(ctx context.Context, collID int64) error {

	mcm.chunkManagerMutex.Lock()
	defer mcm.chunkManagerMutex.Unlock()
	log.Debug("Entered lock to initialize chunk manager for collection id : ", zap.Int64("collectionId", collID))
	// double check to ensure chunk manager is not initialized twice
	if !mcm.isValidChunkManagerPresent(collID) {
		log.Info("Initializing chunk manager for collection id : ", zap.Int64("collectionId", collID))
		transientChunkManager, err := mcm.getNewChunkManager(ctx, collID)
		if err != nil {
			return err
		}
		mcm.chunkManagers[collID] = transientChunkManager
	}
	return nil
}

/*
This method creates a new chunk manager for the given collection id
*/
func (mcm *FabricRemoteChunkManager) getNewChunkManager(ctx context.Context, collID int64) (*TransientFabricRemoteChunkManager, error) {

	//TODO add retries
	accessCredentials, err := accessmanager.GetCredentialsForCollection(
		ctx,
		"",
		fmt.Sprintf("%d", collID),
		mcm.config.BucketName,
		false,
	)

	if err != nil {
		return nil, err
	}

	// cloned the config to be used for this new chunk manager object
	newConfig := mcm.config.Clone()

	newConfig.AccessKeyID = accessCredentials.AccessKeyID
	newConfig.SecretAccessKeyID = accessCredentials.SecretAccessKey
	newConfig.SessionToken = accessCredentials.SessionToken
	newConfig.SseKms = accessCredentials.TenantKeyId

	remoteChunkManager, err := storage.NewRemoteChunkManager(ctx, newConfig)

	transientChunkManager := &TransientFabricRemoteChunkManager{
		remoteChunkManager,
		accessCredentials.ExpirationTimestamp,
	}
	return transientChunkManager, err
}
