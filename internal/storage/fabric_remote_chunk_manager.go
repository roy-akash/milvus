package storage

import (
	"context"
	"fmt"
	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/accessmanager"
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
	chunkManager        *RemoteChunkManager
	expirationTimestamp string
}

type FabricRemoteChunkManager struct {
	*RemoteChunkManager
	globalTransientRemoteChunkManager *TransientFabricRemoteChunkManager
	chunkManagers                     map[int64]*TransientFabricRemoteChunkManager
	config                            *config
	chunkManagerMutex                 sync.Mutex
}

var (
	fabricRemoteChunkManager      *FabricRemoteChunkManager
	fabricRemoteChunkManagerMutex sync.Mutex
)

var params *paramtable.ComponentParam = paramtable.Get()

func NewFabricRemoteChunkManager(ctx context.Context, c *config) (*FabricRemoteChunkManager, error) {
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
				config:                            c}
		}
	}
	log.Debug("Returning Fabric remote chunk manager")
	return fabricRemoteChunkManager, nil
}

func (mcm *FabricRemoteChunkManager) MultiWrite(ctx context.Context, kvs map[string][]byte) error {
	log.Debug("MultiWrite called ")
	var el error
	for key, value := range kvs {
		log.Info("Data to be written to ", zap.String("filepath", key))
		collID, err := mcm.retrieveCollectionIDFromFilepath(key)
		trcm, err := mcm.getNewChunkManager(ctx, collID)

		// if chunk manager is available
		if err == nil {
			err = trcm.chunkManager.Write(ctx, key, value)
		} else {
			el = merr.Combine(el, errors.Wrapf(err, "failed to get chunkmanager for collection id %s", collID))
		}

		// if write  operation failed
		if err != nil {
			el = merr.Combine(el, errors.Wrapf(err, "failed to write %s", key))
		}
	}
	return el
}

func (mcm *FabricRemoteChunkManager) Write(ctx context.Context, filePath string, content []byte) error {

	log.Debug("Write called for path ", zap.String("filePath", filePath))

	var el error
	collID, err := mcm.retrieveCollectionIDFromFilepath(filePath)
	trcm, err := mcm.getNewChunkManager(ctx, collID)
	// if chunk manager is available
	if err == nil {
		err = trcm.chunkManager.Write(ctx, filePath, content)
	} else {
		el = merr.Combine(el, errors.Wrapf(err, "failed to get chunkmanager for collection id %s", collID))
	}

	// if write  operation failed
	if err != nil {
		el = merr.Combine(el, errors.Wrapf(err, "failed to write %s", filePath))
	}

	return el
}

func (mcm *FabricRemoteChunkManager) Read(ctx context.Context, filePath string) ([]byte, error) {

	log.Debug("Read called for path ", zap.String("filePath", filePath))

	var el error
	log.Info("Data to be read from ", zap.String("filepath", filePath))
	collID, err := mcm.retrieveCollectionIDFromFilepath(filePath)
	rcm, err := mcm.getChunkManager(ctx, collID)

	var objectValue []byte
	// if chunk manager is available
	if err == nil {
		objectValue, err = rcm.Read(ctx, filePath)
	} else {
		el = merr.Combine(el, errors.Wrapf(err, "failed to get chunkmanager for collection id %s", collID))
	}

	// if read  operation failed
	if err != nil {
		el = merr.Combine(el, errors.Wrapf(err, "failed to read %s", filePath))
	}

	return objectValue, el
}

func (mcm *FabricRemoteChunkManager) MultiRead(ctx context.Context, keys []string) ([][]byte, error) {

	log.Debug("MultiRead called for path ", zap.Strings("keys", keys))

	var el error
	var objectsValues [][]byte
	for _, key := range keys {
		log.Info("Data to be written to ", zap.String("filepath", key))
		collID, err := mcm.retrieveCollectionIDFromFilepath(key)
		rcm, err := mcm.getChunkManager(ctx, collID)

		var objectValue []byte
		// if chunk manager is available
		if err == nil {
			objectValue, err = rcm.Read(ctx, key)
		} else {
			el = merr.Combine(el, errors.Wrapf(err, "failed to get chunkmanager for collection id %s", collID))
		}

		// if read operation failed
		if err != nil {
			el = merr.Combine(el, errors.Wrapf(err, "failed to read %s", key))
		}
		objectsValues = append(objectsValues, objectValue)
	}

	return objectsValues, el
}

func (mcm *FabricRemoteChunkManager) retrieveCollectionIDFromFilepath(key string) (int64, error) {
	collectionIdIndex := strings.Count(params.MinioCfg.RootPath.GetValue(), "/") + 2
	log.Info("collection id index", zap.Int("collectionIdIndex", collectionIdIndex))
	collId, err := strconv.ParseInt(strings.Split(key, "/")[collectionIdIndex], 10, 64)
	if err != nil {
		log.Error("error occurred while trying to derive collection id", zap.String("key", key))
	}
	return collId, err
}

// Remove deletes an object with @key.
func (mcm *FabricRemoteChunkManager) Remove(ctx context.Context, filePath string) error {
	log.Debug("Remove called for path ", zap.String("filePath", filePath))

	var el error
	collID, err := mcm.retrieveCollectionIDFromFilepath(filePath)
	rcm, err := mcm.getChunkManager(ctx, collID)

	// if chunk manager is available
	if err == nil {
		err = rcm.Remove(ctx, filePath)
	} else {
		el = merr.Combine(el, errors.Wrapf(err, "failed to get chunkmanager for collection id %s", collID))
	}

	// if operation failed
	if err != nil {
		el = merr.Combine(el, errors.Wrapf(err, "failed to remove filePath %s", filePath))
	}
	return el
}

// RemoveWithPrefix removes all objects with the same prefix @prefix from minio.
func (mcm *FabricRemoteChunkManager) RemoveWithPrefix(ctx context.Context, prefix string) error {

	log.Debug("RemoveWithPrefix called for path ", zap.String("prefix", prefix))

	var el error
	collID, err := mcm.retrieveCollectionIDFromFilepath(prefix)
	rcm, err := mcm.getChunkManager(ctx, collID)

	// if chunk manager is available
	if err == nil {
		err = rcm.RemoveWithPrefix(ctx, prefix)
	} else {
		el = merr.Combine(el, errors.Wrapf(err, "failed to get chunkmanager for collection id %s to perform RemoveWithPrefix", collID))
	}

	// if operation failed
	if err != nil {
		el = merr.Combine(el, errors.Wrapf(err, "failed to execute RemoveWithPrefix for prefix %s", prefix))
	}
	return el
}

func (mcm *FabricRemoteChunkManager) ListWithPrefix(ctx context.Context, prefix string, recursive bool) ([]string, []time.Time, error) {
	log.Debug("ListWithPrefix called for path ", zap.String("prefix", prefix))
	var el error
	//always use global chunk manager
	gcm, err := mcm.getGlobalChunkManager(ctx)

	var objectList []string
	var times []time.Time
	// if global chunk manager is available
	if err == nil {
		objectList, times, err = gcm.chunkManager.ListWithPrefix(ctx, prefix, recursive)
	} else {
		el = merr.Combine(el, errors.Wrapf(err, "failed to get global chunkmanager "))
	}

	// if listWithPrefix operation failed
	if err != nil {
		el = merr.Combine(el, errors.Wrapf(err, "failed to perform list with prefix %s", prefix))
	}

	return objectList, times, el
}

func upsertGlobalChunkManager(ctx context.Context, c *config) (*TransientFabricRemoteChunkManager, error) {

	log.Debug("Initializing global chunk manager")

	//TODO add retries
	accessCredentials, err := accessmanager.GetGlobalCredentials(
		ctx,
		c.bucketName,
	)

	if err != nil {
		return nil, err
	}

	// cloned the config to be used for this new chunk manager object
	newConfig := c.Clone()

	newConfig.accessKeyID = accessCredentials.AccessKeyID
	newConfig.secretAccessKeyID = accessCredentials.SecretAccessKey
	newConfig.sessionToken = accessCredentials.SessionToken

	remoteChunkManager, _ := NewRemoteChunkManager(ctx, newConfig)

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

func (mcm *FabricRemoteChunkManager) getChunkManager(ctx context.Context, collID int64) (*RemoteChunkManager, error) {
	log.Info("getting chunk manager for collection id", zap.Int64("collID", collID))
	var err error
	if !mcm.isValidChunkManagerPresent(collID) {
		err = mcm.upsertChunkManager(ctx, collID)
	}
	if err != nil {
		log.Error("Error while loading chunk manager for collection id", zap.Int64("collectionId", collID))
		// TODO decide to return chunk manager with stale credentials if present
		return nil, err
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

	log.Debug("chunkManager expiration details",
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

	log.Debug("Initializing new chunk manager for collection id : ", zap.Int64("collectionId", collID))

	accessCredentials, err := accessmanager.GetCredentialsForCollection(
		ctx,
		fmt.Sprintf("%d", collID),
		mcm.config.bucketName,
	)

	if err != nil {
		return nil, err
	}

	// cloned the config to be used for this new chunk manager object
	newConfig := mcm.config.Clone()

	newConfig.accessKeyID = accessCredentials.AccessKeyID
	newConfig.secretAccessKeyID = accessCredentials.SecretAccessKey
	newConfig.sessionToken = accessCredentials.SessionToken
	newConfig.sseKms = accessCredentials.TenantKeyId

	remoteChunkManager, err := NewRemoteChunkManager(ctx, newConfig)

	transientChunkManager := &TransientFabricRemoteChunkManager{
		remoteChunkManager,
		accessCredentials.ExpirationTimestamp,
	}
	return transientChunkManager, err
}
