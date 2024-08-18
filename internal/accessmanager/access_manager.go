package accessmanager

import (
	"context"
	grpcaccessmanagerclient "github.com/milvus-io/milvus/internal/distributed/accessmanager/client"
	dpccvdpb "github.com/milvus-io/milvus/internal/proto/dpccvspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"
	"os"
	"sync"
	"time"
)

var (
	accessManagerClient types.DpcCvsAccessManagerClient
	once                sync.Once
)

type AccessCredentials struct {
	AccessKeyID         string
	SecretAccessKey     string
	SessionToken        string
	ExpirationTimestamp string
	TenantKeyId         string
}

func GetAccessManagerClient(ctx context.Context) types.DpcCvsAccessManagerClient {
	once.Do(func() {
		log.Debug("Access Manager Client initialized")
		accessManagerClient = grpcaccessmanagerclient.NewClient()
	})
	return accessManagerClient
}

func GetCredentialsForCollection(ctx context.Context, collectionId string, bucketName string) (*AccessCredentials, error) {
	instanceName := os.Getenv("MILVUS_INSTANCE_NAME")

	log.Debug("Milvus Instance Name from env", zap.String("instanceName", instanceName))

	credentialRequest := &dpccvdpb.GetCredentialsRequest{
		ApplicationType: dpccvdpb.ApplicationType_MILVUS,
		CollectionId:    collectionId,
		InstanceName:    instanceName,
		BucketName:      bucketName,
		WriteAccess:     true,
	}

	log.Debug("Credential Request to get Credentials : ", zap.Any("credentialRequest", credentialRequest))

	accessManagerClient := GetAccessManagerClient(ctx)

	// Call the GetCredentials method
	requestStartTime := time.Now().UTC()
	credentialResponse, err := accessManagerClient.GetCredentials(ctx, credentialRequest)
	if err != nil {
		log.Error("Failed to get credentials: %v", zap.Any("error", err))
		return nil, err
	}
	timeTakenInMilliSeconds := time.Now().UTC().Sub(requestStartTime).Milliseconds()
	log.Info("Time taken to fetch credentials", zap.Int64("timeTakenInMilliSeconds", timeTakenInMilliSeconds))

	return &AccessCredentials{
		credentialResponse.GetAccessKeyId(),
		credentialResponse.GetSecretAccessKey(),
		credentialResponse.GetSessionToken(),
		credentialResponse.GetExpirationTimestamp(),
		credentialResponse.GetTenantKeyId(),
	}, nil
}

func GetGlobalCredentials(ctx context.Context, bucketName string) (*AccessCredentials, error) {

	credentialRequest := &dpccvdpb.GetCredentialsRequest{
		ApplicationType: dpccvdpb.ApplicationType_MILVUS,
		BucketName:      bucketName,
	}

	log.Debug("Global Credential Request to get Credentials : ", zap.Any("credentialRequest", credentialRequest))

	accessManagerClient := GetAccessManagerClient(ctx)

	// Call the GetCredentials method
	requestStartTime := time.Now().UTC()
	credentialResponse, err := accessManagerClient.GetCredentials(ctx, credentialRequest)
	if err != nil {
		log.Error("Failed to get global credentials: %v", zap.Any("error", err))
		return nil, err
	}
	timeTakenInMilliSeconds := time.Now().UTC().Sub(requestStartTime).Milliseconds()
	log.Info("Time taken to fetch global credentials", zap.Int64("timeTakenInMilliSeconds", timeTakenInMilliSeconds))

	return &AccessCredentials{
		AccessKeyID:         credentialResponse.GetAccessKeyId(),
		SecretAccessKey:     credentialResponse.GetSecretAccessKey(),
		SessionToken:        credentialResponse.GetSessionToken(),
		ExpirationTimestamp: credentialResponse.GetExpirationTimestamp(),
	}, nil
}
