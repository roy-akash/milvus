#pragma once

#include <memory>
#include <unordered_map>
#include <tuple>
#include <string>
#include <chrono>
#include "ChunkManager.h"
#include "storage/Types.h"
#include <grpcpp/grpcpp.h>
#include "pb/dpc_cvs_access_manager.pb.h"
#include "pb/dpc_cvs_access_manager.grpc.pb.h"
#include "dpccvsaccessmanager/DpcCvsAccessManagerClient.h"

namespace milvus::storage {

class CollectionChunkManager : public ChunkManager {
public:
    CollectionChunkManager(const StorageConfig& config);
    std::string default_bucket_name_;
    std::string remote_root_path_;
    bool use_collectionId_based_index_path_;
    static void Init(const StorageConfig& config);
    static std::shared_ptr<ChunkManager> GetChunkManager(
        const int64_t collection_id,
        const std::string& instance_name,
        bool write_access);
    virtual bool
    Exist(const std::string& filepath);

    virtual uint64_t
    Size(const std::string& filepath);

    virtual uint64_t
    Read(const std::string& filepath,
         uint64_t offset,
         void* buf,
         uint64_t len) {
        throw SegcoreError(NotImplemented,
                           GetName() + "Read with offset not implement");
    }

    virtual void
    Write(const std::string& filepath,
          uint64_t offset,
          void* buf,
          uint64_t len) {
        throw SegcoreError(NotImplemented,
                           GetName() + "Write with offset not implement");
    }

    virtual uint64_t
    Read(const std::string& filepath, void* buf, uint64_t len);

    virtual void
    Write(const std::string& filepath, void* buf, uint64_t len);

    virtual std::vector<std::string>
    ListWithPrefix(const std::string& filepath = "");

    virtual void
    Remove(const std::string& filepath);

    virtual std::string
    GetName() const {
        return "CollectionChunkManager";
    }

    virtual std::string
    GetRootPath() const {
        return remote_root_path_;
    }

    inline std::string
    GetBucketName() {
        return default_bucket_name_;
    }

    inline void
    SetBucketName(const std::string& bucket_name) {
        default_bucket_name_ = bucket_name;
    }

    bool
    BucketExists(const std::string& bucket_name);

    bool
    CreateBucket(const std::string& bucket_name);

    bool
    DeleteBucket(const std::string& bucket_name);

    std::vector<std::string>
    ListBuckets();

    bool
    UseCollectionIdBasedIndexPath() const { return use_collectionId_based_index_path_; }

private:
    static StorageConfig storageConfigTemplate;
    static std::shared_ptr<milvus::dpccvsaccessmanager::DpcCvsAccessManagerClient> GetDpcCvsAccessManagerClient();
    static std::shared_ptr<milvus::dpccvsaccessmanager::DpcCvsAccessManagerClient> dpcCvsAccessManagerClient_;
    static std::unordered_map<int64_t, std::tuple<std::shared_ptr<ChunkManager>, std::chrono::system_clock::time_point>> chunkManagerMemoryCache;
    static bool IsExpired(const std::chrono::system_clock::time_point& expiration);
    static StorageConfig GetUpdatedStorageConfig(const salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsResponse& response);
    static std::shared_ptr<salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsResponse> GetNewCredentials(
        const int64_t collection_id,
        const std::string& instance_name,
        const std::string& bucket_name,
        bool write_access);

    static std::chrono::system_clock::time_point ConvertToChronoTime(const std::string& time_str);
    static std::mutex client_mutex_;
};

} // namespace milvus::storage
