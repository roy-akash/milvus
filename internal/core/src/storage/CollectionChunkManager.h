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
    CollectionChunkManager(const CollectionChunkManager&) = delete;
    CollectionChunkManager& operator=(const CollectionChunkManager&) = delete;
    static std::shared_ptr<CollectionChunkManager> GetInstance(const StorageConfig& config);
    std::string default_bucket_name_;
    std::string remote_root_path_;
    bool use_collectionId_based_index_path_;
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
    UseCollectionIdBasedIndexPath() const { return use_collectionId_based_index_path_; }

    static std::string_view GetPartByIndex(const std::string_view str, char delimiter, int index);
    template <typename ChunkAction, typename... Args>
    static auto ApplyToChunkManager(const std::string& filepath, ChunkAction action, Args&&... args) {
        int64_t collection_id;
        if (std::is_same_v<ChunkAction, decltype(&ChunkManager::ListWithPrefix)>) {
            // Setting collection_id as -1 for ListWithPrefix.
            // The gRPC client won't set this collection ID while requesting for credentials.
            // That would fetch global credentials for ListWithPrefix.
            collection_id = -1;
        } else {
            int numberOfSlashes = std::count(filepath.begin(), filepath.end(), '/');
            int index = numberOfSlashes + 2;
            std::string_view collection_id_str = GetPartByIndex(filepath, '/', index);
            collection_id = std::stoll(std::string(collection_id_str));
        }
        auto chunk_manager = GetChunkManager(collection_id, std::getenv("INSTANCE_NAME"), true);
        return ((*chunk_manager).*action)(std::forward<Args>(args)...);
    }

private:
    CollectionChunkManager(const StorageConfig& config);
    static std::shared_ptr<CollectionChunkManager> instance;
    static std::mutex mutex_;
    static StorageConfig storageConfigTemplate;
    static std::shared_ptr<milvus::dpccvsaccessmanager::DpcCvsAccessManagerClient> dpcCvsAccessManagerClient_;
    static std::unordered_map<int64_t, std::tuple<std::shared_ptr<ChunkManager>, std::chrono::system_clock::time_point>> chunkManagerMemoryCache;
    static std::mutex client_mutex_;

#ifdef UNIT_TESTING
public:
#else
private:
#endif
    static std::shared_ptr<milvus::dpccvsaccessmanager::DpcCvsAccessManagerClient> GetDpcCvsAccessManagerClient();
    static StorageConfig GetUpdatedStorageConfig(const salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsResponse& response);
    static std::shared_ptr<salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsResponse> GetNewCredentials(
        const int64_t collection_id,
        const std::string& instance_name,
        const std::string& bucket_name,
        bool write_access);
    static std::chrono::system_clock::time_point ConvertToChronoTime(const std::string& time_str);
    static bool IsExpired(const std::chrono::system_clock::time_point& expiration);

#ifdef UNIT_TESTING
public:
    static void SetClientForTesting(const std::shared_ptr<milvus::dpccvsaccessmanager::DpcCvsAccessManagerClient>& client) {
        dpcCvsAccessManagerClient_ = client;
    }
    static void ResetClient() {
        dpcCvsAccessManagerClient_.reset();
        chunkManagerMemoryCache.clear();
    }
    static const StorageConfig& GetStorageConfig() {
        return storageConfigTemplate;
    }
#endif
};

} // namespace milvus::storage
