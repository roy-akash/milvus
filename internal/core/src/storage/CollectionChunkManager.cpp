#include "CollectionChunkManager.h"
#include "log/Log.h"
#include "storage/Util.h"
#include <sstream>
#include <iomanip>
#include <ctime>
#include "storage/RemoteChunkManagerSingleton.h"
#include <grpcpp/grpcpp.h>
#include <grpc/grpc.h>
#include <typeinfo>


namespace milvus::storage {

std::shared_ptr<CollectionChunkManager> CollectionChunkManager::instance = nullptr;
std::mutex CollectionChunkManager::mutex_;
std::mutex CollectionChunkManager::client_mutex_;
std::shared_ptr<milvus::dpccvsaccessmanager::DpcCvsAccessManagerClient> CollectionChunkManager::dpcCvsAccessManagerClient_ = nullptr;
StorageConfig CollectionChunkManager::storageConfigTemplate;
std::unordered_map<int64_t, std::tuple<std::shared_ptr<ChunkManager>, std::chrono::system_clock::time_point>> CollectionChunkManager::chunkManagerMemoryCache;


CollectionChunkManager::CollectionChunkManager(const StorageConfig& config) {
    LOG_SEGCORE_INFO_ << "Initializing CollectionChunkManager with config: " << config.ToString();
    storageConfigTemplate = config;
    default_bucket_name_ = config.bucket_name;
    remote_root_path_ = config.root_path;
    use_collectionId_based_index_path_ = config.useCollectionIdIndexPath;
}

std::shared_ptr<CollectionChunkManager> CollectionChunkManager::GetInstance(const StorageConfig& config) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!instance) {
        auto new_instance = new CollectionChunkManager(config);
        instance = std::shared_ptr<CollectionChunkManager>(new_instance);
    }
    return instance;
}

bool CollectionChunkManager::IsExpired(const std::chrono::system_clock::time_point& expiration) {
    bool expired = std::chrono::system_clock::now() > expiration;
    LOG_SEGCORE_INFO_ << "Checking if expiration time is expired: " << expired;
    return expired;
}


std::shared_ptr<milvus::dpccvsaccessmanager::DpcCvsAccessManagerClient> CollectionChunkManager::GetDpcCvsAccessManagerClient() {
    //std::lock_guard<std::mutex> lock(client_mutex_);

    if (!dpcCvsAccessManagerClient_) {
        try {
            dpcCvsAccessManagerClient_ = std::make_shared<milvus::dpccvsaccessmanager::DpcCvsAccessManagerClient>();
            if (!dpcCvsAccessManagerClient_) {
                LOG_SEGCORE_ERROR_ << "Failed to create DpcCvsAccessManagerClient.";
                return nullptr;
            }

            LOG_SEGCORE_INFO_ << "Created new DpcCvsAccessManagerClient.";
        } catch (const std::exception& e) {
            LOG_SEGCORE_ERROR_ << "Exception creating channel: " << e.what();
        } catch (...) {
            LOG_SEGCORE_ERROR_ << "Unknown error creating channel.";
        }
    } else {
        LOG_SEGCORE_INFO_ << "dpcCvsAccessManagerClient_ is already initialized.";
    }

    return dpcCvsAccessManagerClient_;
}

std::shared_ptr<salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsResponse> CollectionChunkManager::GetNewCredentials(
    const int64_t collection_id,
    const std::string& instance_name,
    const std::string& bucket_name,
    bool write_access) {

    auto client = GetDpcCvsAccessManagerClient();
	salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsResponse response;

    try {
		response = client->GetCredentials(std::to_string(collection_id), instance_name, bucket_name, write_access);
        LOG_SEGCORE_INFO_ << "Successfully obtained new credentials for collection ID: " << collection_id;
        return std::make_shared<salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsResponse>(response);
    } catch (const std::exception& e) {
        LOG_SEGCORE_ERROR_ << "Error getting new credentials: " << e.what();
    }
    return nullptr;
}

StorageConfig CollectionChunkManager::GetUpdatedStorageConfig(const salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsResponse& response) {
    StorageConfig updated_config = storageConfigTemplate;

    updated_config.access_key_id = response.access_key_id();
    updated_config.access_key_value = response.secret_access_key();
    updated_config.session_token = response.session_token();
    updated_config.kms_key_id = response.tenant_key_id();
    updated_config.byok_enabled = false;
    LOG_SEGCORE_INFO_ << "Updated storage config with new credentials.";
    return updated_config;
}

std::chrono::system_clock::time_point CollectionChunkManager::ConvertToChronoTime(const std::string& time_str) {
    std::tm tm = {};
    std::istringstream ss(time_str);
    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
    return std::chrono::system_clock::from_time_t(std::mktime(&tm));
}


std::string_view
CollectionChunkManager::GetPartByIndex(const std::string_view str, char delimiter, int index) {
    size_t start = 0;
    size_t end = str.find(delimiter);

    for (int i = 0; i <= index; ++i) {
        if (end == std::string_view::npos) {
            if (i == index) {
                return str.substr(start);
            } else {
                throw std::out_of_range("Index out of range");
            }
        } else {
            if (i == index) {
                return str.substr(start, end - start);
            } else {
                start = end + 1;
                end = str.find(delimiter, start);
            }
        }
    }

    throw std::out_of_range("Index out of range");
}

std::shared_ptr<ChunkManager> CollectionChunkManager::GetChunkManager(
    const int64_t collection_id,
    const std::string& instance_name,
    bool write_access) {
    if (!storageConfigTemplate.byok_enabled) {
        LOG_SEGCORE_INFO_ << "BYOK not enabled, using RemoteChunkManagerSingleton.";
        return milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                .GetRemoteChunkManager();
    }

    const std::string& bucket_name = storageConfigTemplate.bucket_name;
    LOG_SEGCORE_INFO_ << "Getting ChunkManager for collection ID: " << collection_id;

    auto cacheObject = chunkManagerMemoryCache.find(collection_id);
    if (cacheObject != chunkManagerMemoryCache.end()) {
        //std::lock_guard<std::mutex> lock(client_mutex_);
        auto [chunk_manager, expiration] = cacheObject->second;
        if (!IsExpired(expiration)) {
            LOG_SEGCORE_INFO_ << "Found valid ChunkManager in cache for collection ID: " << collection_id;
            return chunk_manager;
        } else {
            LOG_SEGCORE_INFO_ << "Cached ChunkManager expired for collection ID: " << collection_id;
        }
    }

    LOG_SEGCORE_INFO_ << "Getting GetNewCredentials for collection ID: " << collection_id;
    auto credentials = GetNewCredentials(collection_id, instance_name, bucket_name, write_access);
    if (credentials == nullptr) {
        LOG_SEGCORE_ERROR_ << "Failed to get new credentials for collection ID: " << collection_id;
        return nullptr;
    }
    LOG_SEGCORE_INFO_ << "Got NewCredentials for collection ID: " << collection_id;
    auto updated_config = GetUpdatedStorageConfig(*credentials);
    LOG_SEGCORE_INFO_ << "Created updated storage config for collection ID: " << collection_id;
    auto chunk_manager = milvus::storage::CreateChunkManager(updated_config);
    std::chrono::system_clock::time_point expiration = ConvertToChronoTime(credentials->expiration_timestamp());
    chunkManagerMemoryCache[collection_id] = std::make_tuple(chunk_manager, expiration);
    LOG_SEGCORE_INFO_ << "Cached new ChunkManager for collection ID: " << collection_id;
    return chunk_manager;
}

uint64_t CollectionChunkManager::Size(const std::string& filepath) {
    return ApplyToChunkManager(filepath, &ChunkManager::Size, filepath);
}

bool CollectionChunkManager::Exist(const std::string& filepath) {
    return ApplyToChunkManager(filepath, &ChunkManager::Exist, filepath);
}

void CollectionChunkManager::Remove(const std::string& filepath) {
    ApplyToChunkManager(filepath, &ChunkManager::Remove, filepath);
}

std::vector<std::string> CollectionChunkManager::ListWithPrefix(const std::string& filepath) {
    return ApplyToChunkManager(filepath, &ChunkManager::ListWithPrefix, filepath);
}

uint64_t CollectionChunkManager::Read(const std::string& filepath, void* buf, uint64_t size) {
    using ReadFuncType = uint64_t (ChunkManager::*)(const std::string&, void*, uint64_t);
    return ApplyToChunkManager(filepath, static_cast<ReadFuncType>(&ChunkManager::Read), filepath, buf, size);
}

void CollectionChunkManager::Write(const std::string& filepath, void* buf, uint64_t size) {
    using WriteFuncType = void (ChunkManager::*)(const std::string&, void*, uint64_t);
    ApplyToChunkManager(filepath, static_cast<WriteFuncType>(&ChunkManager::Write), filepath, buf, size);
}

} // namespace milvus::storage