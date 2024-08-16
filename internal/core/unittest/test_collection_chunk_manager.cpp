#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "storage/CollectionChunkManager.h"
#include "dpccvsaccessmanager/DpcCvsAccessManagerClient.h"
#include "storage/Types.h"
#include <chrono>

using namespace milvus::storage;
using namespace salesforce::cdp::dpccvsaccessmanager::v1;
using ::testing::_;
using ::testing::Return;
using ::testing::NiceMock;

class MockDpcCvsAccessManager : public milvus::dpccvsaccessmanager::DpcCvsAccessManagerClient {
public:
    MOCK_METHOD(GetCredentialsResponse, GetCredentials,
                (const std::string&, const std::string&, const std::string&, bool), (override));
};


class CollectionChunkManagerTest : public ::testing::Test {
protected:
    std::shared_ptr<MockDpcCvsAccessManager> mock_client;

    void SetUp() override {
        StorageConfig config;
        config.bucket_name = "test_bucket";
        config.byok_enabled = true;
        CollectionChunkManager::Init(config);

        mock_client = std::make_shared<NiceMock<MockDpcCvsAccessManager>>();
        CollectionChunkManager::SetClientForTesting(mock_client);
    }

    void TearDown() override {
        CollectionChunkManager::ResetClient();
    }
};

TEST_F(CollectionChunkManagerTest, InitSetsConfigurationCorrectly) {
    ASSERT_EQ(CollectionChunkManager::GetStorageConfig().bucket_name, "test_bucket");
}

TEST_F(CollectionChunkManagerTest, GetDpcCvsAccessManagerClientCreatesClientCorrectly) {
    auto client = CollectionChunkManager::GetDpcCvsAccessManagerClient();
    ASSERT_NE(client, nullptr);

    auto client_again = CollectionChunkManager::GetDpcCvsAccessManagerClient();
    ASSERT_EQ(client, client_again);
}

TEST_F(CollectionChunkManagerTest, GetNewCredentialsFetchesValidCredentials) {
    GetCredentialsResponse mock_response;
    mock_response.set_access_key_id("test_key");
    EXPECT_CALL(*mock_client, GetCredentials(_, _, _, _))
        .WillOnce(Return(mock_response));

    auto credentials = CollectionChunkManager::GetNewCredentials(123, "instance", "bucket", true);
    ASSERT_EQ(credentials->access_key_id(), "test_key");
}

TEST_F(CollectionChunkManagerTest, IsExpiredCorrectlyDeterminesExpiration) {
    auto now = std::chrono::system_clock::now();
    auto past = now - std::chrono::hours(1);
    auto future = now + std::chrono::hours(1);

    ASSERT_TRUE(CollectionChunkManager::IsExpired(past));
    ASSERT_FALSE(CollectionChunkManager::IsExpired(future));
}

TEST_F(CollectionChunkManagerTest, ConvertToChronoTimeHandlesDifferentFormats) {
    std::string time_str = "2023-08-20T15:00:00Z";
    auto time_point = CollectionChunkManager::ConvertToChronoTime(time_str);
    std::time_t t = std::chrono::system_clock::to_time_t(time_point);
    std::tm* ptm = std::gmtime(&t);
    ASSERT_EQ(ptm->tm_year + 1900, 2023);
    ASSERT_EQ(ptm->tm_mon + 1, 8);
    ASSERT_EQ(ptm->tm_mday, 20);
    ASSERT_EQ(ptm->tm_hour, 15);
}

TEST_F(CollectionChunkManagerTest, GetUpdatedStorageConfigUpdatesCorrectly) {
    salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsResponse response;
    response.set_access_key_id("access_key_id");
    response.set_secret_access_key("secret_access_key");
    response.set_session_token("session_token");
    response.set_tenant_key_id("tenant_key_id");

    auto config = CollectionChunkManager::GetUpdatedStorageConfig(response);

    ASSERT_EQ(config.access_key_id, "access_key_id");
    ASSERT_EQ(config.access_key_value, "secret_access_key");
    ASSERT_EQ(config.session_token, "session_token");
    ASSERT_EQ(config.kms_key_id, "tenant_key_id");
}
