#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "dpccvsaccessmanager/DpcCvsAccessManagerClient.h"

using namespace milvus::dpccvsaccessmanager;
using ::testing::_;
using ::testing::Return;
using ::testing::DoAll;
using ::testing::SetArgPointee;

class MockDpcCvsAccessManager : public salesforce::cdp::dpccvsaccessmanager::v1::DpcCvsAccessManager::StubInterface {
public:
    MOCK_METHOD(grpc::Status, GetCredentials, (grpc::ClientContext*, const salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsRequest&, salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsResponse*), (override));
    MOCK_METHOD(::grpc::ClientAsyncResponseReaderInterface< ::salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsResponse>*, AsyncGetCredentialsRaw, (::grpc::ClientContext* context, const ::salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsRequest& request, ::grpc::CompletionQueue* cq), (override));
    MOCK_METHOD(::grpc::ClientAsyncResponseReaderInterface< ::salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsResponse>*, PrepareAsyncGetCredentialsRaw, (::grpc::ClientContext* context, const ::salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsRequest& request, ::grpc::CompletionQueue* cq), (override));
};

class DpcCvsAccessManagerClientTest : public ::testing::Test {
protected:
    std::unique_ptr<MockDpcCvsAccessManager> mock_stub;
    std::unique_ptr<DpcCvsAccessManagerClient> client;

    void SetUp() override {

        mock_stub = std::make_unique<MockDpcCvsAccessManager>();
        client = std::make_unique<DpcCvsAccessManagerClient>();
        client->SetStub(mock_stub.get());
    }
};

TEST_F(DpcCvsAccessManagerClientTest, GetCredentialsSuccess) {
    salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsResponse response;
    salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsRequest request;
    request.set_collection_id("example_collection");
    request.set_instance_name("instance1");
    request.set_bucket_name("bucket1");
    request.set_write_access(true);

    response.set_access_key_id("test_key");
    response.set_secret_access_key("test_secret");
    response.set_session_token("test_token");
    response.set_expiration_timestamp("test_expiration");

    EXPECT_CALL(*mock_stub, GetCredentials(_, _, _))
        .WillOnce(DoAll(SetArgPointee<2>(response), Return(grpc::Status::OK)));

    auto actual_response = client->GetCredentials(request.collection_id(), request.instance_name(), request.bucket_name(), request.write_access());

    ASSERT_EQ(actual_response.access_key_id(), "test_key");
    ASSERT_EQ(actual_response.secret_access_key(), "test_secret");
    ASSERT_EQ(actual_response.session_token(), "test_token");
    ASSERT_EQ(actual_response.expiration_timestamp(), "test_expiration");
}

TEST_F(DpcCvsAccessManagerClientTest, GetCredentialsFailure) {
    salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsRequest request;
    request.set_collection_id("example_collection");
    request.set_instance_name("instance1");
    request.set_bucket_name("bucket1");
    request.set_write_access(true);

    EXPECT_CALL(*mock_stub, GetCredentials(_, _, _))
        .WillOnce(Return(grpc::Status(grpc::StatusCode::PERMISSION_DENIED, "Access Denied")));

    ASSERT_THROW({
        client->GetCredentials(request.collection_id(), request.instance_name(), request.bucket_name(), request.write_access());
    }, std::runtime_error);
}
