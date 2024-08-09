#include "DpcCvsAccessManagerClient.h"
#include "log/Log.h"
#include <grpcpp/grpcpp.h>
#include <grpc/grpc.h>
#include <thread>
#include <typeinfo>

namespace milvus::dpccvsaccessmanager {

std::mutex DpcCvsAccessManagerClient::stub_mutex_;

DpcCvsAccessManagerClient::DpcCvsAccessManagerClient() {
    auto channel_ = grpc::CreateChannel(std::getenv("ACCESS_MANAGER_SERVICE_URL"), grpc::InsecureChannelCredentials());
    stub_ = salesforce::cdp::dpccvsaccessmanager::v1::DpcCvsAccessManager::NewStub(channel_);
    if (!stub_) {
        LOG_SEGCORE_ERROR_ << "Failed to create stub.";
        throw std::runtime_error("Failed to create gRPC stub.");
    }
}

salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsResponse DpcCvsAccessManagerClient::GetCredentials(
    const std::string& collection_id,
    const std::string& instance_name,
    const std::string& bucket_name,
    bool write_access) {

    LOG_SEGCORE_INFO_ << "Inside DpcCvsAccessManagerClient::GetCredentials";

    salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsRequest request;
    request.set_application_type(salesforce::cdp::dpccvsaccessmanager::v1::ApplicationType::MILVUS);
    request.set_collection_id(collection_id);
    request.set_instance_name(instance_name);
    request.set_bucket_name(bucket_name);
    request.set_write_access(write_access);

    LOG_SEGCORE_INFO_ << "Request prepared - Application Type: " << request.application_type()
                      << ", Collection ID: " << request.collection_id()
                      << ", Instance Name: " << request.instance_name()
                      << ", Bucket Name: " << request.bucket_name()
                      << ", Write Access: " << (request.write_access() ? "true" : "false");

    salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsResponse response;
    grpc::ClientContext context;

    if (!stub_) {
        LOG_SEGCORE_ERROR_ << "Stub is not initialized.";
        throw std::runtime_error("Stub is not initialized");
    }

    LOG_SEGCORE_INFO_ << "Sending gRPC request to GetCredentials." << std::flush;

    try {
        // Perform the gRPC call
        auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(10);
        context.set_deadline(deadline);
        grpc::Status status = stub_->GetCredentials(&context, request, &response);

        if (status.ok()) {
            LOG_SEGCORE_INFO_ << "Received response from GetCredentials.";
            LOG_SEGCORE_INFO_ << "Response - Access Key ID: " << response.access_key_id()
                              << ", Secret Access Key: [REDACTED]"
                              << ", Session Token: [REDACTED]"
                              << ", Expiration: " << response.expiration_timestamp();
            return response;
        } else {
            LOG_SEGCORE_ERROR_ << "gRPC call failed with error: " << status.error_message()
                               << ", error code: " << status.error_code();
            throw std::runtime_error("gRPC call failed: " + status.error_message());
        }
    } catch (const std::exception& e) {
        LOG_SEGCORE_ERROR_ << "Exception during gRPC call: " << e.what();
        throw;
    } catch (...) {
        LOG_SEGCORE_ERROR_ << "Unknown exception during gRPC call.";
        throw;
    }
}

} // namespace milvus::dpccvsaccessmanager
