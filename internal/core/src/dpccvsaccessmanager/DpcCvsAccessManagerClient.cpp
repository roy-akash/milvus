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
        LOG_ERROR("Failed to create stub.");
        throw std::runtime_error("Failed to create gRPC stub.");
    }
}

void DpcCvsAccessManagerClient::SetStub(salesforce::cdp::dpccvsaccessmanager::v1::DpcCvsAccessManager::StubInterface* stub) {
    stub_.reset(stub);
}

salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsResponse DpcCvsAccessManagerClient::GetCredentials(
    const std::string& collection_id,
    const std::string& instance_name,
    const std::string& bucket_name,
    bool write_access) {

    LOG_INFO("Inside DpcCvsAccessManagerClient::GetCredentials");

    salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsRequest request;
    request.set_application_type(salesforce::cdp::dpccvsaccessmanager::v1::ApplicationType::MILVUS);
    if (collection_id != "-1") {
        request.set_collection_id(collection_id);
    }
    request.set_instance_name(instance_name);
    request.set_bucket_name(bucket_name);
    request.set_write_access(write_access);

    LOG_INFO("Request prepared - Application Type: {}, Collection ID: {}, Instance Name: {}, Bucket Name: {}, Write Access: {}",
             request.application_type(),
             request.collection_id(),
             request.instance_name(),
             request.bucket_name(),
             request.write_access() ? "true" : "false");


    salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsResponse response;

    if (!stub_) {
        LOG_ERROR("Stub is not initialized.");
        throw std::runtime_error("Stub is not initialized");
    }

    LOG_INFO("Sending gRPC request to GetCredentials.");

    const int max_retries = 3;
    int attempt = 0;
    grpc::Status status;

    while (attempt < max_retries) {
        grpc::ClientContext context;

        try {
            status = stub_->GetCredentials(&context, request, &response);

            if (status.ok()) {
                LOG_INFO("Received response from GetCredentials.");
                LOG_INFO("Response - Access Key ID: {}, Secret Access Key: [REDACTED], Session Token: [REDACTED], Expiration: {}",
                         response.access_key_id(),
                         response.expiration_timestamp());
                return response;
            } else {
                LOG_ERROR("gRPC call failed with error: {}, error code: {}",
                          status.error_message(),
                          status.error_code());
            }
        } catch (const std::exception& e) {
            LOG_ERROR("Exception during gRPC call: {}", e.what());
        } catch (...) {
            LOG_ERROR("Unknown exception during gRPC call.");
        }

        ++attempt;
        if (attempt < max_retries) {
            auto delay = std::chrono::seconds(1 << attempt);
            LOG_INFO("Retrying GetCredentials ({}/{}) after a delay of {} seconds.",
                     attempt,
                     max_retries,
                     delay.count());
            std::this_thread::sleep_for(delay);
        }
    }

    throw std::runtime_error("gRPC call failed after " + std::to_string(max_retries) + " attempts: " + status.error_message());
}

} // namespace milvus::dpccvsaccessmanager
