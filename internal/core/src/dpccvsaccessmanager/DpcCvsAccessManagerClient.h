#ifndef DPC_CVS_ACCESS_MANAGER_CLIENT_H
#define DPC_CVS_ACCESS_MANAGER_CLIENT_H

#pragma once

#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>
#include "pb/dpc_cvs_access_manager.pb.h"
#include "pb/dpc_cvs_access_manager.grpc.pb.h"

namespace milvus::dpccvsaccessmanager {

class DpcCvsAccessManagerClient {
public:
	DpcCvsAccessManagerClient();
    salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsResponse GetCredentials(
        const std::string& collection_id,
        const std::string& instance_name,
        const std::string& bucket_name,
        bool write_access);
    void SetStub(salesforce::cdp::dpccvsaccessmanager::v1::DpcCvsAccessManager::StubInterface* stub);
private:
    std::unique_ptr<salesforce::cdp::dpccvsaccessmanager::v1::DpcCvsAccessManager::StubInterface> stub_;
	static std::mutex stub_mutex_;
    std::shared_ptr<grpc::Channel> channel_;
};

}
#endif