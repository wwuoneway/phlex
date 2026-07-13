// Copyright (C) 2025 ...

#ifndef FORM_ROOT_STORAGE_ROOT_TBRANCH_READ_CONTAINER_HPP
#define FORM_ROOT_STORAGE_ROOT_TBRANCH_READ_CONTAINER_HPP

#include "storage/storage_read_container.hpp"

#include <memory>
#include <string>

class TFile;
class TTree;
class TBranch;
class TClass;

namespace form::detail::experimental {

  class ROOT_TBranch_Read_ContainerImp : public Storage_Read_Container {
  public:
    explicit ROOT_TBranch_Read_ContainerImp(std::string const& name);
    ~ROOT_TBranch_Read_ContainerImp() override;

    void setFile(std::shared_ptr<IStorage_File> file) override;
    void prime(std::type_info const& type) override;

    bool read(int id, void const** data, std::type_info const& type) override;
    int entries() override;

  private:
    std::shared_ptr<TFile> m_tfile;
    TTree* m_tree{nullptr};
    TBranch* m_branch{nullptr};

    // Current branch buffer - holds the most recent object
    // Managed by cleanupBranchBuffer() to ensure proper cleanup
    void* m_branch_buffer{nullptr};
    TClass* m_branch_buffer_klass{nullptr}; // TClass for complex types, nullptr for fundamental

    // Helper to properly clean up the current buffer
    void cleanupBranchBuffer();
  };

} // namespace form::detail::experimental

#endif // FORM_ROOT_STORAGE_ROOT_TBRANCH_READ_CONTAINER_HPP
