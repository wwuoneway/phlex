// Copyright (C) 2025 ...

#ifndef FORM_STORAGE_STORAGE_READER_HPP
#define FORM_STORAGE_STORAGE_READER_HPP

#include "istorage.hpp"
#include "storage_utils.hpp"

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility> // for std::pair
#include <vector>

namespace form::detail::experimental {

  struct ProductRegistryEntry {
    std::string productName;
    std::string processName;
    std::string producer;
    std::string productID;
  };

  struct FileCatalogMetadata {
    std::string fileUUID;
    int fileFormatVersion = 0;
  };

  struct FileMetadata {
    FileCatalogMetadata fileCatalog;
    std::vector<ProductRegistryEntry> productRegistry;
    bool hasFileCatalog = false;
    bool hasProductRegistry = false;
  };

  class StorageReader : public IStorageReader {
  public:
    StorageReader() = default;
    ~StorageReader() override = default;

    using table_t = form::experimental::config::tech_setting_config::table_t;

    int getIndex(Token const& token,
                 std::string const& id,
                 form::experimental::config::tech_setting_config const& settings) override;
    void readContainer(Token const& token,
                       void const** data,
                       std::type_info const& type,
                       form::experimental::config::tech_setting_config const& settings) override;

    bool hasFileCatalog(std::string const& fileName) const;
    bool hasProductRegistry(std::string const& fileName) const;
    FileMetadata const* getFileMetadata(std::string const& fileName) const;

  private:
    void ensureFileMetadata(std::string const& fileName, int technology);

    std::map<std::string, std::shared_ptr<IStorage_File>> m_files;
    std::unordered_map<std::pair<std::string, std::string>,
                       std::shared_ptr<IStorage_Read_Container>,
                       pair_hash>
      m_read_containers;
    std::map<std::string, std::map<std::string, int>> m_indexMaps;
    std::map<std::string, FileMetadata> m_fileMetadata;
  };

} // namespace form::detail::experimental

#endif // FORM_STORAGE_STORAGE_READER_HPP
