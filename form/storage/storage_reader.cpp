// Copyright (C) 2025 ...

#include "storage_reader.hpp"
#include "storage_file.hpp"
#include "storage_read_container.hpp"

#include "util/factories.hpp"

#ifdef USE_ROOT_STORAGE
#include "TBranch.h"
#include "TFile.h"
#include "TList.h"
#include "TObjString.h"
#include "TTree.h"
#include "root_storage/root_tfile.hpp"
#endif

#include <cstdint>

using namespace form::detail::experimental;

namespace {
#ifdef USE_ROOT_STORAGE
  std::vector<std::string> parseSchemaHeader(std::string const& schema_header)
  {
    std::vector<std::string> schema;
    if (schema_header.size() < 2 || schema_header.front() != '[' || schema_header.back() != ']') {
      return schema;
    }

    std::string token;
    bool in_quotes = false;
    for (std::size_t i = 1; i + 1 < schema_header.size(); ++i) {
      char const ch = schema_header[i];
      if (ch == '"') {
        if (in_quotes) {
          if (!token.empty()) {
            schema.push_back(token);
            token.clear();
          }
          in_quotes = false;
        } else {
          in_quotes = true;
        }
      } else if (in_quotes) {
        token.push_back(ch);
      }
    }

    return schema;
  }

  bool readFileCatalogMetadata(TFile* tfile, FileMetadata& metadata)
  {
    auto* tree = tfile->Get<TTree>("FileCatalog");
    if (tree == nullptr || tree->GetEntries() == 0) {
      return false;
    }

    std::string* fileUUID = nullptr;
    int fileFormatVersion = 0;
    TBranch* uuidBranch = tree->GetBranch("FileUUID");
    TBranch* formatBranch = tree->GetBranch("FileFormatVersion");
    if (uuidBranch == nullptr || formatBranch == nullptr) {
      return false;
    }

    tree->SetBranchAddress("FileUUID", &fileUUID);
    tree->SetBranchAddress("FileFormatVersion", &fileFormatVersion);
    tree->GetEntry(0);

    metadata.fileCatalog.fileUUID = fileUUID ? *fileUUID : std::string();
    metadata.fileCatalog.fileFormatVersion = fileFormatVersion;
    metadata.hasFileCatalog = true;
    return true;
  }

  bool readProductRegistryMetadata(TFile* tfile, FileMetadata& metadata)
  {
    auto* tree = tfile->Get<TTree>("ProductRegistry");
    if (tree == nullptr || tree->GetEntries() == 0) {
      return false;
    }

    std::string* productName = nullptr;
    std::string* processName = nullptr;
    std::string* producer = nullptr;
    std::string* productID = nullptr;
    std::string* productType = nullptr;

    TBranch* productBranch = tree->GetBranch("ProductName");
    TBranch* processBranch = tree->GetBranch("ProcessName");
    TBranch* producerBranch = tree->GetBranch("Producer");
    TBranch* productIDBranch = tree->GetBranch("ProductID");
    if (productBranch == nullptr || processBranch == nullptr || producerBranch == nullptr ||
        productIDBranch == nullptr) {
      return false;
    }

    tree->SetBranchAddress("ProductName", &productName);
    tree->SetBranchAddress("ProcessName", &processName);
    tree->SetBranchAddress("Producer", &producer);
    tree->SetBranchAddress("ProductID", &productID);

    // ProductType is optional: old files written before this feature was added
    // will not have the branch; leave productType as "" for those entries.
    TBranch* productTypeBranch = tree->GetBranch("ProductType");
    if (productTypeBranch != nullptr) {
      tree->SetBranchAddress("ProductType", &productType);
    }

    metadata.productRegistry.clear();
    metadata.productRegistry.reserve(tree->GetEntries());

    for (Long64_t entry = 0; entry < tree->GetEntries(); ++entry) {
      tree->GetEntry(entry);
      metadata.productRegistry.push_back({
        productName ? *productName : std::string(),
        processName ? *processName : std::string(),
        producer ? *producer : std::string(),
        productID ? *productID : std::string(),
        productType ? *productType : std::string()});
    }

    metadata.hasProductRegistry = true;
    return true;
  }

  bool readIndexRegistryMetadata(TFile* tfile, FileMetadata& metadata)
  {
    auto* tree = tfile->Get<TTree>("IndexRegistry");
    if (tree == nullptr || tree->GetEntries() == 0) {
      return false;
    }

    auto* user_info = tree->GetUserInfo();
    if (user_info == nullptr || user_info->GetSize() == 0) {
      return false;
    }

    auto* schema_obj = dynamic_cast<TObjString*>(user_info->At(0));
    if (schema_obj == nullptr) {
      return false;
    }

    metadata.indexLayerSchema = parseSchemaHeader(schema_obj->GetString().Data());
    if (metadata.indexLayerSchema.empty()) {
      return false;
    }

    std::vector<ULong64_t> layer_values(metadata.indexLayerSchema.size(), 0);
    for (std::size_t i = 0; i < metadata.indexLayerSchema.size(); ++i) {
      TBranch* layer_branch = tree->GetBranch(metadata.indexLayerSchema[i].c_str());
      if (layer_branch == nullptr) {
        return false;
      }
      tree->SetBranchAddress(metadata.indexLayerSchema[i].c_str(), &layer_values[i]);
    }

    std::string* productID = nullptr;
    std::string* containerName = nullptr;
    ULong64_t payloadRow = 0;

    TBranch* productIDBranch = tree->GetBranch("ProductID");
    TBranch* containerNameBranch = tree->GetBranch("ContainerName");
    TBranch* payloadRowBranch = tree->GetBranch("PayloadRow");
    if (productIDBranch == nullptr || containerNameBranch == nullptr || payloadRowBranch == nullptr) {
      return false;
    }

    tree->SetBranchAddress("ProductID", &productID);
    tree->SetBranchAddress("ContainerName", &containerName);
    tree->SetBranchAddress("PayloadRow", &payloadRow);

    metadata.indexRegistry.clear();
    metadata.indexRegistry.reserve(tree->GetEntries());

    for (Long64_t entry = 0; entry < tree->GetEntries(); ++entry) {
      tree->GetEntry(entry);
      std::vector<std::uint64_t> stored_layer_values;
      stored_layer_values.reserve(layer_values.size());
      for (auto const value : layer_values) {
        stored_layer_values.push_back(static_cast<std::uint64_t>(value));
      }

      metadata.indexRegistry.push_back(
        {std::move(stored_layer_values),
         productID ? *productID : std::string(),
         containerName ? *containerName : std::string(),
         static_cast<std::uint64_t>(payloadRow)});
    }

    metadata.hasIndexRegistry = true;
    return true;
  }
#endif
}

// Factory function implementation
namespace form::detail::experimental {
  std::unique_ptr<IStorageReader> createStorageReader()
  {
    return std::unique_ptr<IStorageReader>(new StorageReader());
  }
}

int StorageReader::getIndex(Token const& token,
                            std::string const& id,
                            form::experimental::config::tech_setting_config const& settings)
{
  ensureFileMetadata(token.fileName(), token.technology());

  if (m_indexMaps[token.containerName()].empty()) {
    auto key = std::make_pair(token.fileName(), token.containerName());
    auto cont = m_read_containers.find(key);
    if (cont == m_read_containers.end()) {
      auto file = m_files.find(token.fileName());
      if (file == m_files.end()) {
        file =
          m_files.insert({token.fileName(), createFile(token.technology(), token.fileName(), 'i')})
            .first;
        for (auto const& [key, value] : settings.getFileTable(token.technology(), token.fileName()))
          file->second->setAttribute(key, value);
      }
      cont = m_read_containers
               .insert({key, createReadContainer(token.technology(), token.containerName())})
               .first;
      for (auto const& [key, value] :
           settings.getContainerTable(token.technology(), token.containerName()))
        cont->second->setAttribute(key, value);
      cont->second->setFile(file->second);
    }
    auto const& type = typeid(std::string);
    int entry = 1;
    void const* rawData = nullptr;
    while (cont->second->read(entry, &rawData, type)) {
      std::unique_ptr<std::string const> data(static_cast<std::string const*>(rawData));
      m_indexMaps[token.containerName()].insert(std::make_pair(*data, entry));
      entry++;
    }
  }
  int entry = m_indexMaps[token.containerName()][id];
  return entry;
}

void StorageReader::readContainer(Token const& token,
                                  void const** data,
                                  std::type_info const& type,
                                  std::string const& product_type,
                                  form::experimental::config::tech_setting_config const& settings)
{
  ensureFileMetadata(token.fileName(), token.technology());

  auto key = std::make_pair(token.fileName(), token.containerName());
  auto cont = m_read_containers.find(key);
  if (cont == m_read_containers.end()) {
    auto file = m_files.find(token.fileName());
    if (file == m_files.end()) {
      file =
        m_files.insert({token.fileName(), createFile(token.technology(), token.fileName(), 'i')})
          .first;
      for (auto const& [key, value] : settings.getFileTable(token.technology(), token.fileName()))
        file->second->setAttribute(key, value);
    }
    cont = m_read_containers
             .insert({key, createReadContainer(token.technology(), token.containerName())})
             .first;
    cont->second->setFile(file->second);
    for (auto const& [key, value] :
         settings.getContainerTable(token.technology(), token.containerName()))
      cont->second->setAttribute(key, value);
  }
  cont->second->read(token.id(), data, type, product_type);
  return;
}

bool StorageReader::hasFileCatalog(std::string const& fileName) const
{
  auto it = m_fileMetadata.find(fileName);
  return it != m_fileMetadata.end() && it->second.hasFileCatalog;
}

bool StorageReader::hasProductRegistry(std::string const& fileName) const
{
  auto it = m_fileMetadata.find(fileName);
  return it != m_fileMetadata.end() && it->second.hasProductRegistry;
}

FileMetadata const* StorageReader::getFileMetadata(std::string const& fileName) const
{
  auto it = m_fileMetadata.find(fileName);
  return it == m_fileMetadata.end() ? nullptr : &it->second;
}

FileMetadata const* StorageReader::loadFileMetadata(std::string const& fileName, int technology)
{
  ensureFileMetadata(fileName, technology);
  return getFileMetadata(fileName);
}

void StorageReader::ensureFileMetadata(std::string const& fileName, int technology)
{
  if (m_fileMetadata.find(fileName) != m_fileMetadata.end()) {
    return;
  }

#ifdef USE_ROOT_STORAGE
  auto file = m_files.find(fileName);
  if (file == m_files.end()) {
    file = m_files.insert({fileName, createFile(technology, fileName, 'i')}).first;
  }

  ROOT_TFileImp* root_file = dynamic_cast<ROOT_TFileImp*>(file->second.get());
  if (!root_file) {
    m_fileMetadata[fileName] = FileMetadata();
    return;
  }

  auto tfile = root_file->getTFile();
  if (!tfile) {
    m_fileMetadata[fileName] = FileMetadata();
    return;
  }

  FileMetadata metadata;
  readFileCatalogMetadata(tfile.get(), metadata);
  readProductRegistryMetadata(tfile.get(), metadata);
  readIndexRegistryMetadata(tfile.get(), metadata);
  m_fileMetadata[fileName] = std::move(metadata);
#else
  m_fileMetadata[fileName] = FileMetadata();
#endif
}
