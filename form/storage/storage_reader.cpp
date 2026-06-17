// Copyright (C) 2025 ...

#include "storage_reader.hpp"
#include "storage_file.hpp"
#include "storage_read_container.hpp"

#include "util/factories.hpp"

#ifdef USE_ROOT_STORAGE
#include "root_storage/root_tfile.hpp"
#include "TBranch.h"
#include "TFile.h"
#include "TTree.h"
#endif

using namespace form::detail::experimental;

namespace {
#ifdef USE_ROOT_STORAGE
  bool readFileCatalogMetadata(TFile* tfile, StorageReader::FileMetadata& metadata)
  {
    auto* tree = tfile->Get<TTree>("FileCatalog");
    if (tree == nullptr || tree->GetEntries() == 0) {
      return false;
    }

    std::string fileUUID;
    int fileFormatVersion = 0;
    TBranch* uuidBranch = tree->GetBranch("FileUUID");
    TBranch* formatBranch = tree->GetBranch("FileFormatVersion");
    if (uuidBranch == nullptr || formatBranch == nullptr) {
      return false;
    }

    tree->SetBranchAddress("FileUUID", &fileUUID);
    tree->SetBranchAddress("FileFormatVersion", &fileFormatVersion);
    tree->GetEntry(0);

    metadata.fileCatalog.fileUUID = std::move(fileUUID);
    metadata.fileCatalog.fileFormatVersion = fileFormatVersion;
    metadata.hasFileCatalog = true;
    return true;
  }

  bool readProductRegistryMetadata(TFile* tfile, StorageReader::FileMetadata& metadata)
  {
    auto* tree = tfile->Get<TTree>("ProductRegistry");
    if (tree == nullptr || tree->GetEntries() == 0) {
      return false;
    }

    std::string productName;
    std::string processName;
    std::string producer;
    std::string productID;

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

    metadata.productRegistry.clear();
    metadata.productRegistry.reserve(tree->GetEntries());

    for (Long64_t entry = 0; entry < tree->GetEntries(); ++entry) {
      tree->GetEntry(entry);
      metadata.productRegistry.push_back({productName, processName, producer, productID});
    }

    metadata.hasProductRegistry = true;
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
  cont->second->read(token.id(), data, type);
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

StorageReader::FileMetadata const* StorageReader::getFileMetadata(std::string const& fileName) const
{
  auto it = m_fileMetadata.find(fileName);
  return it == m_fileMetadata.end() ? nullptr : &it->second;
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
  m_fileMetadata[fileName] = std::move(metadata);
#else
  m_fileMetadata[fileName] = FileMetadata();
#endif
}
