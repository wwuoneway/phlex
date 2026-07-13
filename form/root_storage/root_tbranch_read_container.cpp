// Copyright (C) 2025 ...

#include "root_tbranch_read_container.hpp"
#include "demangle_name.hpp"
#include "root_tfile.hpp"

#include "TBranch.h"
#include "TClass.h"
#include "TFile.h"
#include "TLeaf.h"
#include "TTree.h"

#include <gsl/pointers>

#include <mutex>

using namespace form::detail::experimental;

namespace {
  std::mutex& root_tbranch_read_mutex()
  {
    static std::mutex m;
    return m;
  }
}

ROOT_TBranch_Read_ContainerImp::ROOT_TBranch_Read_ContainerImp(std::string const& name) :
  Storage_Read_Container(name)
{
}

ROOT_TBranch_Read_ContainerImp::~ROOT_TBranch_Read_ContainerImp() { cleanupBranchBuffer(); }

void ROOT_TBranch_Read_ContainerImp::cleanupBranchBuffer()
{
  if (!m_branch_buffer) {
    return;
  }

  if (m_branch_buffer_klass != nullptr) {
    // Complex type: use TClass::Destructor to properly delete ROOT object
    m_branch_buffer_klass->Destructor(m_branch_buffer);
  } else {
    // Fundamental type: use delete (allocated via new in switch statement)
    delete[] static_cast<char*>(m_branch_buffer);
  }

  m_branch_buffer = nullptr;
  m_branch_buffer_klass = nullptr;
}

void ROOT_TBranch_Read_ContainerImp::setFile(std::shared_ptr<IStorage_File> file)
{
  ROOT_TFileImp* root_tfile_imp = dynamic_cast<ROOT_TFileImp*>(file.get());
  if (root_tfile_imp == nullptr) {
    throw std::runtime_error(
      "ROOT_TBranch_Read_ContainerImp::setFile can't attach to non-ROOT file");
  }
  m_tfile = root_tfile_imp->getTFile();
  return;
}

void ROOT_TBranch_Read_ContainerImp::prime(std::type_info const& type)
{
  std::lock_guard<std::mutex> guard(root_tbranch_read_mutex());

  if (m_tfile == nullptr) {
    throw std::runtime_error("ROOT_TBranch_Read_ContainerImp::prime no file attached");
  }
  if (m_tree == nullptr) {
    m_tree = m_tfile->Get<TTree>(top_name().c_str());
  }
  if (m_tree == nullptr) {
    throw std::runtime_error("ROOT_TBranch_Read_ContainerImp::prime no tree found with name " +
                             top_name());
  }
  if (m_branch == nullptr) {
    m_branch = m_tree->GetBranch(col_name().c_str());
  }
  if (m_branch == nullptr) {
    throw std::runtime_error("ROOT_TBranch_Read_ContainerImp::prime no branch found");
  }

  auto dictInfo = TDictionary::GetDictionary(type);
  if (!dictInfo) {
    throw std::runtime_error(
      std::string{"ROOT_TBranch_Read_ContainerImp::prime unsupported type: "} + DemangleName(type));
  }

  if (!(dictInfo->Property() & EProperty::kIsFundamental)) {
    auto klass = TClass::GetClass(type);
    if (!klass) {
      throw std::runtime_error(
        std::string{"ROOT_TBranch_Read_ContainerImp::prime missing TClass for type: "} +
        DemangleName(type));
    }
  }
}

bool ROOT_TBranch_Read_ContainerImp::read(int id, void const** data, std::type_info const& type)
{
  std::lock_guard<std::mutex> guard(root_tbranch_read_mutex());

  if (m_tfile == nullptr) {
    throw std::runtime_error("ROOT_TBranch_Read_ContainerImp::read no file attached");
  }
  if (m_tree == nullptr) {
    m_tree = m_tfile->Get<TTree>(top_name().c_str());
  }
  if (m_tree == nullptr) {
    throw std::runtime_error("ROOT_TBranch_Read_ContainerImp::read no tree found with name " +
                             top_name());
  }
  if (m_branch == nullptr) {
    m_branch = m_tree->GetBranch(col_name().c_str());
  }
  if (m_branch == nullptr) {
    throw std::runtime_error("ROOT_TBranch_Read_ContainerImp::read no branch found");
  }
  if (id > m_tree->GetEntries())
    return false;

  gsl::owner<void*> branchBuffer = nullptr;
  auto dictInfo = TDictionary::GetDictionary(type);
  int branchStatus = 0;

  if (!dictInfo) {
    throw std::runtime_error(std::string{"ROOT_TBranch_ContainerImp::read unsupported type: "} +
                             DemangleName(type));
  }

  if (dictInfo->Property() & EProperty::kIsFundamental) {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-static-cast-downcast)
    auto fundInfo = static_cast<TDataType*>(dictInfo); // Already checked to be fundamental
    switch (fundInfo->GetType()) {
    case kChar_t:
      branchBuffer = new Char_t;
      break;
    case kUChar_t:
      branchBuffer = new UChar_t;
      break;
    case kShort_t:
      branchBuffer = new Short_t;
      break;
    case kUShort_t:
      branchBuffer = new UShort_t;
      break;
    case kInt_t:
      branchBuffer = new Int_t;
      break;
    case kUInt_t:
      branchBuffer = new UInt_t;
      break;
    case kLong_t:
      branchBuffer = new Long_t;
      break;
    case kULong_t:
      branchBuffer = new ULong_t;
      break;
    case kLong64_t:
      branchBuffer = new Long64_t;
      break;
    case kULong64_t:
      branchBuffer = new ULong64_t;
      break;
    case kFloat_t:
      branchBuffer = new Float_t;
      break;
    case kDouble_t:
      branchBuffer = new Double_t;
      break;
    case kBool_t:
      branchBuffer = new Bool_t;
      break;
    default:
      throw std::runtime_error(
        std::string{"ROOT_TBranch_ContainerImp::read unsupported fundamental type: "} +
        DemangleName(type));
    };

    // Clean up previous buffer before assigning new one
    cleanupBranchBuffer();
    m_branch_buffer = branchBuffer;
    m_branch_buffer_klass = nullptr; // Mark as fundamental type

    branchStatus = m_tree->SetBranchAddress(
      col_name().c_str(), branchBuffer, nullptr, EDataType(fundInfo->GetType()), false);
  } else {
    auto klass = TClass::GetClass(type);
    if (!klass) {
      throw std::runtime_error(std::string{"ROOT_TBranch_ContainerImp::read missing TClass"} +
                               " (col_name='" + col_name() + "', type='" + DemangleName(type) +
                               "')");
    }
    branchBuffer = gsl::owner<void*>(klass->New());

    // Clean up previous buffer before assigning new one
    cleanupBranchBuffer();
    m_branch_buffer = branchBuffer;
    m_branch_buffer_klass = klass; // Store TClass for proper cleanup

    branchStatus = m_tree->SetBranchAddress(
      col_name().c_str(), reinterpret_cast<void*>(&branchBuffer), klass, EDataType::kOther_t, true);
  }

  if (branchStatus < 0) {
    throw std::runtime_error(
      std::string{"ROOT_TBranch_ContainerImp::read SetBranchAddress() failed"} + " (col_name='" +
      col_name() + "', type='" + DemangleName(type) + "')" + " with error code " +
      std::to_string(branchStatus));
  }

  Long64_t tentry = m_tree->LoadTree(id);
  m_branch->GetEntry(tentry);
  *data = branchBuffer;

  // Reset the branch address to avoid unwanted ownership issues.
  m_branch->ResetAddress();

  return true;
}

int ROOT_TBranch_Read_ContainerImp::entries()
{
  std::lock_guard<std::mutex> guard(root_tbranch_read_mutex());

  if (m_tfile == nullptr) {
    throw std::runtime_error("ROOT_TBranch_Read_ContainerImp::entries no file attached");
  }
  if (m_tree == nullptr) {
    m_tree = m_tfile->Get<TTree>(top_name().c_str());
  }
  if (m_tree == nullptr) {
    throw std::runtime_error("ROOT_TBranch_Read_ContainerImp::entries no tree found with name " +
                             top_name());
  }
  if (m_branch == nullptr) {
    m_branch = m_tree->GetBranch(col_name().c_str());
  }
  if (m_branch == nullptr) {
    throw std::runtime_error("ROOT_TBranch_Read_ContainerImp::entries no branch found");
  }
  return static_cast<int>(m_tree->GetEntries());
}
