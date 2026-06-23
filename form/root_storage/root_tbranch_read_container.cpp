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
#include <unordered_map>

using namespace form::detail::experimental;

namespace {
  bool type_name_looks_like_vector(std::string const& type_name)
  {
    return type_name.rfind("std::vector<", 0) == 0;
  }

  std::mutex& root_read_mutex()
  {
    static std::mutex m;
    return m;
  }
}

ROOT_TBranch_Read_ContainerImp::ROOT_TBranch_Read_ContainerImp(std::string const& name) :
  Storage_Read_Container(name)
{
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

bool ROOT_TBranch_Read_ContainerImp::read(int id,
                                          void const** data,
                                          std::type_info const& type,
                                          std::string const& product_type)
{
  std::lock_guard<std::mutex> guard(root_read_mutex());

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

  TClass* branchClass = nullptr;
  EDataType branchDataType = EDataType::kOther_t;
  if (m_branch->GetExpectedType(branchClass, branchDataType) < 0) {
    throw std::runtime_error("ROOT_TBranch_Read_ContainerImp::read unable to determine branch type for '" +
                             col_name() + "'");
  }

  bool const requested_wants_vector =
    !product_type.empty() ? type_name_looks_like_vector(product_type)
                          : type_name_looks_like_vector(DemangleName(type));

  gsl::owner<void*> branchBuffer = nullptr;
  int branchStatus = 0;

  if (branchClass == nullptr) {
    switch (branchDataType) {
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
        std::string{"ROOT_TBranch_ContainerImp::read unsupported fundamental branch type (EDataType="} +
        std::to_string(static_cast<int>(branchDataType)) + ")");
    };
    branchStatus = m_tree->SetBranchAddress(
      col_name().c_str(), branchBuffer, nullptr, branchDataType, false);
  } else {
    auto* requestedClass = TClass::GetClass(type);
    if (!requestedClass) {
      throw std::runtime_error(std::string{"ROOT_TBranch_ContainerImp::read missing requested TClass"} +
                               " (col_name='" + col_name() + "', requested='" + DemangleName(type) +
                               "')");
    }
    if (requestedClass != branchClass) {
      throw std::runtime_error(std::string{"ROOT_TBranch_ContainerImp::read class mismatch"} +
                               " (col_name='" + col_name() + "', branch_class='" +
                               branchClass->GetName() + "', requested='" + requestedClass->GetName() +
                               "', product_type='" + product_type + "')");
    }

    branchBuffer = gsl::owner<void*>(requestedClass->New());
    branchStatus = m_tree->SetBranchAddress(
      col_name().c_str(), reinterpret_cast<void*>(&branchBuffer), requestedClass, EDataType::kOther_t, true);
  }

  if (branchStatus < 0) {
    throw std::runtime_error(
      std::string{"ROOT_TBranch_ContainerImp::read SetBranchAddress() failed"} + " (col_name='" +
      col_name() + "', type='" + DemangleName(type) + "')" + " with error code " +
      std::to_string(branchStatus));
  }

  Long64_t tentry = m_tree->LoadTree(id);
  m_branch->GetEntry(tentry);

  if ((branchClass == nullptr) && requested_wants_vector) {
    switch (branchDataType) {
    case kChar_t: {
      auto* value = static_cast<Char_t*>(branchBuffer);
      *data = new std::vector<Char_t>{*value};
      delete value;
      break;
    }
    case kUChar_t: {
      auto* value = static_cast<UChar_t*>(branchBuffer);
      *data = new std::vector<UChar_t>{*value};
      delete value;
      break;
    }
    case kShort_t: {
      auto* value = static_cast<Short_t*>(branchBuffer);
      *data = new std::vector<Short_t>{*value};
      delete value;
      break;
    }
    case kUShort_t: {
      auto* value = static_cast<UShort_t*>(branchBuffer);
      *data = new std::vector<UShort_t>{*value};
      delete value;
      break;
    }
    case kInt_t: {
      auto* value = static_cast<Int_t*>(branchBuffer);
      *data = new std::vector<Int_t>{*value};
      delete value;
      break;
    }
    case kUInt_t: {
      auto* value = static_cast<UInt_t*>(branchBuffer);
      *data = new std::vector<UInt_t>{*value};
      delete value;
      break;
    }
    case kLong_t: {
      auto* value = static_cast<Long_t*>(branchBuffer);
      *data = new std::vector<Long_t>{*value};
      delete value;
      break;
    }
    case kULong_t: {
      auto* value = static_cast<ULong_t*>(branchBuffer);
      *data = new std::vector<ULong_t>{*value};
      delete value;
      break;
    }
    case kLong64_t: {
      auto* value = static_cast<Long64_t*>(branchBuffer);
      *data = new std::vector<Long64_t>{*value};
      delete value;
      break;
    }
    case kULong64_t: {
      auto* value = static_cast<ULong64_t*>(branchBuffer);
      *data = new std::vector<ULong64_t>{*value};
      delete value;
      break;
    }
    case kFloat_t: {
      auto* value = static_cast<Float_t*>(branchBuffer);
      *data = new std::vector<Float_t>{*value};
      delete value;
      break;
    }
    case kDouble_t: {
      auto* value = static_cast<Double_t*>(branchBuffer);
      *data = new std::vector<Double_t>{*value};
      delete value;
      break;
    }
    case kBool_t: {
      auto* value = static_cast<Bool_t*>(branchBuffer);
      *data = new std::vector<Bool_t>{*value};
      delete value;
      break;
    }
    default:
      throw std::runtime_error(
        std::string{"ROOT_TBranch_ContainerImp::read unsupported fundamental branch type (EDataType="} +
        std::to_string(static_cast<int>(branchDataType)) + ")");
    }
  } else {
    *data = branchBuffer;
  }

  // Reset the branch address to avoid unwanted ownership issues.
  m_branch->ResetAddress();

  return true;
}
