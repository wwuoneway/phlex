// Copyright (C) 2025 ...

#include "data_products/track_start.hpp"
#include "form/form_reader.hpp"
#include "form/technology.hpp"
#include "storage/storage_reader.hpp"
#include "test_helpers.hpp"
#include "test_utils.hpp"

#include "TFile.h"
#include "TTree.h"

#include <string>
#include <cmath>
#include <format>
#include <fstream>
#include <map>
#include <memory>
#include <sstream>
#include <utility>
#include <vector>

static int const NUMBER_EVENT = 4;
static int const NUMBER_SEGMENT = 15;

static float const TOLERANCE = 1e-3f;

// Structs to hold expected checksums
struct SegChecksum {
  float check;
  float cpx, cpy, cpz;
};

struct EvtChecksum {
  float check;
};

struct IndexRegistryRow {
  std::string containerName;
  unsigned long long payloadRow = 0;
};

bool verifyToyFileMetadata(std::string const& filename)
{
  TFile* file = TFile::Open(filename.c_str(), "READ");
  if (!file || file->IsZombie()) {
    std::cerr << "PHLEX: Metadata verification FAILED: could not open file for metadata check: "
              << filename << '\n';
    return false;
  }

  TTree* prodRegistry = file->Get<TTree>("ProductRegistry");
  TTree* indexRegistry = file->Get<TTree>("IndexRegistry");
  if (!prodRegistry || !indexRegistry) {
    std::cerr << "PHLEX: Metadata verification FAILED: missing ProductRegistry or IndexRegistry in "
              << filename << '\n';
    file->Close();
    return false;
  }

  std::string* productName = nullptr;
  std::string* processName = nullptr;
  std::string* producer = nullptr;
  std::string* productID = nullptr;
  std::string* productType = nullptr;

  prodRegistry->ResetBranchAddresses();
  prodRegistry->SetBranchAddress("ProductName", &productName);
  prodRegistry->SetBranchAddress("ProcessName", &processName);
  prodRegistry->SetBranchAddress("Producer", &producer);
  prodRegistry->SetBranchAddress("ProductID", &productID);
  if (prodRegistry->GetBranch("ProductType") != nullptr) {
    prodRegistry->SetBranchAddress("ProductType", &productType);
  }

  bool found_toy_tracker = false;
  bool found_toy_tracker_event = false;
  std::string toy_tracker_id;
  std::string toy_tracker_event_id;
  std::string toy_tracker_type;
  std::string toy_tracker_event_type;
  std::map<std::string, std::string> productIDToName;

  for (Long64_t entry = 0; entry < prodRegistry->GetEntries(); ++entry) {
    prodRegistry->GetEntry(entry);
    std::string pid = productID ? *productID : std::string();
    std::string pname = productName ? *productName : std::string();
    productIDToName[pid] = pname;
    if (pname == "Toy_Tracker") {
      found_toy_tracker = true;
      toy_tracker_id = pid;
      toy_tracker_type = productType ? *productType : std::string();
    } else if (pname == "Toy_Tracker_Event") {
      found_toy_tracker_event = true;
      toy_tracker_event_id = pid;
      toy_tracker_event_type = productType ? *productType : std::string();
    }
  }

  if (!found_toy_tracker || !found_toy_tracker_event) {
    std::cerr << "PHLEX: Metadata verification FAILED: expected Toy_Tracker and Toy_Tracker_Event entries missing from ProductRegistry." << '\n';
    file->Close();
    return false;
  }

  if (toy_tracker_type != "std::vector<TrackStart>" || toy_tracker_event_type != "std::vector<float>") {
    std::cerr << "PHLEX: Metadata verification FAILED: unexpected ProductType values in ProductRegistry." << '\n';
    std::cerr << "  Toy_Tracker type='" << toy_tracker_type << "'\n";
    std::cerr << "  Toy_Tracker_Event type='" << toy_tracker_event_type << "'\n";
    file->Close();
    return false;
  }

  std::string* indexProductID = nullptr;
  std::string* indexContainerName = nullptr;
  unsigned long long indexPayloadRow = 0;

  indexRegistry->ResetBranchAddresses();
  indexRegistry->SetBranchAddress("ProductID", &indexProductID);
  indexRegistry->SetBranchAddress("ContainerName", &indexContainerName);
  indexRegistry->SetBranchAddress("PayloadRow", &indexPayloadRow);

  std::map<std::string, std::vector<IndexRegistryRow>> indexEntries;
  for (Long64_t entry = 0; entry < indexRegistry->GetEntries(); ++entry) {
    indexRegistry->GetEntry(entry);
    std::string pid = indexProductID ? *indexProductID : std::string();
    std::string cname = indexContainerName ? *indexContainerName : std::string();
    if (productIDToName.find(pid) == productIDToName.end()) {
      std::cerr << "PHLEX: Metadata verification FAILED: IndexRegistry references unknown ProductID=" << pid << '\n';
      file->Close();
      return false;
    }
    indexEntries[pid].push_back({cname, indexPayloadRow});
  }

  auto verifyTreeStructure = [&](std::string const& treeName, std::vector<std::string> const& expectedBranches) {
    TTree* tree = file->Get<TTree>(treeName.c_str());
    if (!tree) {
      std::cerr << "PHLEX: Metadata verification FAILED: expected container tree not found: " << treeName << '\n';
      return false;
    }
    for (auto const& branchName : expectedBranches) {
      if (!tree->GetBranch(branchName.c_str())) {
        std::cerr << "PHLEX: Metadata verification FAILED: expected branch '" << branchName << "' missing in tree " << treeName << '\n';
        return false;
      }
    }
    return true;
  };

  if (!verifyTreeStructure("Toy_Tracker", {"trackNumberHits", "trackStart", "trackStartPoints", "index"}) ||
      !verifyTreeStructure("Toy_Tracker_Event", {"trackStartX", "index"})) {
    file->Close();
    return false;
  }

  auto verifyPayloadRows = [&](std::string const& treeName, std::vector<IndexRegistryRow> const& entries) {
    TTree* tree = file->Get<TTree>(treeName.c_str());
    if (!tree) {
      std::cerr << "PHLEX: Metadata verification FAILED: missing tree while validating payload rows: " << treeName << '\n';
      return false;
    }
    auto const rowCount = static_cast<unsigned long long>(tree->GetEntries());
    for (auto const& e : entries) {
      if (e.payloadRow >= rowCount) {
        std::cerr << "PHLEX: Metadata verification FAILED: PayloadRow out of range for tree " << treeName
                  << ", row=" << e.payloadRow << ", entries=" << rowCount << '\n';
        return false;
      }
    }
    return true;
  };

  if (indexEntries[toy_tracker_id].size() != 60) {
    std::cerr << "PHLEX: Metadata verification FAILED: unexpected IndexRegistry entry counts for Toy_Tracker." << '\n';
    std::cerr << "  Toy_Tracker entries=" << indexEntries[toy_tracker_id].size() << '\n';
    file->Close();
    return false;
  }

  // Event-level product may not be represented in IndexRegistry (top-level tree exists)
  if (!indexEntries[toy_tracker_event_id].empty() && indexEntries[toy_tracker_event_id].size() != 4) {
    std::cerr << "PHLEX: Metadata verification FAILED: unexpected IndexRegistry entry counts for Toy_Tracker_Event." << '\n';
    std::cerr << "  Toy_Tracker_Event entries=" << indexEntries[toy_tracker_event_id].size() << '\n';
    file->Close();
    return false;
  }

  if (!verifyPayloadRows("Toy_Tracker", indexEntries[toy_tracker_id])) {
    file->Close();
    return false;
  }

  if (!indexEntries[toy_tracker_event_id].empty()) {
    if (!verifyPayloadRows("Toy_Tracker_Event", indexEntries[toy_tracker_event_id])) {
      file->Close();
      return false;
    }
  } else {
    // No index entries for event-level product; ensure the tree exists and has expected entries
    TTree* evtTree = file->Get<TTree>("Toy_Tracker_Event");
    if (!evtTree || evtTree->GetEntries() != 4) {
      std::cerr << "PHLEX: Metadata verification FAILED: Toy_Tracker_Event tree missing or wrong size." << '\n';
      if (evtTree) std::cerr << "  entries=" << evtTree->GetEntries() << " expected=4\n";
      file->Close();
      return false;
    }
  }

  std::cout << "PHLEX: Metadata verification PASSED. ProductRegistry and IndexRegistry are consistent with actual containers." << '\n';
  file->Close();
  return true;
}

int main(int argc, char** argv)
{
  std::cout << "In main" << '\n';

  std::string const filename = (argc > 1) ? argv[1] : "toy.root";
  std::string const checksum_filename = (argc > 2) ? argv[2] : "toy_checksums.txt";
  int const technology = form::test::getTechnology((argc > 3) ? argv[3] : "ROOT_TTREE");

  // Load expected checksums from file
  std::map<std::pair<int, int>, SegChecksum> expected_seg;
  std::map<int, EvtChecksum> expected_evt;

  std::ifstream checksum_file(checksum_filename);
  if (!checksum_file.is_open()) {
    std::cerr << "ERROR: Could not open checksum file: " << checksum_filename << '\n';
    return 1;
  }

  std::string line;
  while (std::getline(checksum_file, line)) {
    std::istringstream iss(line);
    std::string type;
    iss >> type;
    if (type == "SEG") {
      SegChecksum cs{};
      int nevent{}, nseg{};
      iss >> nevent >> nseg >> cs.check >> cs.cpx >> cs.cpy >> cs.cpz;
      expected_seg[{nevent, nseg}] = cs;
    } else if (type == "EVT") {
      EvtChecksum cs{};
      int nevent{};
      iss >> nevent >> cs.check;
      expected_evt[nevent] = cs;
    }
  }
  checksum_file.close();

  // TODO: Read configuration from config file instead of hardcoding
  form::experimental::config::ItemConfig config_items;
  config_items.addItem("trackStart", filename, technology);
  config_items.addItem("trackNumberHits", filename, technology);
  config_items.addItem("trackStartPoints", filename, technology);
  config_items.addItem("trackStartX", filename, technology);

  form::experimental::config::tech_setting_config tech_config;

  form::experimental::form_reader_interface form(config_items, tech_config);

  bool all_passed = true;

  for (int nevent = 0; nevent < NUMBER_EVENT; nevent++) {
    std::cout << "PHLEX: Read Event No. " << nevent << '\n';

    std::unique_ptr<std::vector<float> const> track_x;

    for (int nseg = 0; nseg < NUMBER_SEGMENT; nseg++) {

      void const* rawPtr = nullptr;
      std::string const seg_id_text = std::format("[EVENT={:08X};SEG={:08X}]", nevent, nseg);

      std::string const& segment_id = seg_id_text;

      std::string const creator = "Toy_Tracker";

      form::experimental::product_with_name pb = {
        "trackStart", rawPtr, &typeid(std::vector<float>)};

      form.read(creator, segment_id, pb);
      std::unique_ptr<std::vector<float> const> track_start_x(
        static_cast<std::vector<float> const*>(pb.data));

      rawPtr = nullptr;
      form::experimental::product_with_name pb_int = {
        "trackNumberHits", rawPtr, &typeid(std::vector<int>)};

      form.read(creator, segment_id, pb_int);
      std::unique_ptr<std::vector<int> const> track_n_hits(
        static_cast<std::vector<int> const*>(pb_int.data));

      rawPtr = nullptr;
      form::experimental::product_with_name pb_points = {
        "trackStartPoints", rawPtr, &typeid(std::vector<TrackStart>)};

      form.read(creator, segment_id, pb_points);
      std::unique_ptr<std::vector<TrackStart> const> start_points(
        static_cast<std::vector<TrackStart> const*>(pb_points.data));

      float check = 0.0;
      for (float val : *track_start_x)
        check += val;
      for (int val : *track_n_hits)
        check += static_cast<float>(val);
      TrackStart checkPoints;
      for (TrackStart val : *start_points)
        checkPoints += val;
      std::cout << "PHLEX: Segment = " << nseg << ": seg_id_text = " << seg_id_text
                << ", check = " << check << '\n';
      std::cout << "PHLEX: Segment = " << nseg << ": seg_id_text = " << seg_id_text
                << ", checkPoints = " << checkPoints << '\n';

      // Verify segment checksums
      auto key = std::make_pair(nevent, nseg);
      if (expected_seg.count(key)) {
        auto const& exp = expected_seg[key];
        bool seg_ok = (std::fabs(check - exp.check) <= TOLERANCE) &&
                      (std::fabs(checkPoints.getX() - exp.cpx) <= TOLERANCE) &&
                      (std::fabs(checkPoints.getY() - exp.cpy) <= TOLERANCE) &&
                      (std::fabs(checkPoints.getZ() - exp.cpz) <= TOLERANCE);
        if (seg_ok) {
          std::cout << "VERIFY PASS: event=" << nevent << " seg=" << nseg << '\n';
        } else {
          std::cerr << "VERIFY FAIL: event=" << nevent << " seg=" << nseg
                    << " expected check=" << exp.check << " got=" << check
                    << " expected cpx=" << exp.cpx << " got=" << checkPoints.getX()
                    << " expected cpy=" << exp.cpy << " got=" << checkPoints.getY()
                    << " expected cpz=" << exp.cpz << " got=" << checkPoints.getZ() << '\n';
          all_passed = false;
        }
      } else {
        std::cerr << "VERIFY FAIL: no expected checksum for event=" << nevent << " seg=" << nseg
                  << '\n';
        all_passed = false;
      }
    }
    std::cout << "PHLEX: Read Event segments done " << nevent << '\n';

    std::string const evt_id_text = std::format("[EVENT={:08X}]", nevent);

    std::string const& event_id = evt_id_text;

    std::string const creator = "Toy_Tracker_Event";

    void const* rawEvtPtr = nullptr;
    form::experimental::product_with_name pb_evt = {
      "trackStartX", rawEvtPtr, &typeid(std::vector<float>)};

    form.read(creator, event_id, pb_evt);
    track_x.reset(static_cast<std::vector<float> const*>(pb_evt.data));

    float check = 0.0;
    for (float val : *track_x)
      check += val;
    std::cout << "PHLEX: Event = " << nevent << ": evt_id_text = " << evt_id_text
              << ", check = " << check << '\n';

    // Verify event checksum
    if (expected_evt.count(nevent)) {
      auto const& exp = expected_evt[nevent];
      bool evt_ok = (std::fabs(check - exp.check) <= TOLERANCE);
      if (evt_ok) {
        std::cout << "VERIFY PASS: event=" << nevent << '\n';
      } else {
        std::cerr << "VERIFY FAIL: event=" << nevent << " expected check=" << exp.check
                  << " got=" << check << '\n';
        all_passed = false;
      }
    } else {
      std::cerr << "VERIFY FAIL: no expected checksum for event=" << nevent << '\n';
      all_passed = false;
    }

    std::cout << "PHLEX: Read Event done " << nevent << '\n';
  }

  // Verify file metadata through StorageReader to ensure ProductRegistry is present.
  // In toy.root, ProductRegistry entries represent the logical production names
  // (Toy_Tracker and Toy_Tracker_Event) rather than the contained field names.
  {
    form::detail::experimental::StorageReader metadata_reader;
    auto const* meta = metadata_reader.loadFileMetadata(filename, technology);
    if (meta == nullptr || !meta->hasFileCatalog || !meta->hasProductRegistry) {
      std::cerr << "PHLEX: Metadata verification FAILED: missing FileCatalog or ProductRegistry." << '\n';
      return 1;
    }

    bool found_toy_tracker = false;
    bool found_toy_tracker_event = false;
    std::string toy_tracker_type;
    std::string toy_tracker_event_type;

    for (auto const& entry : meta->productRegistry) {
      if (entry.productName == "Toy_Tracker") {
        found_toy_tracker = true;
        toy_tracker_type = entry.productType;
      } else if (entry.productName == "Toy_Tracker_Event") {
        found_toy_tracker_event = true;
        toy_tracker_event_type = entry.productType;
      }
    }

    if (!found_toy_tracker || !found_toy_tracker_event) {
      std::cerr << "PHLEX: Metadata verification FAILED: expected Toy_Tracker and Toy_Tracker_Event entries missing from ProductRegistry." << '\n';
      return 1;
    }

    if (toy_tracker_type != "std::vector<TrackStart>" || toy_tracker_event_type != "std::vector<float>") {
      std::cerr << "PHLEX: Metadata verification FAILED: unexpected ProductType values in ProductRegistry." << '\n';
      std::cerr << "  Toy_Tracker type='" << toy_tracker_type << "'\n";
      std::cerr << "  Toy_Tracker_Event type='" << toy_tracker_event_type << "'\n";
      return 1;
    }

    std::cout << "PHLEX: ProductRegistry validation PASSED. ProductRegistry contains expected product entries." << '\n';
    if (!verifyToyFileMetadata(filename)) {
      return 1;
    }
  }

  if (all_passed) {
    std::cout << "PHLEX: All verification checks PASSED." << '\n';
    return 0;
  } else {
    std::cerr << "PHLEX: Some verification checks FAILED." << '\n';
    return 1;
  }
}
