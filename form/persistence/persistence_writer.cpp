// Copyright (C) 2025 ...

#include "persistence_writer.hpp"
#include "persistence_utils.hpp"

#include <algorithm>
#include <cstring>
#include <stdexcept>
#include <string>
#include <typeinfo>
#include <utility>
#include <vector>

using namespace form::detail::experimental;

namespace {
  // Name of the navigation container: not a data product, but the locator written alongside the
  // products so a segment id can be read back later. The literal "index" is the pre-existing
  // on-disk name (the reader looks for it by that name), kept unchanged for file compatibility.
  constexpr char const* index_label = "index";
}

namespace form::detail::experimental {
  std::unique_ptr<i_persistence_writer> create_persistence_writer()
  {
    return std::make_unique<persistence_writer>();
  }
}

persistence_writer::persistence_writer() : persistence_writer(create_storage_writer()) {}

persistence_writer::persistence_writer(std::unique_ptr<i_storage_writer> store_writer) :
  store_writer_(std::move(store_writer)), tech_settings_()
{
  if (!store_writer_) {
    throw std::invalid_argument("persistence_writer requires a non-null storage writer");
  }
}

void persistence_writer::configure_tech_settings(
  form::experimental::config::tech_setting_config const& tech_config_settings)
{
  tech_settings_ = tech_config_settings;
}

void persistence_writer::configure(form::experimental::config::item_config const& config_items)
{
  // Resolve the configuration once, here, so the per-record write path does no config scanning.
  // Each product label maps to its {file, technology}; the navigation ("index") container gets an
  // explicit entry of its own rather than borrowing items[0] at lookup time.
  resolved_config_.clear();
  auto const& items = config_items.get_items();
  for (auto const& item : items) {
    // emplace, not insert_or_assign: on a duplicate product_name the FIRST entry wins, matching
    // item_config::find_item's first-match linear scan that this map replaces.
    resolved_config_.emplace(item.product_name, item);
  }
  if (!items.empty()) {
    // The navigation container is written alongside the products, into the first configured item's
    // file and technology (preserving the pre-PR behaviour where find_config_item("index") always
    // returned items[0]). insert_or_assign so this wins even over a product literally named
    // "index", exactly as that special case did.
    auto const& first = items.front();
    resolved_config_.insert_or_assign(
      index_label,
      form::experimental::config::persistence_item(index_label, first.file_name, first.technology));
  }
}

void persistence_writer::create_containers(
  std::string const& creator, std::map<std::string, std::type_info const*> const& products)
{
  // Each container is created exactly once. A container counts as created only after the storage
  // call below returns successfully (created_containers_); on later records every container is
  // already created, so there is nothing new and we skip the storage call entirely.
  std::map<std::unique_ptr<placement>, std::type_info const*> new_containers;
  std::vector<std::string> pending; // full labels to mark created iff the storage call succeeds

  auto add_if_new = [&](std::string const& label, std::type_info const* type) {
    std::string full_label = build_full_label(creator, label);
    if (created_containers_.count(full_label) != 0) {
      return; // already created on an earlier record
    }
    placement const& plcmnt = ensure_placement(creator, label);
    new_containers.insert(std::make_pair(std::make_unique<placement>(plcmnt), type));
    pending.push_back(std::move(full_label));
  };

  for (auto const& [label, type] : products) {
    add_if_new(label, type);
  }
  add_if_new(index_label, &typeid(std::string));

  if (!new_containers.empty()) {
    store_writer_->create_containers(new_containers, tech_settings_);
    // Only now, past a successful create, are these containers guaranteed to exist. If the call
    // above throws, created_containers_ is left untouched and the creation is retried next record.
    for (auto& full_label : pending) {
      created_containers_.insert(std::move(full_label));
    }
  }
}

token persistence_writer::register_write(std::string const& creator,
                                         std::string const& label,
                                         void const* data,
                                         std::type_info const& type)
{
  placement const& plcmnt = ensure_placement(creator, label);
  std::uint64_t const row = store_writer_->fill_container(plcmnt, data, type);
  // A returned token must locate a readable product: its row is the read-side navigation key.
  // invalid_row_id means backend does not address rows,so a product routed there could not be located on read, so throw here for such an unusable token
  if (row == invalid_row_id) {
    throw std::runtime_error("persistence_writer::register_write backend for product '" + label +
                             "' from creator '" + creator + "' does not address rows; " +
                             "cannot produce a token locating the written product");
  }
  return token{plcmnt.file_name(), plcmnt.container_name(), plcmnt.technology(), row};
}

void persistence_writer::commit_output(std::string const& creator, std::string const& id)
{
  placement const& plcmnt = ensure_placement(creator, index_label);
  store_writer_->fill_container(plcmnt, &id, typeid(std::string));
  store_writer_->commit_containers(plcmnt);
}

placement const& persistence_writer::ensure_placement(std::string const& creator,
                                                      std::string const& label)
{
  std::string const full_label = build_full_label(creator, label);
  auto const cached = placements_.find(full_label);
  if (cached != placements_.end()) {
    return *cached->second;
  }

  auto const config_item = resolved_config_.find(label);
  if (config_item == resolved_config_.end()) {
    throw std::runtime_error("No configuration found for product: " + label +
                             " from creator: " + creator);
  }

  // The find above proved the key is absent, so emplace always inserts (no assign case to handle).
  auto const inserted = placements_.emplace(
    full_label,
    std::make_unique<placement>(
      config_item->second.file_name, full_label, config_item->second.technology));
  return *inserted.first->second;
}
