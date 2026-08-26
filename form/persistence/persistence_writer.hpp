// Copyright (C) 2025 ...

#ifndef FORM_PERSISTENCE_PERSISTENCE_WRITER_HPP
#define FORM_PERSISTENCE_PERSISTENCE_WRITER_HPP

#include "ipersistence_writer.hpp"

#include "core/placement.hpp"
#include "form/config.hpp"
#include "storage/istorage.hpp"

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace form::detail::experimental {

  class persistence_writer : public i_persistence_writer {
  public:
    persistence_writer();
    // Dependency-injection seam for tests: supply the storage backend. Throws std::invalid_argument
    // if store_writer is null.
    explicit persistence_writer(std::unique_ptr<i_storage_writer> store_writer);
    ~persistence_writer() override = default;
    void configure_tech_settings(
      form::experimental::config::tech_setting_config const& tech_config_settings) override;

    void configure(form::experimental::config::item_config const& config_items) override;

    void create_containers(std::string const& creator,
                           std::map<std::string, std::type_info const*> const& products) override;
    token register_write(std::string const& creator,
                         std::string const& label,
                         void const* data,
                         std::type_info const& type) override;
    void commit_output(std::string const& creator, std::string const& id) override;

  private:
    // Resolve+cache the placement for (creator, label) on first sight; reused thereafter. This
    // only records that the placement has been *resolved* (its file/container/technology are known)
    // -- it says nothing about whether the container has been created; see created_containers_.
    placement const& ensure_placement(std::string const& creator, std::string const& label);

    std::unique_ptr<i_storage_writer> store_writer_;
    form::experimental::config::tech_setting_config tech_settings_;

    // Configuration resolved once (in configure()): product label -> {file, technology}, plus an
    // explicit entry for the navigation ("index") container. Removes per-record config scanning.
    std::unordered_map<std::string, form::experimental::config::persistence_item> resolved_config_;
    // Placements resolved once, keyed by full label (creator + "/" + label). A resolution cache
    // only -- presence does NOT imply the container exists.
    std::unordered_map<std::string, std::unique_ptr<placement>> placements_;
    // The create-once guard: a full label is inserted here only *after* store_writer_ has
    // successfully created its container, so a creation that throws is retried on the next record
    // (the authoritative idempotency guard also lives in storage_writer::write_containers_).
    std::unordered_set<std::string> created_containers_;
  };

} // namespace form::detail::experimental

#endif // FORM_PERSISTENCE_PERSISTENCE_WRITER_HPP
