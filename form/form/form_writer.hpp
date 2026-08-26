// Copyright (C) 2025 ...

#ifndef FORM_FORM_FORM_WRITER_HPP
#define FORM_FORM_FORM_WRITER_HPP

#include "core/token_registry.hpp"
#include "form/config.hpp"
#include "form/product_with_name.hpp"
#include "persistence/ipersistence_writer.hpp"

#include <map>
#include <memory>
#include <string>
#include <vector>

namespace form::experimental {

  class form_writer_interface {
  public:
    form_writer_interface(config::item_config const& config_item,
                          config::tech_setting_config const& tech_config);
    // Dependency-injection seam for tests: supply the persistence backend. Throws
    // std::invalid_argument if pers_writer is null.
    form_writer_interface(
      config::item_config const& config_item,
      config::tech_setting_config const& tech_config,
      std::unique_ptr<form::detail::experimental::i_persistence_writer> pers_writer);
    ~form_writer_interface() = default;

    void write(std::string const& creator,
               std::string const& segment_id,
               product_with_name const& product);

    void write(std::string const& creator,
               std::string const& segment_id,
               std::vector<product_with_name> const& products);

    // Tokens collected from register_write, keyed by segment id + creator + product label. The
    // write-side navigation seam: a later navigation writer will read this to persist the locators.
    form::detail::experimental::token_registry const& tokens() const { return token_registry_; }

  private:
    std::unique_ptr<form::detail::experimental::i_persistence_writer> pers_writer_;
    std::map<std::string, form::experimental::config::persistence_item> product_to_config_;
    form::detail::experimental::token_registry token_registry_;
  };
}

#endif // FORM_FORM_FORM_WRITER_HPP
