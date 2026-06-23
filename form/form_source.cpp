#include "phlex/source.hpp"

#include "form/config.hpp"
#include "form/form_reader.hpp"
#include "form/form/form_source_type_registry.hpp"
#include "form/technology.hpp"
#include "form/storage/storage_reader.hpp"
#include "phlex/model/data_cell_index.hpp"

#include <algorithm>
#include <cctype>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>
#include <unordered_map>
#include <vector>

namespace {

  std::string normalize_layer_name(std::string layer)
  {
    std::transform(layer.begin(), layer.end(), layer.begin(), [](unsigned char c) {
      return static_cast<char>(std::tolower(c));
    });
    return layer;
  }
}

namespace {

  class FormInputSource : public phlex::experimental::source {
  public:
    FormInputSource(std::string input_file,
                    std::vector<std::string> products,
                    std::string advertised_creator,
                    int technology,
                    int starting_cell,
                    int cells_to_read,
                    int skip_every,
                    int cell_number) :
      input_file_(std::move(input_file)),
      products_(std::move(products)),
      advertised_creator_(std::move(advertised_creator)),
      technology_(technology),
      starting_cell_(starting_cell),
      cells_to_read_(cells_to_read),
      skip_every_(skip_every),
      cell_number_(cell_number),
      reader_(nullptr),
      metadata_(nullptr)
    {
      // Load FORM metadata and retain the reader for lifetime safety.
      metadata_ = storage_reader_.loadFileMetadata(input_file_, technology_);
      if (metadata_ == nullptr || !metadata_->hasProductRegistry) {
        throw std::runtime_error("FORM input file " + input_file_ +
                                 " does not contain a ProductRegistry");
      }

      // Build input config for FORM reader
      form::experimental::config::ItemConfig input_cfg;
      form::experimental::config::tech_setting_config tech_cfg;
      for (auto const& name : products_) {
        input_cfg.addItem(name, input_file_, technology_);
      }

      reader_ = std::make_shared<form::experimental::form_reader_interface>(input_cfg, tech_cfg);

      build_indices_from_metadata();
    }

    phlex::experimental::provider_bundles create_providers(
      phlex::product_selector const& selector) override
    {
      using namespace phlex::experimental;
      provider_bundles bundles;

      // For each product in the products list, check if it matches the selector
      for (auto const& product_name : products_) {
        // Find product entry in metadata
        std::optional<std::string> product_type_opt;
        std::optional<form::detail::experimental::ProductRegistryEntry> entry_opt;

        for (auto const& entry : metadata_->productRegistry) {
          if (entry.productName == product_name) {
            product_type_opt = entry.productType;
            entry_opt = entry;
            break;
          }
        }

        if (!product_type_opt.has_value() || product_type_opt->empty()) {
          throw std::runtime_error("Unable to determine FORM container type for product: " +
                                   product_name);
        }

        if (!entry_opt.has_value()) {
          throw std::runtime_error("Product not found in ProductRegistry: " + product_name);
        }

        // Determine actual FORM creator from metadata, or fall back to advertised creator.
        std::string actual_creator = entry_opt->producer;
        if (actual_creator.empty()) {
          actual_creator = advertised_creator_;
        }

        // Use advertised creator for Phlex product registration.
        std::string advertised_creator = advertised_creator_.empty() ? actual_creator : advertised_creator_;
        product_specification spec(algorithm_name::create(advertised_creator),
                                   identifier(product_name),
                                   make_type_id_for_form_type(*product_type_opt));

        // Check if selector matches this product (layer="event", stage="CURRENT")
        if (!selector.match(spec, identifier("event"), identifier("CURRENT"))) {
          continue;
        }

        // Create provider bundle based on type
        auto bundle = create_provider_bundle_for_type(
          actual_creator, product_name, *product_type_opt, spec);
        if (bundle.has_value()) {
          bundles.push_back(std::move(bundle.value()));
        }
      }

      return bundles;
    }

    phlex::index_generator indices() override
    {
      for (auto const& idx : index_sequence_) {
        co_yield idx;
      }
    }

  private:
    std::string input_file_;
    std::vector<std::string> products_;
    std::string advertised_creator_;
    int technology_;
    int starting_cell_;
    int cells_to_read_;
    int skip_every_;
    int cell_number_;
    form::detail::experimental::StorageReader storage_reader_;
    std::shared_ptr<form::experimental::form_reader_interface> reader_;
    form::detail::experimental::FileMetadata const* metadata_;
    std::vector<phlex::data_cell_index_ptr> index_sequence_;

    void build_indices_from_metadata()
    {
      if (metadata_ == nullptr || !metadata_->hasIndexRegistry ||
          metadata_->indexLayerSchema.empty() || metadata_->indexRegistry.empty()) {
        return;
      }

      std::vector<phlex::data_cell_index_ptr> all_indices;
      all_indices.reserve(metadata_->indexRegistry.size());
      std::unordered_set<std::string> seen;

      for (auto const& row : metadata_->indexRegistry) {
        if (row.layerValues.size() != metadata_->indexLayerSchema.size()) {
          continue;
        }

        std::string dedupe_key;
        for (std::size_t i = 0; i < row.layerValues.size(); ++i) {
          if (i > 0) {
            dedupe_key.push_back('|');
          }
          dedupe_key += std::to_string(row.layerValues[i]);
        }

        if (!seen.insert(dedupe_key).second) {
          continue;
        }

        auto idx = phlex::data_cell_index::job();
        for (std::size_t i = 0; i < metadata_->indexLayerSchema.size(); ++i) {
          idx = idx->make_child(normalize_layer_name(metadata_->indexLayerSchema[i]),
                                static_cast<std::size_t>(row.layerValues[i]));
        }
        all_indices.push_back(std::move(idx));
      }

      if (all_indices.empty()) {
        return;
      }

      if (cell_number_ >= 0) {
        std::size_t const selected = static_cast<std::size_t>(cell_number_);
        if (selected < all_indices.size()) {
          index_sequence_.push_back(all_indices[selected]);
        }
        return;
      }

      std::size_t const begin =
        static_cast<std::size_t>(std::max(starting_cell_, 0));
      if (begin >= all_indices.size()) {
        return;
      }

      std::size_t end = all_indices.size();
      if (cells_to_read_ > 0) {
        std::size_t const requested = static_cast<std::size_t>(cells_to_read_);
        if (requested < (end - begin)) {
          end = begin + requested;
        }
      }

      for (std::size_t i = begin; i < end; ++i) {
        if (skip_every_ > 0 && (i % static_cast<std::size_t>(skip_every_) == 0)) {
          continue;
        }
        index_sequence_.push_back(all_indices[i]);
      }
    }

    // Helper to create type_id from FORM product type string
    phlex::experimental::type_id make_type_id_for_form_type(std::string const& type_str)
    {
      auto const* entry = form::experimental::find_form_product_type(type_str);
      if (entry == nullptr) {
        throw std::runtime_error("Unsupported FORM product type: " + type_str);
      }
      return entry->type_id;
    }

    // Type-erased read: dispatches on productType string at runtime
    // Returns product_ptr (type-erased container with runtime type info)
    phlex::experimental::product_ptr read_product_from_form(
      std::string const& creator,
      std::string const& product_name,
      std::string const& index_str,
      std::string const& product_type)
    {
      auto const* entry = form::experimental::find_form_product_type(product_type);
      if (entry == nullptr) {
        throw std::runtime_error("Unsupported FORM product type: " + product_type);
      }
      return entry->reader_fn(*reader_, creator, product_name, index_str, product_type);
    }

    std::optional<phlex::experimental::provider_bundle> create_provider_bundle_for_type(
      std::string const& actual_creator,
      std::string const& product_name,
      std::string const& product_type,
      phlex::experimental::product_specification const& spec)
    {
      // Capture only what's needed for the provider function
      // The actual type dispatch happens at call time in read_product_from_form
      auto provider_func = [this, actual_creator, product_name, product_type](
                             phlex::data_cell_index const& id) -> phlex::experimental::product_ptr {
        return read_product_from_form(actual_creator, product_name, id.to_string(), product_type);
      };

      return phlex::experimental::provider_bundle{
        .provider_function = std::move(provider_func),
        .max_concurrency = phlex::concurrency::serial,
        .spec = spec,
        .layer = "event",
        .stage = "CURRENT"};
    }
  };

}

PHLEX_REGISTER_SOURCE(s, config)
{
  std::string const input_file = config.get<std::string>("input_file");
  auto const products = config.get<std::vector<std::string>>("products");
  std::string const tech_string = config.get<std::string>("technology", "ROOT_TTREE");
  std::string const advertised_creator =
    config.get<std::string>("advertised_creator", config.get<std::string>("module_label", ""));
  int const starting_cell = config.get<int>("starting_cell", 0);
  int const cells_to_read = config.get<int>("cells_to_read", -1);
  int const skip_every = config.get<int>("skip_every", 0);
  int const cell_number = config.get<int>("cell_number", -1);

  std::unordered_map<std::string_view, int> const tech_lookup = {
    {"ROOT_TTREE", form::technology::ROOT_TTREE},
    {"ROOT_RNTUPLE", form::technology::ROOT_RNTUPLE},
    {"HDF5", form::technology::HDF5}};

  auto it = tech_lookup.find(tech_string);
  if (it == tech_lookup.end()) {
    throw std::runtime_error("Unknown technology: " + tech_string);
  }

  int const technology = it->second;

  s.source<FormInputSource>(config.get<std::string>("module_label"),
                             input_file,
                             products,
                             advertised_creator,
                             technology,
                             starting_cell,
                             cells_to_read,
                             skip_every,
                             cell_number);
}
