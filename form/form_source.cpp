#include "phlex/source.hpp"

#include "form/config.hpp"
#include "form/form_reader.hpp"
#include "form/technology.hpp"
#include "form/storage/storage_reader.hpp"

#include <cassert>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <vector>

namespace {

  class FormInputSource {
  public:
    FormInputSource(form::experimental::config::ItemConfig const& input_cfg,
                    form::experimental::config::tech_setting_config const& tech_cfg) :
      reader_(std::make_shared<form::experimental::form_reader_interface>(input_cfg, tech_cfg))
    {
    }

    template <typename T>
    T const& read(std::string const& creator,
                  std::string const& product_name,
                  phlex::data_cell_index const& id)
    {
      std::string const index_str = id.to_string();
      form::experimental::product_with_name pb{product_name, nullptr, &typeid(T)};
      reader_->read(creator, index_str, pb);
      if (!pb.data) {
        throw std::runtime_error("FORM Error: Failed to retrieve product [" + product_name +
                                 "] for " + index_str);
      }
      return *static_cast<T const*>(pb.data);
    }
  
  private:
    std::shared_ptr<form::experimental::form_reader_interface> reader_;
  };

  template <typename T, typename Source>
  void register_form_input_provider(Source& s,
                                    std::shared_ptr<FormInputSource> const& form_input,
                                    std::string const& provider_name,
                                    std::string const& creator,
                                    std::string const& product_name)
  {
    s.provide(provider_name,
              [form_input, creator, product_name](phlex::data_cell_index const& id) -> T {
                return form_input->read<T>(creator, product_name, id);
              })
      .output_product(phlex::experimental::algorithm_name::create(creator),
                      phlex::experimental::identifier(product_name),
                      phlex::experimental::identifier("event"));
  }

  template <typename Source>
  void register_provider_by_metadata(Source& s,
                                     std::shared_ptr<FormInputSource> const& form_input,
                                     std::string const& provider_name,
                                     std::string const& creator,
                                     std::string const& product_name,
                                     std::string const& product_type)
  {
    if (product_type == "std::vector<int>") {
      register_form_input_provider<std::vector<int>>(s, form_input, provider_name, creator, product_name);
    } else if (product_type == "std::vector<unsigned int>") {
      register_form_input_provider<std::vector<unsigned int>>(s, form_input, provider_name, creator, product_name);
    } else if (product_type == "std::vector<long>") {
      register_form_input_provider<std::vector<long>>(s, form_input, provider_name, creator, product_name);
    } else if (product_type == "std::vector<unsigned long>") {
      register_form_input_provider<std::vector<unsigned long>>(s, form_input, provider_name, creator, product_name);
    } else if (product_type == "std::vector<long long>") {
      register_form_input_provider<std::vector<long long>>(s, form_input, provider_name, creator, product_name);
    } else if (product_type == "std::vector<unsigned long long>") {
      register_form_input_provider<std::vector<unsigned long long>>(s, form_input, provider_name, creator, product_name);
    } else if (product_type == "std::vector<float>") {
      register_form_input_provider<std::vector<float>>(s, form_input, provider_name, creator, product_name);
    } else if (product_type == "std::vector<double>") {
      register_form_input_provider<std::vector<double>>(s, form_input, provider_name, creator, product_name);
    } else if (product_type == "std::vector<bool>") {
      register_form_input_provider<std::vector<bool>>(s, form_input, provider_name, creator, product_name);
    } else if (product_type == "std::vector<char>") {
      register_form_input_provider<std::vector<char>>(s, form_input, provider_name, creator, product_name);
    } else if (product_type == "std::vector<std::string>") {
      register_form_input_provider<std::vector<std::string>>(s, form_input, provider_name, creator, product_name);
    } else {
      throw std::runtime_error("Unsupported FORM product type: " + product_type + " for product " + product_name);
    }
  }

  std::optional<std::string> findProductType(
    form::detail::experimental::FileMetadata const& metadata,
    std::string const& product_name)
  {
    for (auto const& entry : metadata.productRegistry) {
      if (entry.productName == product_name) {
        return entry.productType;
      }
    }
    return std::nullopt;
  }

}

PHLEX_REGISTER_PROVIDERS(s, config)
{
  std::cout << "Registering FORM input source...\n";

  // Extract configuration from Phlex config
  std::string const input_file = config.get<std::string>("input_file");
  std::string const creator = config.get<std::string>("creator");
  std::string const tech_string = config.get<std::string>("technology", "ROOT_TTREE");
  auto const products = config.get<std::vector<std::string>>("products");

  std::cout << "Configuration:\n";
  std::cout << "  input_file: " << input_file << "\n";
  std::cout << "  creator: " << creator << "\n";
  std::cout << "  technology: " << tech_string << "\n";

  std::unordered_map<std::string_view, int> const tech_lookup = {
    {"ROOT_TTREE", form::technology::ROOT_TTREE},
    {"ROOT_RNTUPLE", form::technology::ROOT_RNTUPLE},
    {"HDF5", form::technology::HDF5}};

  auto it = tech_lookup.find(tech_string);

  if (it == tech_lookup.end()) {
    throw std::runtime_error("Unknown technology: " + tech_string);
  }

  int const technology = it->second;

  form::detail::experimental::StorageReader reader;
  auto const* metadata = reader.loadFileMetadata(input_file, technology);
  if (metadata == nullptr) {
    throw std::runtime_error("Failed to load FORM file metadata for " + input_file);
  }
  if (!metadata->hasProductRegistry) {
    throw std::runtime_error("FORM input file " + input_file + " does not contain a ProductRegistry");
  }

  // Build input config
  form::experimental::config::ItemConfig input_cfg;
  form::experimental::config::tech_setting_config tech_cfg;
  for (auto const& name : products) {
    input_cfg.addItem(name, input_file, technology);
  }

  auto form_input = std::make_shared<FormInputSource>(input_cfg, tech_cfg);

  for (auto const& name : products) {
    auto const type_name = findProductType(*metadata, name);
    if (!type_name.has_value() || type_name->empty()) {
      throw std::runtime_error("Unable to determine FORM container type for product: " + name);
    }

    register_provider_by_metadata(s, form_input, "provide_" + name, creator, name, *type_name);
  }

  std::cout << "FORM input source registered successfully\n";
}
