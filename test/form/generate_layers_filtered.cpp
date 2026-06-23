#include "phlex/driver.hpp"
#include "plugins/layer_generator.hpp"

#include <cstddef>
#include <string>
#include <stdexcept>

PHLEX_REGISTER_DRIVER(d, config)
{
  using namespace phlex;

  auto gen = std::make_shared<experimental::layer_generator>();

  auto const layers = config.get<configuration>("layers", {});
  int const starting_cell = config.get<int>("starting_cell", 0);
  int const cells_to_read = config.get<int>("cells_to_read", -1);
  int const skip_every = config.get<int>("skip_every", 0);
  int const cell_number = config.get<int>("cell_number", -1);

  if (starting_cell < 0 || cells_to_read < -1 || skip_every < 0 || cell_number < -1) {
    throw std::runtime_error("generate_layers_filtered: invalid cell filtering config values");
  }

  for (auto const& key : layers.keys()) {
    auto const layer_config = layers.get<configuration>(key);
    gen->add_layer(key,
                   {.parent_layer_name = layer_config.get<std::string>("parent", "job"),
                    .total_per_parent_data_cell = layer_config.get<unsigned int>("total"),
                    .starting_value = layer_config.get<unsigned int>("starting_number", 0)});
  }

  return d.driver(gen->hierarchy(),
                  [gen, starting_cell, cells_to_read, skip_every, cell_number](
                    data_cell_yielder const yield) {
    std::size_t event_ordinal = 0;
    std::size_t emitted = 0;
    for (data_cell_index_ptr const& index : gen->indices()) {
      // Always emit the job root index so framework bookkeeping/finalization remains valid.
      if (!index->has_parent()) {
        yield(index);
        continue;
      }

      if (cell_number >= 0) {
        if (event_ordinal == static_cast<std::size_t>(cell_number)) {
          yield(index);
          return;
        }
        ++event_ordinal;
        continue;
      }

      if (event_ordinal < static_cast<std::size_t>(starting_cell)) {
        ++event_ordinal;
        continue;
      }

      if (skip_every > 0 && (event_ordinal % static_cast<std::size_t>(skip_every) == 0)) {
        ++event_ordinal;
        continue;
      }

      if (cells_to_read >= 0 && emitted >= static_cast<std::size_t>(cells_to_read)) {
        return;
      }

      yield(index);
      ++emitted;
      ++event_ordinal;
    }
  });
}