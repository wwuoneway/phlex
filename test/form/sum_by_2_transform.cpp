#include "phlex/core/product_selector.hpp"
#include "phlex/module.hpp"

using namespace phlex;

namespace {

  float sum_multiply_by_2(std::vector<int> const& sum)
  {
    if (sum.empty()) {
      return 0.0F;
    }
    return static_cast<float>(sum.front()) * 2.0F + 2.0F;
  }

}

PHLEX_REGISTER_ALGORITHMS(m, config)
{
  auto const layer = config.get<std::string>("layer");
  auto const creator = config.get<std::string>("creator");
  auto const output_suffix = config.get<std::string>("output_suffix");

  m.transform(
     "multiply_sum_by_2",
      [](std::vector<int> const& input) -> float { return sum_multiply_by_2(input); },
     concurrency::unlimited)
    .input_family(product_selector{.creator = creator, .layer = layer})
    .output_product_suffixes(output_suffix);
}
