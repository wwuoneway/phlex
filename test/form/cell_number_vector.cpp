// Writes each data cell's own layer numbers (outermost first, e.g. {run, event}) so that read-back
// can verify which cell a row belongs to.
//
// The "stamp" transform sleeps for a random time so that stores reach the output out of cell
// order; the "check_cell_numbers" observer fails the job if a served value belongs to another cell.

#include "phlex/core/product_selector.hpp"
#include "phlex/model/data_cell_index.hpp"
#include "phlex/model/handle.hpp"
#include "phlex/model/identifier.hpp"
#include "phlex/module.hpp"
#include "phlex/source.hpp"

#include <chrono>
#include <random>
#include <ranges>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using namespace phlex;

namespace {
  std::vector<int> number_of(data_cell_index const& id)
  {
    std::vector<int> numbers(id.depth());
    auto const* cell = &id;
    for (auto& number : numbers | std::views::reverse) {
      number = static_cast<int>(cell->number());
      cell = cell->parent().get();
    }
    return numbers;
  }

  std::string to_string(std::vector<int> const& numbers)
  {
    std::string result{"{"};
    for (auto const number : numbers) {
      result += (result.size() == 1 ? "" : ", ") + std::to_string(number);
    }
    return result + "}";
  }
}

PHLEX_REGISTER_PROVIDERS(graph, config)
{
  auto const creator = config.get<std::string>("creator");
  graph.provide("cell_numbers", number_of, concurrency::unlimited)
    .output_product(
      creator, experimental::identifier("numbers"), experimental::identifier("event"));
}

PHLEX_REGISTER_ALGORITHMS(m, config)
{
  auto const input_creator = config.get<std::string>("input_creator");
  if (config.get<bool>("check", false)) {
    m.observe(
       "check_cell_numbers",
       [](handle<std::vector<int>> const values) {
         if (*values != number_of(values.data_cell_index())) {
           throw std::runtime_error("cell " + values.data_cell_index().to_string() +
                                    " was served the value " + to_string(*values));
         }
       },
       concurrency::unlimited)
      .input_family(product_selector{.creator = experimental::identifier{input_creator},
                                     .layer = "event",
                                     .suffix = "cell_numbers"});
    return;
  }

  m.transform(
     "stamp",
     [](std::vector<int> const& values) {
       thread_local std::mt19937 gen{std::random_device{}()};
       std::this_thread::sleep_for(
         std::chrono::microseconds(std::uniform_int_distribution<int>(0, 2000)(gen)));
       return values;
     },
     concurrency::unlimited)
    .input_family(product_selector{
      .creator = experimental::identifier{input_creator}, .layer = "event", .suffix = "numbers"})
    .output_product_suffixes("cell_numbers");
}
