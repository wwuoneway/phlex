// Dumps a cell_numbers file through FORM's storage read path and checks every row.
//
// For each id in the index container (in row order), get_index() locates its row and
// read_container() reads the payload stored there. The payload must be the cell's own layer
// numbers, outermost first: {N} for [event:N], {N, M} for [run:N, event:M].
//
// usage: form_cell_numbers_dump <file> <technology> <creator> <stage>

#include "core/container_naming.hpp"
#include "core/technology.hpp"
#include "core/token.hpp"
#include "form/config.hpp"
#include "storage/storage_reader.hpp"
#include "test/form/navigation_check.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <format>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

using namespace form::detail::experimental;

namespace {
  std::string to_string(std::vector<int> const& numbers)
  {
    std::string result{"{"};
    for (auto const number : numbers) {
      result += (result.size() == 1 ? "" : ", ") + std::to_string(number);
    }
    return result + "}";
  }
}

int main(int const argc, char* argv[])
{
  if (argc != 5) {
    std::cerr << "usage: form_cell_numbers_dump <file> <technology> <creator> <stage>\n";
    return 1;
  }

  std::string const file_name{argv[1]};
  auto const technology = form::technology::from_string(argv[2]);
  std::string const row_space = build_row_space_name(technology, argv[3], argv[4]);
  form::experimental::config::tech_setting_config const settings{};

  std::size_t mismatches = 0;
  try {
    storage_reader reader;
    token const index{file_name, build_full_label(row_space, "index"), technology};
    auto const ids = reader.list_indices(index, settings);

    std::cout << std::format("{:<24} {:>6}   {}\n", "index", "row", "cell_numbers");
    for (std::size_t position = 0; position != ids.size(); ++position) {
      auto const& id = ids[position];
      auto const row = reader.get_index(index, id, settings);

      void const* raw = nullptr;
      reader.read_container(token{file_name,
                                  build_full_label(row_space, "cell_numbers"),
                                  technology,
                                  static_cast<std::uint64_t>(row)},
                            &raw,
                            typeid(std::vector<int>),
                            settings);
      std::unique_ptr<std::vector<int> const> const value{
        static_cast<std::vector<int> const*>(raw)};

      auto const layers = form::test::layer_values_of(id);
      bool const ok = value && layers && std::cmp_equal(row, position) &&
                      std::ranges::equal(*value, *layers, [](int v, std::uint64_t l) {
                        return std::cmp_equal(v, l);
                      });
      std::cout << std::format(
        "{:<24} {:>6}   {}{}\n", id, row, value ? to_string(*value) : "-", ok ? "" : "  MISMATCH");
      if (!ok) {
        ++mismatches;
      }
    }
    std::cout << std::format("{} cells, {} mismatches\n", ids.size(), mismatches);
  } catch (std::exception const& e) {
    std::cerr << "error: " << e.what() << '\n';
    return 1;
  }
  return mismatches == 0 ? 0 : 1;
}
