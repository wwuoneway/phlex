// Copyright (C) 2025 ...

#ifndef FORM_CORE_TOKEN_REGISTRY_HPP
#define FORM_CORE_TOKEN_REGISTRY_HPP

#include "core/token.hpp"

#include <cstddef>
#include <map>
#include <optional>
#include <string>
#include <tuple>

/* @class token_registry
 * @brief Collects the tokens returned by register_write, keyed by logical id + creator + product
 *        label. This is the write-side seam for fine-grained navigation: a later navigation writer
 *        will read it to persist a logical_id -> token(file, container, row) map. In-memory only;
 *        nothing here is written to disk yet.
 */
namespace form::detail::experimental {

  class token_registry {
  public:
    using key_type = std::tuple<std::string, std::string, std::string>; // (id, creator, label)
    using map_type = std::map<key_type, token>;

    /// Record the token locating one written product. A repeated (id, creator, label) overwrites.
    void add(std::string const& id,
             std::string const& creator,
             std::string const& label,
             token tok);

    /// Look up the token for one (id, creator, label), if present.
    std::optional<token> find(std::string const& id,
                              std::string const& creator,
                              std::string const& label) const;

    /// Number of tokens collected.
    std::size_t size() const;
    /// Whether no tokens have been collected.
    bool empty() const;

    /// Const iteration over (key, token) pairs, for the future navigation-writer drain.
    map_type::const_iterator begin() const;
    map_type::const_iterator end() const;

  private:
    map_type tokens_;
  };

} // namespace form::detail::experimental

#endif // FORM_CORE_TOKEN_REGISTRY_HPP
