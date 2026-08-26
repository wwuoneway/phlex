// Copyright (C) 2025 ...

#include "token_registry.hpp"

#include <utility>

using namespace form::detail::experimental;

void token_registry::add(std::string const& id,
                         std::string const& creator,
                         std::string const& label,
                         token tok)
{
  tokens_.insert_or_assign(std::make_tuple(id, creator, label), std::move(tok));
}

std::optional<token> token_registry::find(std::string const& id,
                                          std::string const& creator,
                                          std::string const& label) const
{
  auto const it = tokens_.find(std::make_tuple(id, creator, label));
  if (it == tokens_.end()) {
    return std::nullopt;
  }
  return it->second;
}

std::size_t token_registry::size() const { return tokens_.size(); }

bool token_registry::empty() const { return tokens_.empty(); }

token_registry::map_type::const_iterator token_registry::begin() const { return tokens_.begin(); }

token_registry::map_type::const_iterator token_registry::end() const { return tokens_.end(); }
