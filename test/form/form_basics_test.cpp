#include "core/technology.hpp"
#include "core/token.hpp"
#include "core/token_registry.hpp"
#include "form/config.hpp"
#include "form/form_reader.hpp"
#include "form/form_source_type_registry.hpp"
#include "form/form_writer.hpp"
#include "form/product_with_name.hpp"
#include "persistence/ipersistence_writer.hpp"
#include "persistence/persistence_reader.hpp"
#include "persistence/persistence_writer.hpp"
#include "storage/factories.hpp"
#include "storage/istorage.hpp"
#include "storage/storage_associative_write_container.hpp"
#include "storage/storage_file.hpp"
#include "storage/storage_read_container.hpp"
#include "storage/storage_write_association.hpp"
#include "storage/storage_write_container.hpp"
#ifdef USE_ROOT_STORAGE
#include "root_storage/root_tbranch_read_container.hpp"
#include "root_storage/root_tbranch_write_container.hpp"
#include "root_storage/root_ttree_write_container.hpp"
#endif
#ifdef USE_RNTUPLE_STORAGE
#include "root_storage/root_rfield_read_container.hpp"
#include "root_storage/root_rfield_write_container.hpp"
#include "root_storage/root_rntuple_write_container.hpp"
#endif
#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

using namespace form::detail::experimental;

TEST_CASE("token default constructor", "[form]")
{
  token t;
  CHECK(t.file_name().empty());
  CHECK(t.container_name().empty());
  CHECK(t.technology() == form::technology::id{});
  // Default-constructed token has no id set
  CHECK_FALSE(t.has_id());
}

TEST_CASE("token basics", "[form]")
{
  token t("file.root", "container", form::technology::root_ttree, 42);
  CHECK(t.file_name() == "file.root");
  CHECK(t.container_name() == "container");
  CHECK(t.technology() == form::technology::root_ttree);
  CHECK(t.has_id());
  CHECK(t.id() == 42u);
}

TEST_CASE("technology::id string conversions", "[form]")
{
  using namespace form::technology;

  // Round-trip the implemented backends through from_string / to_string
  CHECK(from_string("ROOT_TTREE") == root_ttree);
  CHECK(from_string("ROOT_RNTUPLE") == root_rntuple);

  CHECK(to_string(root_ttree) == "ROOT_TTREE");
  CHECK(to_string(root_rntuple) == "ROOT_RNTUPLE");
  CHECK(to_string(hdf5) == "HDF5"); // reserved: still names itself for diagnostics

  // HDF5 is reserved but unimplemented: reject it at parse time rather than
  // silently falling back to a different storage.
  CHECK_THROWS_AS(from_string("HDF5"), std::runtime_error);

  // An unknown name throws; an unknown id stringifies to the sentinel
  CHECK_THROWS_AS(from_string("NOT_A_TECH"), std::runtime_error);
  CHECK(to_string(id{}) == "UNKNOWN");
}

TEST_CASE("technology::id members and ordering", "[form]")
{
  using namespace form::technology;

  // (major, minor) decomposition
  CHECK(root_ttree.major == major::root);
  CHECK(root_ttree.minor == 1);
  CHECK(root_rntuple.major == major::root);
  CHECK(root_rntuple.minor == 2);
  CHECK(hdf5.major == major::hdf5);
  CHECK(id{}.major == major::generic);

  // operator<=> compares BOTH parts: same major, different minor stay distinct
  CHECK(root_ttree != root_rntuple);
  CHECK(root_ttree < root_rntuple);
  CHECK(id{} == id{major::generic, 0});
}

TEST_CASE("storage_file basics", "[form]")
{
  storage_file f("test.root", 'o');
  CHECK(f.name() == "test.root");
  CHECK(f.mode() == 'o');
  CHECK_THROWS_AS(f.set_attribute("key", "value"), std::runtime_error);
}

TEST_CASE("storage_read_container basics", "[form]")
{
  storage_read_container c("my_container");
  CHECK(c.name() == "my_container");

  auto f = std::make_shared<storage_file>("test.root", 'o');
  c.set_file(f);

  void const* data = nullptr;
  CHECK_FALSE(c.read(1, &data, typeid(int)));
  c.prime(typeid(int));
  CHECK(c.entries() == 0);

  CHECK_THROWS_AS(c.set_attribute("key", "value"), std::runtime_error);

  SECTION("With slash")
  {
    storage_read_container c("parent/child");
    CHECK(c.top_name() == "parent");
    CHECK(c.col_name() == "child");
  }
  SECTION("Without slash")
  {
    storage_read_container c("no_slash");
    CHECK(c.top_name() == "no_slash");
    CHECK(c.col_name() == "Main");
  }
}

TEST_CASE("storage_write_container basics", "[form]")
{
  storage_write_container c("my_container");
  CHECK(c.name() == "my_container");

  auto f = std::make_shared<storage_file>("test.root", 'o');
  c.set_file(f);

  c.setup_write(typeid(int));
  int value = 0;
  c.fill(&value);
  c.commit();

  CHECK_THROWS_AS(c.set_attribute("key", "value"), std::runtime_error);
}

TEST_CASE("storage_write_association basics", "[form]")
{
  storage_write_association a("my_assoc/extra");
  CHECK(a.name() == "my_assoc"); // maybe_remove_suffix should remove /extra

  a.set_attribute("key",
                  "value"); // storage_write_association overrides set_attribute to do nothing
}

TEST_CASE("storage_associative_write_container basics", "[form]")
{
  SECTION("With slash")
  {
    storage_associative_write_container c("parent/child");
    CHECK(c.top_name() == "parent");
    CHECK(c.col_name() == "child");
  }
  SECTION("Without slash")
  {
    storage_associative_write_container c("no_slash");
    CHECK(c.top_name() == "no_slash");
    CHECK(c.col_name() == "Main");
  }

  storage_associative_write_container c("p/c");
  auto parent = std::make_shared<storage_write_container>("p");
  c.set_parent(parent);
}

TEST_CASE("Factories fallback", "[form]")
{
  auto f = create_file(form::technology::id{}, "test.root", 'o');
  CHECK(dynamic_cast<storage_file*>(f.get()) != nullptr);

  auto rc = create_read_container(form::technology::id{}, "cont");
  CHECK(dynamic_cast<storage_read_container*>(rc.get()) != nullptr);

  auto wa = create_write_association(form::technology::id{}, "assoc");
  CHECK(dynamic_cast<storage_write_association*>(wa.get()) != nullptr);

  auto wc = create_write_container(form::technology::id{}, "cont");
  CHECK(dynamic_cast<storage_write_container*>(wc.get()) != nullptr);

  // HDF5 is reserved but unimplemented: every factory must fail loudly on the
  // hdf5 dispatch branch rather than silently return generic storage.
  CHECK_THROWS_AS(create_file(form::technology::hdf5, "test.h5", 'o'), std::runtime_error);
  CHECK_THROWS_AS(create_read_container(form::technology::hdf5, "cont"), std::runtime_error);
  CHECK_THROWS_AS(create_write_association(form::technology::hdf5, "assoc"), std::runtime_error);
  CHECK_THROWS_AS(create_write_container(form::technology::hdf5, "cont"), std::runtime_error);

  // A major FORM doesn't recognize at all must also fail loudly
  // major has a fixed underlying type, so an out-of-range value is legal at runtime
  auto const unknown_major =
    // NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange)
    form::technology::id{.major = static_cast<form::technology::major>(99), .minor = 0};
  CHECK_THROWS_AS(create_file(unknown_major, "test.dat", 'o'), std::runtime_error);
  CHECK_THROWS_AS(create_read_container(unknown_major, "cont"), std::runtime_error);
  CHECK_THROWS_AS(create_write_association(unknown_major, "assoc"), std::runtime_error);
  CHECK_THROWS_AS(create_write_container(unknown_major, "cont"), std::runtime_error);
}

TEST_CASE("Factories ROOT storage dispatch", "[form]")
{
#ifdef USE_ROOT_STORAGE
  auto rc_ttree = create_read_container(form::technology::root_ttree, "cont");
  CHECK(dynamic_cast<root_tbranch_read_container_imp*>(rc_ttree.get()) != nullptr);

  auto wa_ttree = create_write_association(form::technology::root_ttree, "assoc");
  CHECK(dynamic_cast<root_ttree_write_container_imp*>(wa_ttree.get()) != nullptr);

  auto wc_ttree = create_write_container(form::technology::root_ttree, "cont");
  CHECK(dynamic_cast<root_tbranch_write_container_imp*>(wc_ttree.get()) != nullptr);

  auto const unsupported_root =
    form::technology::id{.major = form::technology::major::root, .minor = 99};
  CHECK_THROWS_AS(create_read_container(unsupported_root, "cont"), std::runtime_error);
  CHECK_THROWS_AS(create_write_association(unsupported_root, "assoc"), std::runtime_error);
  CHECK_THROWS_AS(create_write_container(unsupported_root, "cont"), std::runtime_error);
#else
  CHECK_THROWS_AS(create_read_container(form::technology::root_ttree, "cont"), std::runtime_error);
  CHECK_THROWS_AS(create_write_association(form::technology::root_ttree, "assoc"),
                  std::runtime_error);
  CHECK_THROWS_AS(create_write_container(form::technology::root_ttree, "cont"), std::runtime_error);
#endif
}

TEST_CASE("Factories RNTuple storage dispatch", "[form]")
{
#ifdef USE_RNTUPLE_STORAGE
  auto rc_rntuple = create_read_container(form::technology::root_rntuple, "cont");
  CHECK(dynamic_cast<root_rfield_read_container_imp*>(rc_rntuple.get()) != nullptr);

  auto wa_rntuple = create_write_association(form::technology::root_rntuple, "assoc");
  CHECK(dynamic_cast<root_rntuple_write_container_imp*>(wa_rntuple.get()) != nullptr);

  auto wc_rntuple = create_write_container(form::technology::root_rntuple, "cont");
  CHECK(dynamic_cast<root_rfield_write_container_imp*>(wc_rntuple.get()) != nullptr);
#else
  CHECK_THROWS_AS(create_read_container(form::technology::root_rntuple, "cont"),
                  std::runtime_error);
  CHECK_THROWS_AS(create_write_association(form::technology::root_rntuple, "assoc"),
                  std::runtime_error);
  CHECK_THROWS_AS(create_write_container(form::technology::root_rntuple, "cont"),
                  std::runtime_error);
#endif
}

TEST_CASE("storage_reader basic operations", "[form]")
{
  auto storage = create_storage_reader();
  REQUIRE(storage != nullptr);

  form::experimental::config::tech_setting_config settings;

  token product_token("file.root", "cont", form::technology::id{}, 1);
  void const* read_data = nullptr;
  storage->read_container(product_token, &read_data, typeid(int), settings);

  int index = storage->get_index(product_token, "some_id", settings);
  CHECK(index == 0);
}

TEST_CASE("storage_writer basic operations", "[form]")
{
  auto storage = create_storage_writer();
  REQUIRE(storage != nullptr);

  form::experimental::config::tech_setting_config settings;

  std::map<std::unique_ptr<placement>, std::type_info const*> containers;
  auto p = std::make_unique<placement>("file.root", "cont", form::technology::id{});
  containers.emplace(std::move(p), &typeid(int));

  storage->create_containers(containers, settings);

  placement p2("file.root", "cont", form::technology::id{});
  int data = 42;
  storage->fill_container(p2, &data, typeid(int));
  storage->commit_containers(p2);
}

TEST_CASE("persistence_reader basic operations", "[form]")
{
  auto p = create_persistence_reader();
  REQUIRE(p != nullptr);

  using namespace form::experimental::config;
  item_config out_cfg;
  out_cfg.add_item("prod", "file.root", form::technology::id{});
  out_cfg.add_item("parent/child", "file.root", form::technology::id{});
  p->configure(out_cfg);

  tech_setting_config tech_cfg;
  p->configure_tech_settings(tech_cfg);

  SECTION("Full Lifecycle")
  {
    void const* data = nullptr;
    // This will call get_token -> get_index (returns 0 for Storage_Container) -> read_container
    CHECK_NOTHROW(p->read("my_creator", "prod", "event_1", &data, typeid(int)));
  }
}

TEST_CASE("persistence_writer basic operations", "[form]")
{
  auto p = create_persistence_writer();
  REQUIRE(p != nullptr);

  using namespace form::experimental::config;
  item_config out_cfg;
  out_cfg.add_item("prod", "file.root", form::technology::id{});
  out_cfg.add_item("parent/child", "file.root", form::technology::id{});
  p->configure(out_cfg);

  tech_setting_config tech_cfg;
  p->configure_tech_settings(tech_cfg);

  SECTION("Errors")
  {
    int val = 42;
    CHECK_THROWS_AS(p->register_write("my_creator", "unknown", &val, typeid(int)),
                    std::runtime_error);
  }
}

TEST_CASE("form::experimental::config tests", "[form]")
{
  using namespace form::experimental::config;

  SECTION("item_config")
  {
    item_config cfg;
    cfg.add_item("prod1", "file1.root", form::technology::root_ttree);

    auto item = cfg.find_item("prod1");
    REQUIRE(item);
    // NOLINTNEXTLINE(bugprone-unchecked-optional-access) -- `REQUIRE` protects against incorrect access
    CHECK(item->product_name == "prod1");

    CHECK_FALSE(cfg.find_item("nonexistent").has_value());
  }

  SECTION("tech_setting_config")
  {
    tech_setting_config cfg;
    cfg.file_settings[form::technology::root_ttree]["file1.root"] = {{"attr", "val"}};
    cfg.container_settings[form::technology::root_ttree]["cont1"] = {{"cattr", "cval"}};

    auto ftable = cfg.get_file_table(form::technology::root_ttree, "file1.root");
    REQUIRE(ftable.size() == 1);
    CHECK(ftable[0].first == "attr");
    CHECK(ftable[0].second == "val");

    auto ctable = cfg.get_container_table(form::technology::root_ttree, "cont1");
    REQUIRE(ctable.size() == 1);
    CHECK(ctable[0].first == "cattr");
    CHECK(ctable[0].second == "cval");
  }
}

TEST_CASE("FORM source registry prefers exact type matches", "[form]")
{
  struct local_product {
    int value{};
  };

  constexpr char const* local_name = "std::vector<local_product>";

  form::experimental::register_form_vector_product_type<local_product>(local_name);

  auto const local_type = phlex::detail::make_type_id<std::vector<local_product>>();
  auto const* resolved_name = form::experimental::find_form_product_type_name(local_type);

  REQUIRE(resolved_name != nullptr);
  CHECK(*resolved_name == local_name);

  auto const* entry = form::experimental::find_form_product_type(*resolved_name);
  REQUIRE(entry != nullptr);
  REQUIRE(entry->cpp_type != nullptr);
  CHECK(*entry->cpp_type == typeid(std::vector<local_product>));
}

TEST_CASE("FORM source registry keeps builtin mappings", "[form]")
{
  auto const bool_type = phlex::detail::make_type_id<std::vector<bool>>();
  auto const* resolved_name = form::experimental::find_form_product_type_name(bool_type);

  REQUIRE(resolved_name != nullptr);
  CHECK(*resolved_name == "std::vector<bool>");
}

TEST_CASE("persistence_reader: throws for missing product in config", "[form]")
{
  using namespace form::experimental::config;

  auto reader = form::detail::experimental::create_persistence_reader();
  REQUIRE(reader != nullptr);
  reader->configure(item_config{});
  reader->configure_tech_settings(tech_setting_config{});

  CHECK_THROWS_AS(reader->prime("creator", "nonexistent", typeid(int)), std::runtime_error);
  CHECK_THROWS_AS(reader->list_indices("creator", "nonexistent"), std::runtime_error);
}

TEST_CASE("form_reader_interface::indices exercises persistence list_indices path", "[form]")
{
  using namespace form::experimental::config;

  item_config cfg;
  cfg.add_item("prod", "dummy_reader_test.root", form::technology::id{});
  form::experimental::form_reader_interface reader{cfg, tech_setting_config{}};

  // indices() calls persistence list_indices; with tech=0 the index container is
  // always empty, so it throws -- but the call itself covers form_reader.cpp L48.
  CHECK_THROWS_AS(reader.indices("creator", "prod"), std::runtime_error);
}

TEST_CASE("form_reader_interface::read throws for missing product config", "[form]")
{
  using namespace form::experimental::config;

  item_config cfg;
  cfg.add_item("prod", "dummy_reader_test.root", form::technology::id{});
  form::experimental::form_reader_interface reader{cfg, tech_setting_config{}};

  form::experimental::product_with_name product{
    .label = "missing", .data = nullptr, .type = &typeid(int)};
  CHECK_THROWS_AS(reader.read("creator", "segment", product), std::runtime_error);
}

TEST_CASE("form_writer_interface handles missing product config without crashing", "[form]")
{
  using namespace form::experimental::config;

  item_config cfg;
  cfg.add_item("prod", "dummy_writer_test.root", form::technology::id{});
  form::experimental::form_writer_interface writer{cfg, tech_setting_config{}};

  form::experimental::product_with_name product{
    .label = "missing", .data = nullptr, .type = &typeid(int)};
  CHECK_NOTHROW(writer.write("creator", "segment", product));
}

TEST_CASE("form_source_type_registry product_from_data_fn throws on null data", "[form]")
{
  using namespace form::experimental;

  ensure_builtin_form_product_types_registered();
  auto const* entry = find_form_product_type("std::vector<int>");
  REQUIRE(entry != nullptr);
  REQUIRE(entry->product_from_data_fn != nullptr);

  CHECK_THROWS_AS(entry->product_from_data_fn(nullptr, "prod", "[]"), std::runtime_error);
}

TEST_CASE("FORM source registry: unregistered type returns nullptr", "[form]")
{
  // find_form_product_type_name returns nullptr for a type never registered.
  // Exercises the null-return path form_source::create_providers checks at L76-77.
  struct never_registered {};
  auto const unknown_type = phlex::detail::make_type_id<never_registered>();
  CHECK(form::experimental::find_form_product_type_name(unknown_type) == nullptr);
}

TEST_CASE("FORM source registry: unknown name returns nullptr entry", "[form]")
{
  // find_form_product_type returns nullptr for an unregistered name.
  // Exercises the null-entry path form_source::create_providers checks at L80-82.
  CHECK(form::experimental::find_form_product_type("__nonexistent_product_type__") == nullptr);
}

TEST_CASE("FORM source registry: registration error paths", "[form]")
{
  using phlex::detail::make_type_id;

  SECTION("empty product type name throws")
  {
    CHECK_THROWS_AS(
      form::experimental::register_form_product_type(
        "",
        make_type_id<int>(),
        typeid(int),
        [](void const*, std::string const&, std::string const&) -> phlex::detail::product_ptr {
          return nullptr;
        }),
      std::runtime_error);
  }

  SECTION("null conversion function throws")
  {
    CHECK_THROWS_AS(form::experimental::register_form_product_type(
                      "some_new_type_for_error_test",
                      make_type_id<double>(),
                      typeid(double),
                      form::experimental::form_source_product_from_data_fn{}),
                    std::runtime_error);
  }
}

// ---------------------------------------------------------------------------
// parse config once, create containers once, collect tokens for navigation
// ---------------------------------------------------------------------------

namespace {
  // A spy backend: records how many times containers were created and hands out an increasing
  // 0-based row per container so register_write produces real, locatable tokens.
  class fake_storage_writer : public i_storage_writer {
  public:
    void create_containers(
      std::map<std::unique_ptr<placement>, std::type_info const*> const& containers,
      form::experimental::config::tech_setting_config const& /*settings*/) override
    {
      ++create_calls;
      for (auto const& [plcmnt, type] : containers) {
        created.push_back(plcmnt->container_name());
      }
    }

    std::uint64_t fill_container(placement const& plcmnt,
                                 void const* /*data*/,
                                 std::type_info const& /*type*/) override
    {
      return next_row[plcmnt.container_name()]++;
    }

    void commit_containers(placement const& /*plcmnt*/) override { ++commit_calls; }

    int create_calls = 0;
    int commit_calls = 0;
    std::vector<std::string> created;
    std::map<std::string, std::uint64_t> next_row;
  };

  // A spy persistence writer: returns a token per product with an increasing row per label, so a
  // form_writer_interface test can verify token collection without a real storage backend.
  class fake_persistence_writer : public i_persistence_writer {
  public:
    void configure_tech_settings(
      form::experimental::config::tech_setting_config const& /*settings*/) override
    {
    }
    void configure(form::experimental::config::item_config const& /*config_items*/) override {}
    void create_containers(
      std::string const& /*creator*/,
      std::map<std::string, std::type_info const*> const& /*products*/) override
    {
      ++create_calls;
    }
    token register_write(std::string const& creator,
                         std::string const& label,
                         void const* /*data*/,
                         std::type_info const& /*type*/) override
    {
      std::uint64_t const row = next_row[label]++;
      return token{"out.root", creator + "/" + label, form::technology::root_ttree, row};
    }
    void commit_output(std::string const& /*creator*/, std::string const& /*id*/) override
    {
      ++commit_calls;
    }

    int create_calls = 0;
    int commit_calls = 0;
    std::map<std::string, std::uint64_t> next_row;
  };
}

TEST_CASE("token_registry collects, finds, overwrites, and iterates", "[form]")
{
  token_registry reg;
  CHECK(reg.empty());
  CHECK(reg.size() == 0);

  token t1("out.root", "creatorX/prodA", form::technology::root_ttree, 3);
  reg.add("[event:0]", "creatorX", "prodA", t1);
  CHECK_FALSE(reg.empty());
  CHECK(reg.size() == 1);

  auto const found = reg.find("[event:0]", "creatorX", "prodA");
  REQUIRE(found.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access) -- REQUIRE guards the access
  CHECK(found->container_name() == "creatorX/prodA");
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  CHECK(found->id() == 3u);

  // A key that was never added is absent.
  CHECK_FALSE(reg.find("[event:1]", "creatorX", "prodA").has_value());

  // Re-adding the same (id, creator, label) overwrites in place.
  token t2("out.root", "creatorX/prodA", form::technology::root_ttree, 9);
  reg.add("[event:0]", "creatorX", "prodA", t2);
  CHECK(reg.size() == 1);
  auto const updated = reg.find("[event:0]", "creatorX", "prodA");
  REQUIRE(updated.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  CHECK(updated->id() == 9u);

  // Const iteration visits every collected token.
  reg.add("[event:1]", "creatorX", "prodA", t1);
  std::size_t count = 0;
  for (auto const& entry : reg) {
    (void)entry;
    ++count;
  }
  CHECK(count == reg.size());
  CHECK(reg.size() == 2);
}

TEST_CASE("persistence_writer: config parsed once, containers created once, tokens carry rows",
          "[form]")
{
  using namespace form::experimental::config;

  auto fake = std::make_unique<fake_storage_writer>();
  auto* spy = fake.get();
  persistence_writer writer{std::move(fake)};

  item_config cfg;
  cfg.add_item("prodA", "out.root", form::technology::root_ttree);
  cfg.add_item("prodB", "out.root", form::technology::root_ttree);
  writer.configure(cfg);
  writer.configure_tech_settings(tech_setting_config{});

  std::map<std::string, std::type_info const*> products = {{"prodA", &typeid(int)},
                                                           {"prodB", &typeid(int)}};
  int val = 7;

  // First event: the two products plus the navigation ("index") container are created exactly once.
  writer.create_containers("creatorX", products);
  CHECK(spy->create_calls == 1);
  CHECK(spy->created.size() == 3);

  auto const tok_a = writer.register_write("creatorX", "prodA", &val, typeid(int));
  auto const tok_b = writer.register_write("creatorX", "prodB", &val, typeid(int));

  // Config resolved once: each product routes to its configured file, technology, and full label.
  CHECK(tok_a.file_name() == "out.root");
  CHECK(tok_a.container_name() == "creatorX/prodA");
  CHECK(tok_a.technology() == form::technology::root_ttree);
  CHECK(tok_a.id() == 0u);
  CHECK(tok_b.container_name() == "creatorX/prodB");
  CHECK(tok_b.id() == 0u);

  writer.commit_output("creatorX", "[event:0]");
  CHECK(spy->commit_calls == 1);

  // Second event: everything already exists, so create_containers makes no new storage call.
  writer.create_containers("creatorX", products);
  CHECK(spy->create_calls == 1);

  // Rows keep advancing per container across events.
  auto const tok_a2 = writer.register_write("creatorX", "prodA", &val, typeid(int));
  CHECK(tok_a2.id() == 1u);
}

TEST_CASE("persistence_writer: unknown product has no resolved config", "[form]")
{
  using namespace form::experimental::config;

  auto fake = std::make_unique<fake_storage_writer>();
  persistence_writer writer{std::move(fake)};

  item_config cfg;
  cfg.add_item("prodA", "out.root", form::technology::root_ttree);
  writer.configure(cfg);
  writer.configure_tech_settings(tech_setting_config{});

  int val = 0;
  CHECK_THROWS_AS(writer.register_write("creatorX", "unknown", &val, typeid(int)),
                  std::runtime_error);
}

TEST_CASE("form_writer_interface collects a token per written product", "[form]")
{
  using namespace form::experimental::config;

  item_config cfg;
  cfg.add_item("prodA", "out.root", form::technology::root_ttree);
  cfg.add_item("prodB", "out.root", form::technology::root_ttree);

  auto fake = std::make_unique<fake_persistence_writer>();
  auto* spy = fake.get();
  form::experimental::form_writer_interface writer{cfg, tech_setting_config{}, std::move(fake)};

  int v = 1;
  std::vector<form::experimental::product_with_name> products = {
    {.label = "prodA", .data = &v, .type = &typeid(int)},
    {.label = "prodB", .data = &v, .type = &typeid(int)}};

  writer.write("creatorX", "[event:0]", products);
  writer.write("creatorX", "[event:1]", products);

  auto const& tokens = writer.tokens();
  CHECK(tokens.size() == 4); // 2 products x 2 events

  auto const a0 = tokens.find("[event:0]", "creatorX", "prodA");
  REQUIRE(a0.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  CHECK(a0->container_name() == "creatorX/prodA");
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  CHECK(a0->id() == 0u);

  auto const a1 = tokens.find("[event:1]", "creatorX", "prodA");
  REQUIRE(a1.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access)
  CHECK(a1->id() == 1u); // second event advances the row

  CHECK(spy->create_calls == 2);
  CHECK(spy->commit_calls == 2);
}
