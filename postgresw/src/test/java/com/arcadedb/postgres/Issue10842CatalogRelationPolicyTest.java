/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.postgres;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10842: the policy for a catalog relation this server does not model, and the {@code pg_depend} rows of the indexes.
 * <ol>
 * <li>An unmodelled relation is refused by default, not answered with no rows: most of them hold rows in every PostgreSQL.</li>
 * <li>Every relation psql's {@code describe.c} and psqlodbc's {@code info.c} read is either modelled or refused on purpose, with
 * the reason written next to it, the way #10591 holds pgjdbc's {@code PgDatabaseMetaData} to the catalog. Neither source is on the
 * test classpath, so their relations are checked in here: a client release that reads a new relation is added to the list.</li>
 * <li>{@code pg_depend} lists the dependencies {@code index_create} records for each index, on its table's columns or on the
 * unique constraint it backs, instead of no row at all.</li>
 * </ol>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10842CatalogRelationPolicyTest {
  private static final String DATABASE_PATH = "./target/databases/Issue10842CatalogRelationPolicyTest";
  private static final String USER          = "root";

  /** {@code pg_class} and {@code pg_constraint} as relations, the values pg_depend's classid and refclassid hold. */
  private static final int PG_CLASS      = 1259;
  private static final int PG_CONSTRAINT = 2606;
  /** pg_namespace and pg_type, the other catalogs pg_depend names since issue #10961. */
  private static final int PG_NAMESPACE  = 2615;
  private static final int PG_TYPE       = 1247;

  /**
   * The catalog relations psql 17's {@code src/bin/psql/describe.c} reads (REL_17_STABLE), extracted from its SQL text: every
   * {@code pg_*} name it puts in a FROM or JOIN, without its functions ({@code pg_get_*}, {@code *_is_visible}, ...).
   */
  static final List<String> PSQL_DESCRIBE_RELATIONS = List.of("pg_am", "pg_amop", "pg_amproc", "pg_attrdef", "pg_attribute",
      "pg_auth_members", "pg_cast", "pg_class", "pg_collation", "pg_constraint", "pg_conversion", "pg_database",
      "pg_db_role_setting", "pg_default_acl", "pg_depend", "pg_description", "pg_enum", "pg_event_trigger", "pg_extension",
      "pg_foreign_data_wrapper", "pg_foreign_server", "pg_foreign_table", "pg_index", "pg_inherits", "pg_language",
      "pg_largeobject_metadata", "pg_namespace", "pg_opclass", "pg_operator", "pg_opfamily", "pg_parameter_acl", "pg_policy",
      "pg_proc", "pg_publication", "pg_publication_namespace", "pg_publication_rel", "pg_rewrite", "pg_roles", "pg_sequence",
      "pg_settings", "pg_statistic_ext", "pg_subscription", "pg_tablespace", "pg_trigger", "pg_ts_config", "pg_ts_config_map",
      "pg_ts_dict", "pg_ts_parser", "pg_ts_template", "pg_type", "pg_user_mappings");

  /** The catalog relations psqlodbc's {@code info.c} reads (main branch), extracted the same way. */
  static final List<String> PSQLODBC_INFO_RELATIONS = List.of("pg_am", "pg_attrdef", "pg_attribute", "pg_class", "pg_constraint",
      "pg_group", "pg_index", "pg_namespace", "pg_proc", "pg_trigger", "pg_type", "pg_user", "information_schema.column_privileges");

  /**
   * The relations those clients read that this catalog refuses on purpose, each with why. Every one holds rows in a stock
   * PostgreSQL, or describes an object ArcadeDB could hold and does not model yet; answering it with no rows would claim the
   * database has none, which is the #10371 failure. (What a stock PostgreSQL holds no row of, and ArcadeDB has no equivalent of,
   * is modelled with no rows instead: issue #10960.)
   */
  static final Map<String, String> REFUSED_ON_PURPOSE = Map.ofEntries(//
      Map.entry("pg_amop", "the operators of the built-in operator families (psql \\dAo)"),
      Map.entry("pg_amproc", "the support functions of the built-in operator families (psql \\dAp)"),
      Map.entry("pg_cast", "the built-in casts (psql \\dC)"),
      Map.entry("pg_conversion", "the built-in encoding conversions (psql \\dc)"),
      Map.entry("pg_extension", "plpgsql is installed in every PostgreSQL (psql \\dx)"),
      Map.entry("pg_language", "internal, c, sql and plpgsql exist in every PostgreSQL (psql \\dL)"),
      Map.entry("pg_opclass", "the built-in operator classes (psql \\dAc)"),
      Map.entry("pg_operator", "the built-in operators (psql \\do)"),
      Map.entry("pg_opfamily", "the built-in operator families (psql \\dAf)"),
      Map.entry("pg_ts_config", "the built-in text-search configurations (psql \\dF)"),
      Map.entry("pg_ts_config_map", "the mappings of the built-in text-search configurations (psql \\dF+)"),
      Map.entry("pg_ts_dict", "the built-in text-search dictionaries (psql \\dFd)"),
      Map.entry("pg_ts_parser", "the built-in text-search parser (psql \\dFp)"),
      Map.entry("pg_ts_template", "the built-in text-search templates (psql \\dFt)"),
      Map.entry("pg_group", "the predefined roles that cannot log in exist in every PostgreSQL (psqlodbc SQLTablePrivileges)"),
      Map.entry("information_schema.column_privileges", "a row per column privilege the user holds (psqlodbc SQLColumnPrivileges)"));

  /**
   * Issue #11023: the relations that hold rows in EVERY PostgreSQL, a fresh {@code initdb} of PostgreSQL 17, with what the rows
   * are. A fact about PostgreSQL, not about this catalog, so it does not move when the catalog does: the bootstrap data of
   * {@code src/include/catalog/*.dat} and what {@code initdb} creates after it (the plpgsql extension, the system views and their
   * rules, the comments). Answering one of these with no rows claims a PostgreSQL that does not exist, which is the #10371 lie,
   * and {@link #NO_ROWS_ALTHOUGH_POSTGRES_HOLDS_ROWS} is the only way past it.
   */
  static final Map<String, String> HOLDS_ROWS_IN_EVERY_POSTGRES = Map.ofEntries(//
      Map.entry("pg_aggregate", "the built-in aggregates (pg_aggregate.dat)"),
      Map.entry("pg_am", "the built-in access methods: heap, btree, hash, gist, gin, spgist, brin (pg_am.dat)"),
      Map.entry("pg_amop", "the operators of the built-in operator families (pg_amop.dat)"),
      Map.entry("pg_amproc", "the support functions of the built-in operator families (pg_amproc.dat)"),
      Map.entry("pg_attribute", "the columns of the system catalogs and views"),
      Map.entry("pg_auth_members", "the memberships of the predefined roles: pg_monitor in pg_read_all_settings, ..."),
      Map.entry("pg_cast", "the built-in casts (pg_cast.dat)"),
      Map.entry("pg_class", "the system catalogs, their indexes and the system views"),
      Map.entry("pg_collation", "default, C, POSIX, ucs_basic and the collations initdb imports (pg_collation.dat)"),
      Map.entry("pg_constraint", "the CHECK constraints of the information_schema domains (cardinal_number, ...)"),
      Map.entry("pg_conversion", "the built-in encoding conversions (pg_conversion.dat)"),
      Map.entry("pg_database", "postgres, template0 and template1 (pg_database.dat)"),
      Map.entry("pg_depend", "the dependencies of the system views and of the information_schema"),
      Map.entry("pg_description", "the comments on the built-in objects (postgres.description)"),
      Map.entry("pg_extension", "plpgsql, which initdb installs"),
      Map.entry("pg_group", "the predefined roles that cannot log in"),
      Map.entry("pg_index", "the indexes of the system catalogs"),
      Map.entry("pg_language", "internal, c, sql (pg_language.dat) and plpgsql"),
      Map.entry("pg_namespace", "pg_catalog, pg_toast, public (pg_namespace.dat) and information_schema"),
      Map.entry("pg_opclass", "the built-in operator classes (pg_opclass.dat)"),
      Map.entry("pg_operator", "the built-in operators (pg_operator.dat)"),
      Map.entry("pg_opfamily", "the built-in operator families (pg_opfamily.dat)"),
      Map.entry("pg_proc", "the built-in functions (pg_proc.dat)"),
      Map.entry("pg_range", "the built-in range types: int4range, int8range, numrange, tsrange, tstzrange, daterange (pg_range.dat)"),
      Map.entry("pg_rewrite", "the _RETURN rules of the system views"),
      Map.entry("pg_roles", "the bootstrap superuser and the predefined roles"),
      Map.entry("pg_settings", "a row per configuration parameter"),
      Map.entry("pg_shdescription", "the comments on postgres, template0 and template1"),
      Map.entry("pg_tablespace", "pg_default and pg_global (pg_tablespace.dat)"),
      Map.entry("pg_ts_config", "the built-in text-search configurations (pg_ts_config.dat)"),
      Map.entry("pg_ts_config_map", "the mappings of the built-in text-search configurations (pg_ts_config_map.dat)"),
      Map.entry("pg_ts_dict", "the built-in text-search dictionaries (pg_ts_dict.dat)"),
      Map.entry("pg_ts_parser", "the built-in text-search parser (pg_ts_parser.dat)"),
      Map.entry("pg_ts_template", "the built-in text-search templates (pg_ts_template.dat)"),
      Map.entry("pg_type", "the built-in types (pg_type.dat) and the row types of the system catalogs"),
      Map.entry("pg_user", "the bootstrap superuser"));

  /**
   * Issue #11023: the relations answered with no rows although every PostgreSQL holds rows in them, each a decision of its own
   * (issue #10591): the rows PostgreSQL holds describe objects this catalog lists nowhere else, so no row here could name them and
   * no client sees a contradiction. The reason is the production one, {@code PostgresCatalog.noRowRelations()}, and it has to say
   * so. A relation that enters the no-row set is checked against {@link #HOLDS_ROWS_IN_EVERY_POSTGRES}: one that is in it fails
   * {@link #aNoRowRelationHoldsNoRowInAStockPostgresUnlessItIsADecision} until it is added here as well, by a reviewer who read
   * what PostgreSQL holds there.
   */
  static final Set<String> NO_ROWS_ALTHOUGH_POSTGRES_HOLDS_ROWS = Set.of("pg_proc", "pg_range", "pg_auth_members", "pg_shdescription");

  private Database database;

  @BeforeEach
  void createDatabase() {
    final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
    if (factory.exists())
      factory.open().drop();

    database = factory.create();
    // a unique index over two columns, which backs a constraint; a plain index over one column; and an index whose key is a hidden
    // property, which PostgreSQL calls an expression
    database.transaction(() -> {
      final Schema schema = database.getSchema();
      final DocumentType account = schema.createDocumentType("Account");
      account.createProperty("code", Type.STRING);
      account.createProperty("region", Type.STRING);
      account.createProperty("secret", Type.INTEGER);
      account.createProperty("note", Type.STRING);
      schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Account", "code", "region");
      schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Account", "note");
      schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Account", "secret");
      schema.createDocumentType("Plain").createProperty("name", Type.STRING);
    });
    database.command("sql", "ALTER PROPERTY Account.secret HIDDEN true");
  }

  @AfterEach
  void dropDatabase() {
    if (database != null && database.isOpen())
      database.drop();
  }

  // ------------------------------------------------------------------------------------------------- question 1: the default

  /** The decision: a relation that is neither modelled nor known is refused, never answered "none". */
  @Test
  void anUnmodelledRelationIsRefusedByDefault() {
    for (final String relation : List.of("pg_stat_activity", "pg_locks", "pg_extension", "pg_language", "pg_opclass",
        "information_schema.routines", "pg_relation_added_by_a_future_release")) {
      assertThat(PostgresCatalog.resolve("select * from " + relation, database, USER)).as(relation).isSameAs(PostgresCatalog.DECLINED);
      assertThat(PostgresCatalog.refusalOf("select * from " + relation).getMessage()).as(relation).contains("\"" + relation + "\"");
    }
  }

  // ------------------------------------------------------------------------------------------------- question 2: the guards

  @Test
  void everyRelationPsqlDescribeReadsIsModelledOrRefusedOnPurpose() {
    assertModelledOrRefusedOnPurpose("psql describe.c", PSQL_DESCRIBE_RELATIONS);
  }

  @Test
  void everyRelationPsqlodbcReadsIsModelledOrRefusedOnPurpose() {
    assertModelledOrRefusedOnPurpose("psqlodbc info.c", PSQLODBC_INFO_RELATIONS);
  }

  /**
   * A relation the catalog later models leaves the refusal list in the same change, so the list never lies about the catalog. The
   * move is to model it WITH its rows: a move to the no-row set is a different claim, which
   * {@link #aNoRowRelationHoldsNoRowInAStockPostgresUnlessItIsADecision} holds to what PostgreSQL holds (issue #11023).
   */
  @Test
  void noRelationIsBothModelledAndRefusedOnPurpose() {
    final Set<String> both = new TreeSet<>(REFUSED_ON_PURPOSE.keySet());
    both.retainAll(PostgresCatalog.modelledRelations());
    assertThat(both).as("relations both modelled and refused on purpose: model the rows PostgreSQL holds and drop the refusal, "
        + "never answer one with no rows").isEmpty();

    final Set<String> read = new HashSet<>(PSQL_DESCRIBE_RELATIONS);
    read.addAll(PSQLODBC_INFO_RELATIONS);
    assertThat(read).as("an entry of the refusal list no client reads").containsAll(REFUSED_ON_PURPOSE.keySet());
  }

  // ------------------------------------------------------------------------------------------------- the no-row set (#11023)

  /** Every relation answered with no rows says why no row is the truth: the judgement is written where it is made. */
  @Test
  void everyNoRowRelationSaysWhyItHoldsNoRow() {
    assertThat(PostgresCatalog.noRowRelations()).as("the no-row set").isNotEmpty();
    for (final Map.Entry<String, String> entry : PostgresCatalog.noRowRelations().entrySet())
      assertThat(entry.getValue()).as("why %s holds no row", entry.getKey()).isNotBlank().hasSizeGreaterThan(30)
          .doesNotContainIgnoringCase("todo");
  }

  /**
   * The no-row set is open in production; this closes it against a fact that does not move with it. A relation every PostgreSQL
   * holds rows in is answered with none only as a decision named in {@link #NO_ROWS_ALTHOUGH_POSTGRES_HOLDS_ROWS}, so moving
   * {@code pg_amop} from refused to no-row (the #11023 mutation, every other test green) fails here.
   */
  @Test
  void aNoRowRelationHoldsNoRowInAStockPostgresUnlessItIsADecision() {
    final Set<String> holdRowsInPostgres = new TreeSet<>(PostgresCatalog.noRowRelations().keySet());
    holdRowsInPostgres.retainAll(HOLDS_ROWS_IN_EVERY_POSTGRES.keySet());
    assertThat(holdRowsInPostgres)
        .as("relations every PostgreSQL holds rows in, answered with none: refuse them (REFUSED_ON_PURPOSE) or model their rows, "
            + "unless the rows PostgreSQL holds describe objects no other relation of this catalog lists - then say so in "
            + "PostgresCatalog.NEVER_HAVE_ROWS and add the relation to NO_ROWS_ALTHOUGH_POSTGRES_HOLDS_ROWS")
        .containsExactlyInAnyOrderElementsOf(NO_ROWS_ALTHOUGH_POSTGRES_HOLDS_ROWS);
  }

  /** Every relation that holds rows in every PostgreSQL is modelled, or refused: never answered "none" by default. */
  @Test
  void aRelationThatHoldsRowsInEveryPostgresIsModelledOrRefused() {
    for (final String relation : HOLDS_ROWS_IN_EVERY_POSTGRES.keySet()) {
      if (PostgresCatalog.modelledRelations().contains(relation))
        continue; // its rows, or a decision the test above holds it to
      assertThat(PostgresCatalog.resolve("select * from pg_catalog." + relation, database, USER)).as(relation)
          .isSameAs(PostgresCatalog.DECLINED);
      assertThat(PostgresCatalog.resolve("select count(*) from " + relation, database, USER)).as(relation + " count")
          .isSameAs(PostgresCatalog.DECLINED);
    }
  }

  private void assertModelledOrRefusedOnPurpose(final String client, final List<String> relations) {
    final List<String> unaccounted = new ArrayList<>();
    for (final String relation : relations) {
      if (PostgresCatalog.modelledRelations().contains(relation))
        continue;
      if (!REFUSED_ON_PURPOSE.containsKey(relation)) {
        unaccounted.add(relation);
        continue;
      }
      // a relation refused on purpose is refused, with the 0A000 that names it
      assertThat(PostgresCatalog.resolve("select * from pg_catalog." + relation.replace("pg_catalog.", ""), database, USER))
          .as(relation).isSameAs(PostgresCatalog.DECLINED);
    }
    assertThat(unaccounted).as("relations %s reads that are neither modelled nor refused on purpose", client).isEmpty();
  }

  // ------------------------------------------------------------------------------------------------- question 3: pg_depend

  /** An index that backs no constraint depends on each column of its key, automatically. */
  @Test
  void aPlainIndexDependsOnTheColumnOfItsKey() {
    final int table = oidOfClass("Account");
    final int index = oidOfClass("Account[note]");
    final int note = attnum(table, "note");

    final List<Map<String, Object>> rows = resolve(
        "select classid, objsubid, refclassid, refobjid, refobjsubid, deptype from pg_depend where objid = " + index).rows;
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst()).containsEntry("classid", PG_CLASS).containsEntry("objsubid", 0).containsEntry("refclassid", PG_CLASS)
        .containsEntry("refobjid", table).containsEntry("refobjsubid", note).containsEntry("deptype", "a");
  }

  /** An index that backs a unique constraint depends on that constraint internally, and the constraint on each key column. */
  @Test
  void aConstraintIndexDependsOnItsConstraintAndTheConstraintOnTheColumns() {
    final int table = oidOfClass("Account");
    final int index = oidOfClass("Account[code,region]");
    final int constraint = number(resolve("select oid from pg_constraint where conindid = " + index).rows.getFirst(), "oid");

    final List<Map<String, Object>> ofIndex = resolve(
        "select classid, refclassid, refobjid, refobjsubid, deptype from pg_depend where objid = " + index).rows;
    assertThat(ofIndex).hasSize(1);
    assertThat(ofIndex.getFirst()).containsEntry("classid", PG_CLASS).containsEntry("refclassid", PG_CONSTRAINT)
        .containsEntry("refobjid", constraint).containsEntry("refobjsubid", 0).containsEntry("deptype", "i");

    final List<Map<String, Object>> ofConstraint = resolve(
        "select classid, refclassid, refobjid, refobjsubid, deptype from pg_depend where objid = " + constraint
            + " order by refobjsubid").rows;
    assertThat(ofConstraint).extracting(row -> row.get("refobjsubid")).containsExactly(attnum(table, "code"), attnum(table, "region"));
    for (final Map<String, Object> row : ofConstraint)
      assertThat(row).containsEntry("classid", PG_CONSTRAINT).containsEntry("refclassid", PG_CLASS).containsEntry("refobjid", table)
          .containsEntry("deptype", "a");
  }

  /** A key that is no column of the table is an expression: the index depends on the whole table, refobjsubid 0. */
  @Test
  void anExpressionIndexDependsOnTheWholeTable() {
    final List<Map<String, Object>> rows = resolve(
        "select refobjid, refobjsubid, deptype from pg_depend where objid = " + oidOfClass("Account[secret]")).rows;
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst()).containsEntry("refobjid", oidOfClass("Account")).containsEntry("refobjsubid", 0)
        .containsEntry("deptype", "a");
  }

  /** The walk a tool makes from a table to what is built on it: every index, directly or through its constraint. */
  @Test
  void walkingFromATableReachesEveryIndexOfIt() {
    final int table = oidOfClass("Account");
    final Set<String> reached = new TreeSet<>();
    for (final Map<String, Object> row : resolve("""
        select c.relname from pg_depend d join pg_class c on c.oid = d.objid
        where d.classid = 1259 and d.refclassid = 1259 and d.refobjid = %d and d.deptype = 'a'
        union
        select c.relname from pg_depend d join pg_depend i on i.refobjid = d.objid join pg_class c on c.oid = i.objid
        where d.classid = 2606 and d.refobjid = %d and i.deptype = 'i'""".formatted(table, table)).rows)
      reached.add((String) row.get("relname"));
    assertThat(reached).containsExactly("Account[code,region]", "Account[note]", "Account[secret]");

    // a table with no index has no index depending on it: only its row type does (issue #10961)
    assertThat(resolve("select * from pg_depend where refobjid = " + oidOfClass("Plain") + " and classid = " + PG_CLASS).rows).isEmpty();
  }

  /**
   * psql's own read of pg_depend (describe.c, the column that owns a sequence), which names the catalogs with a {@code regclass}
   * cast: its FROM and WHERE verbatim, answered now that pg_depend has rows to filter. Asked of an index, it names the column the
   * index is on. The projection is psql's without its {@code quote_ident}, which this catalog does not evaluate (and which
   * PostgresCatalogTest uses as its example of a function it cannot read); psql only asks this of a sequence, which ArcadeDB has none of.
   */
  @Test
  void psqlsDependencyQueryWithRegclassCastsIsAnswered() {
    final List<Map<String, Object>> rows = resolve("""
        SELECT nspname, relname, attname, d.deptype
        FROM pg_catalog.pg_class c
        INNER JOIN pg_catalog.pg_depend d ON c.oid=d.refobjid
        INNER JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
        INNER JOIN pg_catalog.pg_attribute a ON (
         a.attrelid=c.oid AND
         a.attnum=d.refobjsubid)
        WHERE d.classid='pg_catalog.pg_class'::pg_catalog.regclass
         AND d.refclassid='pg_catalog.pg_class'::pg_catalog.regclass
         AND d.objid='%d'
         AND d.deptype IN ('a', 'i')""".formatted(oidOfClass("Account[note]"))).rows;
    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst()).containsEntry("relname", "Account").containsEntry("attname", "note").containsEntry("deptype", "a");
  }

  /** The catalogs pg_depend points into are named by a regclass cast in both directions, as PostgreSQL names them. */
  @Test
  void theCatalogsPgDependNamesAreRegclassNames() {
    final Map<String, Object> row = resolve("""
        select 'pg_class'::regclass as c, 'pg_catalog.pg_constraint'::pg_catalog.regclass as k, 'other.pg_class'::regclass as o,
        1259::regclass as cn, 2606::regclass as kn
        from pg_namespace limit 1""").rows.getFirst();
    assertThat(row).containsEntry("c", PG_CLASS).containsEntry("k", PG_CONSTRAINT).containsEntry("cn", "pg_class")
        .containsEntry("kn", "pg_constraint");
    // a schema other than pg_catalog is not the catalog's
    assertThat(row.get("o")).isNull();
  }

  /** Every OID pg_depend names is a row of the catalog it says it is in. */
  @Test
  void everyObjectPgDependNamesExists() {
    final Set<Integer> classes = oids("select oid from pg_class");
    final Set<Integer> constraints = oids("select oid from pg_constraint");
    final Set<Integer> namespaces = oids("select oid from pg_namespace");
    final Set<Integer> types = oids("select oid from pg_type");
    final List<Map<String, Object>> rows = resolve("select * from pg_depend").rows;
    // the constraint index: its constraint, and the constraint's two columns; the plain index's column; the expression index's table;
    // and for each of the two tables its schema and its row type on it (issue #10961)
    assertThat(rows).hasSize(9);
    for (final Map<String, Object> row : rows) {
      assertThat(of(number(row, "classid"), classes, constraints, namespaces, types)).as("objid of %s", row)
          .contains(number(row, "objid"));
      assertThat(of(number(row, "refclassid"), classes, constraints, namespaces, types)).as("refobjid of %s", row)
          .contains(number(row, "refobjid"));
    }
    // and the count agrees with the rows
    assertThat(number(resolve("select count(*) as n from pg_depend").rows.getFirst(), "n")).isEqualTo(9);
  }

  /** The columns of pg_depend are announced with PostgreSQL's types: oid and int4, and deptype as text. */
  @Test
  void pgDependAnnouncesItsColumnsAsIntegers() {
    final PostgresCatalog.Answer answer = resolve("select classid, objid, objsubid, refclassid, refobjid, refobjsubid, deptype from pg_depend");
    for (final String column : List.of("classid", "objid", "objsubid", "refclassid", "refobjid", "refobjsubid"))
      assertThat(answer.columns.get(column)).as(column).isEqualTo(PostgresType.INTEGER);
    assertThat(answer.columns.get("deptype")).isEqualTo(PostgresType.VARCHAR);
  }

  // ------------------------------------------------------------------------------------------------- helpers

  private static Set<Integer> of(final int catalog, final Set<Integer> classes, final Set<Integer> constraints,
      final Set<Integer> namespaces, final Set<Integer> types) {
    return switch (catalog) {
      case PG_CLASS -> classes;
      case PG_CONSTRAINT -> constraints;
      case PG_NAMESPACE -> namespaces;
      case PG_TYPE -> types;
      default -> Set.of();
    };
  }

  private Set<Integer> oids(final String query) {
    final Set<Integer> oids = new HashSet<>();
    for (final Map<String, Object> row : resolve(query).rows)
      oids.add(number(row, "oid"));
    return oids;
  }

  private int oidOfClass(final String name) {
    final List<Map<String, Object>> rows = resolve("select oid from pg_class where relname = '" + name + "'").rows;
    assertThat(rows).as(name).hasSize(1);
    return number(rows.getFirst(), "oid");
  }

  private int attnum(final int table, final String column) {
    final List<Map<String, Object>> rows = resolve(
        "select attnum from pg_attribute where attrelid = " + table + " and attname = '" + column + "'").rows;
    assertThat(rows).as(column).hasSize(1);
    return number(rows.getFirst(), "attnum");
  }

  private static int number(final Map<String, Object> row, final String column) {
    return ((Number) row.get(column)).intValue();
  }

  private PostgresCatalog.Answer resolve(final String query) {
    final PostgresCatalog.Answer answer = PostgresCatalog.resolve(query, database, USER);
    assertThat(answer).as("query was not recognised as a catalog query: %s", query).isNotNull();
    assertThat(answer).as("query was declined: %s", query).isNotSameAs(PostgresCatalog.DECLINED);
    return answer;
  }
}
