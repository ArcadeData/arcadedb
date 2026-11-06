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
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10711 and the two issues it groups: a reference column of the emulated catalog must name a row the catalog has, and a
 * relation has one row set whichever shape of statement reads it.
 * <ul>
 * <li>#10592: six scalar types named an array type ({@code typarray}) that {@code pg_type} had no row for, so a client resolving
 * {@code varchar[]} - pgjdbc's {@code createArrayOf("varchar", ...)} - found nothing.</li>
 * <li>#10693 (refiled as #10745): {@code pg_namespace} read on its own had no row for {@code pg_catalog} (OID 11), the
 * {@code typnamespace} of every type, while the same relation read through a join had it; {@code pg_am} had the same split.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10711CatalogReferencesTest {
  private static final String DATABASE_PATH = "./target/databases/Issue10711CatalogReferencesTest";
  private static final String USER          = "root";

  private Database database;

  @BeforeEach
  void createDatabase() {
    final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
    if (factory.exists())
      factory.open().drop();

    database = factory.create();
    // one of everything a family of the catalog has rows for: a type with a super type, a declared default, a unique index and a
    // materialized view
    database.transaction(() -> {
      final Schema schema = database.getSchema();
      final VertexType person = schema.createVertexType("Person");
      person.createProperty("name", Type.STRING);
      person.createProperty("age", Type.INTEGER).setDefaultValue("0");
      schema.createVertexType("Employee").addSuperType("Person").createProperty("salary", Type.DOUBLE);
      schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Person", "name");
    });
    database.transaction(() -> database.command("sql", "INSERT INTO Person SET name = 'a', age = 1"));
    database.command("sql", "CREATE MATERIALIZED VIEW PersonView AS SELECT name FROM Person");
  }

  @AfterEach
  void dropDatabase() {
    if (database != null && database.isOpen())
      database.drop();
  }

  // ---------------------------------------------------------------- #10592: typarray and typelem resolve

  /** The invariant the report states, over the rows a client reads: every array and element OID pg_type names is a row of it. */
  @Test
  void everyTypeReferenceOfPgTypeIsARowOfPgType() {
    final List<Map<String, Object>> rows = resolve("select oid, typname, typelem, typarray from pg_catalog.pg_type").rows;
    final Map<Integer, Map<String, Object>> byOid = new HashMap<>();
    for (final Map<String, Object> row : rows)
      byOid.put(number(row, "oid"), row);

    for (final Map<String, Object> row : rows) {
      final int oid = number(row, "oid");
      final int array = number(row, "typarray");
      final int element = number(row, "typelem");
      if (array != 0) {
        assertThat(byOid).as("the array type of %s", row.get("typname")).containsKey(array);
        assertThat(number(byOid.get(array), "typelem")).as("the element of the array type of %s", row.get("typname")).isEqualTo(oid);
      }
      if (element != 0) {
        assertThat(byOid).as("the element type of %s", row.get("typname")).containsKey(element);
        assertThat(number(byOid.get(element), "typarray")).as("the array type of the element of %s", row.get("typname"))
            .isEqualTo(oid);
      }
    }
  }

  /** The report's own sequence, the one a JDBC driver runs for {@code createArrayOf("varchar", ...)}. */
  @Test
  void theArrayTypeOfVarcharIsAType() {
    assertThat(column(resolve("select t.typarray from pg_catalog.pg_type t where t.typname = 'varchar'"), "typarray"))
        .containsExactly(1015);
    assertThat(column(resolve("select typtype, typname, pg_type.oid from pg_catalog.pg_type where oid = 1015"), "typname"))
        .containsExactly("_varchar");
    assertThat(column(resolve("select oid from pg_catalog.pg_type where typname = '_varchar'"), "oid")).containsExactly(1015);
    assertThat(column(resolve("select pg_catalog.format_type(1015, -1) as t from pg_type where oid = 1015"), "t"))
        .containsExactly("character varying[]");
  }

  @Test
  void theSixMissingArrayTypesAreRows() {
    final PostgresCatalog.Answer answer = resolve(
        "select oid, typname, typelem from pg_catalog.pg_type where oid in (1001, 1005, 1014, 1015, 1182, 1115) order by oid");

    assertThat(column(answer, "typname")).containsExactly("_bytea", "_int2", "_bpchar", "_varchar", "_timestamp", "_date");
    assertThat(column(answer, "typelem")).containsExactly(17, 21, 1042, 1043, 1114, 1082);
  }

  /** A client that binds a parameter of one of the six array types sends its OID: the protocol parses it rather than refusing. */
  @Test
  void aParameterOfTheNewArrayTypesIsParsed() {
    assertThat(PostgresType.deserialize(1015, 0, "{a,\"b c\"}".getBytes())).isEqualTo(List.of("a", "b c"));
    assertThat(PostgresType.deserialize(1005, 0, "{1,2}".getBytes())).isEqualTo(List.of((short) 1, (short) 2));
    assertThat(PostgresType.deserialize(1014, 0, "{x}".getBytes())).isEqualTo(List.of("x"));
    assertThat((List<?>) PostgresType.deserialize(1182, 0, "{2024-01-02}".getBytes())).hasSize(1);
    assertThat((List<?>) PostgresType.deserialize(1115, 0, "{\"2024-01-02 03:04:05\"}".getBytes())).hasSize(1);
    assertThat((List<?>) PostgresType.deserialize(1001, 0, "{\"\\\\x0102\"}".getBytes())).hasSize(1);
  }

  // ---------------------------------------------------------------- #10745: one row set per relation

  /** The report's repro: the namespace every type names is a row of pg_namespace when pg_namespace is read on its own. */
  @Test
  void theNamespaceOfEveryTypeIsARowOfPgNamespace() {
    assertThat(column(resolve("select typname, typnamespace from pg_type where typname in ('int4', 'varchar')"), "typnamespace"))
        .containsOnly(11);

    final PostgresCatalog.Answer namespaces = resolve("select oid, nspname from pg_namespace order by oid");
    assertThat(column(namespaces, "oid")).containsExactly(11, 2200);
    assertThat(column(namespaces, "nspname")).containsExactly("pg_catalog", database.getName());

    assertThat(column(resolve("select oid, nspname from pg_namespace where oid = 11"), "nspname")).containsExactly("pg_catalog");
    assertThat(column(resolve("select count(*) from pg_namespace"), "count")).containsExactly(2L);

    assertThat(column(resolve("select t.typname, n.nspname from pg_type t, pg_namespace n where t.typnamespace = n.oid and "
        + "t.typname = 'int4'"), "nspname")).containsExactly("pg_catalog");
    assertThat(column(resolve("select distinct n.nspname, n.oid from pg_namespace n, pg_index i where i.indrelid > 0 order by 2"),
        "oid")).containsExactly(11, 2200);
  }

  /** The user's relations are still all in the one schema, and a schema browser filtering out the catalog sees only that. */
  @Test
  void theUsersRelationsStayInTheirOneSchema() {
    assertThat(column(resolve("select distinct n.nspname from pg_class c join pg_namespace n on n.oid = c.relnamespace"), "nspname"))
        .containsExactly(database.getName());
    // psql's \dn
    assertThat(column(resolve("""
        SELECT n.nspname AS "Name", pg_catalog.pg_get_userbyid(n.nspowner) AS "Owner"
        FROM pg_catalog.pg_namespace n
        WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
        ORDER BY 1;"""), "Name")).containsExactly(database.getName());
  }

  /**
   * Found with the two-row pg_namespace: an ORDER BY naming a column the projection carries under another name - DbVisualizer's
   * schema list - left the rows in the order they were built, as it did any table list ordered that way.
   */
  @Test
  void anOrderByAColumnProjectedUnderAnotherNameSorts() {
    assertThat(column(resolve("select nspname as schema_name from pg_namespace order by NSPNAME desc"), "schema_name"))
        .containsExactly("pg_catalog", database.getName());
    assertThat(column(resolve("select nspname as schema_name from pg_namespace order by nspname asc"), "schema_name"))
        .containsExactly(database.getName(), "pg_catalog");
    assertThat(column(resolve("select c.relname as r from pg_class c where c.relkind = 'r' order by c.relname desc"), "r"))
        .containsExactly("Person", "Employee");
    assertThat(column(resolve("select relname as r from pg_class where relkind = 'r' order by relname desc"), "r"))
        .containsExactly("Person", "Employee");
  }

  /** {@code pg_am} on its own: the heap every type is stored in and the index families, as a join reads it. */
  @Test
  void pgAmIsTheSameRelationOnItsOwnAndInAJoin() {
    final PostgresCatalog.Answer alone = resolve("select oid, amname, amtype from pg_am order by oid");
    assertThat(column(alone, "amname")).containsExactly("heap", "btree", "hash", "gist", "gin", "brin", "hnsw");
    assertThat(column(alone, "amtype")).containsExactly("t", "i", "i", "i", "i", "i", "i");

    // \dt joins it on relam, and the table's access method is the heap
    assertThat(column(resolve("select am.amname from pg_class c left join pg_am am on am.oid = c.relam where c.relname = 'Person'"),
        "amname")).containsExactly("heap");
  }

  /** The default collation every text type names in typcollation is a row of pg_collation. */
  @Test
  void theCollationOfTheTextTypesIsARowOfPgCollation() {
    assertThat(column(resolve("select distinct typcollation from pg_type where typcollation <> 0"), "typcollation"))
        .containsExactly(100);
    final PostgresCatalog.Answer collations = resolve("select oid, collname, collnamespace from pg_collation");
    assertThat(column(collations, "oid")).containsExactly(100);
    assertThat(column(collations, "collname")).containsExactly("default");
    assertThat(column(collations, "collnamespace")).containsExactly(11);
  }

  /**
   * The guarantee the report asks for: for every relation this catalog models, the rows a join reads are the rows the relation has
   * when it is read on its own. A join may filter, it may not conjure; and a relation that is refused on its own has nothing a join
   * can read either.
   */
  @Test
  void everyRelationHasTheSameRowsReadAloneAndThroughAJoin() {
    final Set<String> checked = new HashSet<>();
    for (final String relation : PostgresCatalog.modelledRelations()) {
      final PostgresCatalog.Answer alone = PostgresCatalog.resolve("select * from " + relation, database, USER);
      // the join path: a table function next to it makes the statement a join of the rows of each entry
      final PostgresCatalog.Answer joined = PostgresCatalog.resolve(
          "select x.* from " + relation + " x, generate_series(1, 1) g", database, USER);

      assertThat(joined).as("answered: %s through a join", relation).isNotNull();
      assertThat(joined.rows).as("answered: %s through a join", relation).isNotNull();
      if (alone == null || alone.rows == null) {
        assertThat(joined.rows).as("%s is refused on its own, so a join has no row of it", relation).isEmpty();
        continue;
      }
      assertThat(normalized(joined.rows)).as("the rows of %s through a join", relation)
          .containsExactlyInAnyOrderElementsOf(normalized(alone.rows));
      checked.add(relation);
    }
    assertThat(checked).contains("pg_namespace", "pg_am", "pg_type", "pg_class", "pg_attribute", "pg_index", "pg_collation");
  }

  // ---------------------------------------------------------------- helpers

  private PostgresCatalog.Answer resolve(final String query) {
    final PostgresCatalog.Answer answer = PostgresCatalog.resolve(query, database, USER);
    assertThat(answer).as("answered: %s", query).isNotNull();
    assertThat(answer.rows).as("answered rather than declined: %s", query).isNotNull();
    return answer;
  }

  /** Rows compared by value, column names folded and vectors read as text, whatever map each path builds them in. */
  private static List<Map<String, Object>> normalized(final List<Map<String, Object>> rows) {
    final List<Map<String, Object>> result = new ArrayList<>(rows.size());
    for (final Map<String, Object> row : rows) {
      final Map<String, Object> copy = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
      for (final Map.Entry<String, Object> entry : row.entrySet())
        copy.put(entry.getKey(), entry.getValue() instanceof List<?> list ? list.toString() : entry.getValue());
      result.add(copy);
    }
    return result;
  }

  private static int number(final Map<String, Object> row, final String column) {
    return ((Number) row.get(column)).intValue();
  }

  private static List<Object> column(final PostgresCatalog.Answer answer, final String name) {
    final List<Object> values = new ArrayList<>();
    for (final Map<String, Object> row : answer.rows)
      values.add(row.get(name));
    return values;
  }
}
