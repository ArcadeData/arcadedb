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
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The PostgreSQL catalog emulation answers from the schema before a statement reaches the SQL engine, so no per-type gate ran
 * for it: a user denied a type by its per-type permissions read the type's whole definition - its name, every column and its
 * type, its indexes, its inheritance and, for a materialized view, the defining query - through pg_class, pg_attribute, pg_index,
 * pg_inherits, information_schema and pg_matviews, while {@code SELECT FROM schema:types} hid it from the same user. Every
 * relation of the catalog now describes only the types the session's principal may read.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PostgresCatalogPerTypeAclTest {
  private static final String DATABASE_PATH = "./target/databases/PostgresCatalogPerTypeAclTest";
  private static final String USER          = "scoped";

  /** Relations that name the types of the database, each queried whole. */
  private static final List<String> QUERIES = List.of(//
      "select relname, relkind from pg_class",//
      "select tablename from pg_tables",//
      "select table_name from information_schema.tables",//
      "select table_name, column_name from information_schema.columns",//
      "select table_name from information_schema.views",//
      "select attname from pg_attribute",//
      "select * from pg_index",//
      "select * from pg_inherits",//
      "select matviewname, definition from pg_matviews");

  private Database database;

  @BeforeEach
  void createDatabase() {
    final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
    if (factory.exists())
      factory.open().drop();

    database = factory.create();
    database.transaction(() -> {
      final Schema schema = database.getSchema();
      schema.createDocumentType("PublicDoc").createProperty("title", Type.STRING);

      schema.createDocumentType("SecretDoc").createProperty("cardNumber", Type.STRING);
      schema.getType("SecretDoc").createProperty("ssn", Type.STRING);
      schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "SecretDoc", "cardNumber");

      // a readable type extending one the user may not read
      schema.createDocumentType("SecretBase").createProperty("internalCode", Type.STRING);
      schema.createDocumentType("PublicChild").addSuperType("SecretBase");
    });
    database.command("sql", "CREATE MATERIALIZED VIEW SecretView AS SELECT ssn FROM SecretDoc WHERE ssn = 'literal-in-the-query'");
  }

  @AfterEach
  void dropDatabase() {
    if (database != null && database.isOpen()) {
      DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
      database.drop();
    }
  }

  @Test
  void theCatalogDescribesOnlyTheTypesThePrincipalMayRead() {
    // control: unbound (embedded), every type is described
    final String everything = render(QUERIES);
    assertThat(everything).contains("SecretDoc", "cardNumber", "SecretView", "literal-in-the-query", "PublicDoc");

    bindUserDenying(Set.of("SecretDoc", "SecretBase", "SecretView"));

    // the same principal is refused the records and does not see the type in the SQL schema listing
    try (final ResultSet rs = database.query("sql", "SELECT name FROM schema:types")) {
      final Set<String> names = new HashSet<>();
      rs.stream().forEach(r -> names.add(r.getProperty("name")));
      assertThat(names).contains("PublicDoc").doesNotContain("SecretDoc");
    }

    for (final String query : QUERIES) {
      final String answer = render(List.of(query));
      assertThat(answer).as(query).doesNotContain("SecretDoc", "cardNumber", "SecretView", "literal-in-the-query",
          "SecretBase");
    }
    // a column a readable type inherits is one of ITS columns, as SELECT FROM schema:types lists it: only the parent is hidden
    assertThat(render(List.of("select column_name from information_schema.columns where table_name = 'PublicChild'")))
        .contains("internalCode");
    assertThat(render(List.of("select relname from pg_class"))).contains("PublicDoc", "PublicChild");

    // a readable type extending a hidden one lists no parent, not a dangling oid
    assertThat(PostgresCatalog.resolve("select * from pg_inherits", database, USER).rows).isEmpty();

    // a lookup by name answers a hidden type as one that does not exist
    assertThat(render(List.of("select column_name from information_schema.columns where table_name = 'SecretDoc'")))
        .isEqualTo(render(List.of("select column_name from information_schema.columns where table_name = 'NoSuchType'")));

    // and the row types of pg_type, which the executor answers before the catalog
    final StringBuilder rowTypes = new StringBuilder();
    for (final Map<String, Object> row : PostgresCatalog.rowTypes(database))
      rowTypes.append(row).append('\n');
    assertThat(rowTypes.toString()).contains("PublicDoc").doesNotContain("SecretDoc", "SecretView", "SecretBase");
  }

  private String render(final List<String> queries) {
    final StringBuilder out = new StringBuilder();
    for (final String query : queries) {
      final PostgresCatalog.Answer answer = PostgresCatalog.resolve(query, database, USER);
      assertThat(answer).as(query).isNotNull();
      assertThat(answer).as(query).isNotSameAs(PostgresCatalog.DECLINED);
      for (final Map<String, Object> row : answer.rows)
        out.append(row).append('\n');
    }
    return out.toString();
  }

  /** Denies the buckets of the given types, as the per-type ACL of a server user resolves them. */
  private void bindUserDenying(final Set<String> deniedTypes) {
    final Set<Integer> deniedBucketIds = new HashSet<>();
    for (final String typeName : deniedTypes)
      deniedBucketIds.addAll(database.getSchema().getType(typeName).getBucketIds(false));

    DatabaseContext.INSTANCE.getContext(((DatabaseInternal) database).getDatabasePath()).setCurrentUser(new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return USER;
      }

      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        return !deniedBucketIds.contains(fileId);
      }

      @Override
      public boolean requestAccessOnType(final String typeName, final ACCESS access) {
        return !deniedTypes.contains(typeName);
      }

      @Override
      public long getResultSetLimit() {
        return -1;
      }

      @Override
      public long getReadTimeout() {
        return -1;
      }
    });
  }
}
