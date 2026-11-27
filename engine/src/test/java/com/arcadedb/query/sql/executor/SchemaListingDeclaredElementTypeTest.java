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
package com.arcadedb.query.sql.executor;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityHelper;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The schema listings leave out the types the bound user may not read, and the parents of a type it may read. The type a
 * property is declared of ({@code EMBEDDED OF T}, {@code LIST OF T}, {@code MAP OF T}, {@code LINK OF T}) names a type too, so
 * a listing shows it only when the user may read that type, and shows the property without it otherwise.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class SchemaListingDeclaredElementTypeTest {
  private static final String PATH       = "target/databases/SchemaListingDeclaredElementTypeTest";
  private static final String RESTRICTED = "Restricted";
  private static final String HOLDER     = "Holder";

  private DatabaseFactory factory;
  private Database        database;

  @BeforeEach
  void setUp() {
    factory = new DatabaseFactory(PATH);
    if (factory.exists())
      factory.open().drop();

    database = factory.create();

    final DocumentType restricted = database.getSchema().createDocumentType(RESTRICTED);
    restricted.createProperty("code", Type.STRING);
    restricted.setAliases(Set.of("Classified"));

    final DocumentType holder = database.getSchema().createDocumentType(HOLDER);
    holder.createProperty("embedded", Type.EMBEDDED, RESTRICTED);
    holder.createProperty("items", Type.LIST, RESTRICTED);
    holder.createProperty("byKey", Type.MAP, RESTRICTED);
    holder.createProperty("target", Type.LINK, RESTRICTED);
    holder.createProperty("viaAlias", Type.LIST, "Classified");
    holder.createProperty("names", Type.LIST, "STRING");
    holder.createProperty("counters", Type.MAP, "LONG");
    holder.createProperty("title", Type.STRING);
  }

  @AfterEach
  void tearDown() {
    unbindUser();
    database.drop();
    factory.close();
  }

  @Test
  void schemaTypesShowsTheDeclaredTypeOnlyToAUserWhoMayReadIt() {
    bindUserDenying(Set.of(RESTRICTED));

    final Map<String, Result> restrictedView = propertiesOfHolder();
    for (final String name : List.of("embedded", "items", "byKey", "target", "viaAlias"))
      assertThat(restrictedView.get(name).<String>getProperty("ofType")).as("ofType of %s", name).isNull();
    assertThat(restrictedView.get("names").<String>getProperty("ofType")).isEqualTo("STRING");
    assertThat(restrictedView.get("counters").<String>getProperty("ofType")).isEqualTo("LONG");
    assertThat(restrictedView.get("embedded").<Type>getProperty("type")).isEqualTo(Type.EMBEDDED);

    final List<String> wholeAnswer = new ArrayList<>();
    database.query("sql", "SELECT FROM schema:types").forEachRemaining(r -> wholeAnswer.add(r.toJSON().toString()));
    assertThat(wholeAnswer).isNotEmpty().noneMatch(row -> row.contains(RESTRICTED) || row.contains("Classified"));

    unbindUser();
    final Map<String, Result> fullView = propertiesOfHolder();
    for (final String name : List.of("embedded", "items", "byKey", "target"))
      assertThat(fullView.get(name).<String>getProperty("ofType")).as("ofType of %s", name).isEqualTo(RESTRICTED);
    assertThat(fullView.get("viaAlias").<String>getProperty("ofType")).isEqualTo("Classified");
  }

  @Test
  void schemaTypesKeepsTheDeclaredTypeOfAReadableType() {
    final DocumentType readable = database.getSchema().createDocumentType("Shared");
    readable.createProperty("label", Type.STRING);
    database.getSchema().getType(HOLDER).createProperty("shared", Type.LIST, "Shared");

    bindUserDenying(Set.of(RESTRICTED));

    assertThat(propertiesOfHolder().get("shared").<String>getProperty("ofType")).isEqualTo("Shared");
  }

  @Test
  void showConstraintsShowsTheDeclaredTypeOnlyToAUserWhoMayReadIt() {
    bindUserDenying(Set.of(RESTRICTED));

    final List<String> rows = new ArrayList<>();
    final List<String> listTypes = new ArrayList<>();
    database.query("opencypher", "SHOW CONSTRAINTS").forEachRemaining(r -> {
      rows.add(r.toJSON().toString());
      if (r.<List<String>>getProperty("properties").contains("items"))
        listTypes.add(r.getProperty("propertyType"));
    });
    assertThat(rows).isNotEmpty().noneMatch(row -> row.contains(RESTRICTED) || row.contains("Classified"));
    assertThat(listTypes).containsExactly("LIST<ANY>");

    unbindUser();
    final List<String> unrestrictedListTypes = new ArrayList<>();
    database.query("opencypher", "SHOW CONSTRAINTS").forEachRemaining(r -> {
      if (r.<List<String>>getProperty("properties").contains("items"))
        unrestrictedListTypes.add(r.getProperty("propertyType"));
    });
    assertThat(unrestrictedListTypes).containsExactly("LIST<" + RESTRICTED + ">");
  }

  @Test
  void readableOfTypeAnswersWhatTheUserMayBeTold() {
    final DocumentType holder = database.getSchema().getType(HOLDER);
    final Property items = holder.getProperty("items");
    final Property names = holder.getProperty("names");
    final Property title = holder.getProperty("title");

    // no bound user sees the declaration as it is
    assertThat(SecurityHelper.readableOfType(database, items)).isEqualTo(RESTRICTED);

    bindUserDenying(Set.of(RESTRICTED));
    assertThat(SecurityHelper.readableOfType(database, items)).isNull();
    assertThat(SecurityHelper.readableOfType(database, holder.getProperty("viaAlias"))).isNull();
    assertThat(SecurityHelper.readableOfType(database, holder.getProperty("target"))).isNull();
    // a scalar element type is no schema type, and a property declared of nothing has nothing to hide
    assertThat(SecurityHelper.readableOfType(database, names)).isEqualTo("STRING");
    assertThat(SecurityHelper.readableOfType(database, title)).isNull();
  }

  private Map<String, Result> propertiesOfHolder() {
    final Result row;
    try (final ResultSet rs = database.query("sql", "SELECT FROM schema:types WHERE name = '" + HOLDER + "'")) {
      assertThat(rs.hasNext()).isTrue();
      row = rs.next();
    }
    final Map<String, Result> byName = new HashMap<>();
    final List<Result> properties = row.getProperty("properties");
    for (final Result property : properties)
      byName.put(property.getProperty("name"), property);
    return byName;
  }

  private void unbindUser() {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
  }

  /** Denies every access on the buckets of the given types and grants everything else. */
  private void bindUserDenying(final Set<String> deniedTypes) {
    final Set<Integer> deniedBucketIds = new HashSet<>();
    for (final String typeName : deniedTypes)
      deniedBucketIds.addAll(database.getSchema().getType(typeName).getBucketIds(false));

    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return "restricted";
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
        return -1L;
      }

      @Override
      public long getReadTimeout() {
        return -1L;
      }
    });
  }
}
