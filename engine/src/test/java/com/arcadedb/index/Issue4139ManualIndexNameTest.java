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
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #4139 regression: when {@code CREATE INDEX <manual_name> ON ...} is executed via SQL,
 * the user-supplied name must be honoured by the resulting {@link com.arcadedb.index.TypeIndex}
 * and persisted in {@code schema.json}, not silently replaced with the auto-generated
 * {@code typeName + "[" + propertyNames + "]"} form.
 *
 * Before the fix:
 *   {@code CREATE INDEX Abogado_uuid IF NOT EXISTS ON Abogado (uuid) UNIQUE NULL_STRATEGY SKIP}
 * created the index but registered it under the auto-derived name {@code Abogado[uuid]}, so
 * {@code existsIndex("Abogado_uuid")} returned {@code false} and the schema view in Studio
 * displayed the auto-derived name.
 *
 * After the fix the manual name survives the CREATE statement, the schema reload, and is
 * usable as the DROP INDEX target.
 *
 * https://github.com/ArcadeData/arcadedb/issues/4139
 */
class Issue4139ManualIndexNameTest extends TestHelper {

  @Test
  void manualIndexNameIsRegisteredOnSqlCreate() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Abogado");
      type.createProperty("uuid", String.class);
    });

    try (final ResultSet rs = database.command("sql",
        "create index Abogado_uuid if not exists on Abogado (uuid) unique NULL_STRATEGY SKIP")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("Abogado_uuid");
    }

    // The manual name must be registered with the schema so existsIndex/getIndexByName work
    // off it directly. This is the essence of the user report.
    assertThat(database.getSchema().existsIndex("Abogado_uuid")).isTrue();
    assertThat(database.getSchema().getIndexByName("Abogado_uuid")).isNotNull();
    assertThat(database.getSchema().getIndexByName("Abogado_uuid").getName()).isEqualTo("Abogado_uuid");

    // The auto-derived form must NOT also exist - otherwise the same TypeIndex would be
    // visible under two names and Studio's "Indexes" tab would show duplicates.
    assertThat(database.getSchema().existsIndex("Abogado[uuid]")).isFalse();

    // The TypeIndex should still be findable by the property tuple, so all the existing index-
    // selection paths in the planner continue to work.
    final TypeIndex byProperty = database.getSchema().getType("Abogado").getIndexByProperties("uuid");
    assertThat(byProperty).isNotNull();
    assertThat(byProperty.getName()).isEqualTo("Abogado_uuid");
  }

  @Test
  void manualIndexNameSurvivesReload() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Abogado");
      type.createProperty("uuid", String.class);
    });

    try (final ResultSet rs = database.command("sql",
        "create index Abogado_uuid if not exists on Abogado (uuid) unique NULL_STRATEGY SKIP")) {
      rs.hasNext();
    }

    reopenDatabase();

    // After closing and reopening, the manual name must still be the one carried by the
    // TypeIndex; otherwise the user has to re-issue the CREATE INDEX after every restart and
    // the post-restart Studio view reverts to the auto-derived name.
    assertThat(database.getSchema().existsIndex("Abogado_uuid")).isTrue();
    assertThat(database.getSchema().getType("Abogado").getIndexByProperties("uuid").getName())
        .isEqualTo("Abogado_uuid");
    assertThat(database.getSchema().existsIndex("Abogado[uuid]")).isFalse();
  }

  @Test
  void ifNotExistsIsIdempotentOnManualName() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Abogado");
      type.createProperty("uuid", String.class);
    });

    // First create - real creation.
    try (final ResultSet rs = database.command("sql",
        "create index Abogado_uuid if not exists on Abogado (uuid) unique NULL_STRATEGY SKIP")) {
      assertThat(rs.next().<Boolean>getProperty("created")).isTrue();
    }

    // Second create with same manual name - must be a no-op (created=false), not a duplicate.
    try (final ResultSet rs = database.command("sql",
        "create index Abogado_uuid if not exists on Abogado (uuid) unique NULL_STRATEGY SKIP")) {
      assertThat(rs.next().<Boolean>getProperty("created")).isFalse();
    }

    // Still exactly one TypeIndex.
    assertThat(database.getSchema().getType("Abogado").getAllIndexes(false)).hasSize(1);
  }

  @Test
  void manualIndexNameViaTypeIndexBuilder() {
    // Builder-level coverage so the fix is also validated independently of the SQL parser path
    // (parser might one day route through a different entry point).
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Abogado");
      type.createProperty("uuid", String.class);

      database.getSchema()
          .buildTypeIndex("Abogado", new String[] { "uuid" })
          .withType(Schema.INDEX_TYPE.LSM_TREE)
          .withUnique(true)
          .withIndexName("Abogado_uuid")
          .create();
    });

    assertThat(database.getSchema().existsIndex("Abogado_uuid")).isTrue();
    assertThat(database.getSchema().existsIndex("Abogado[uuid]")).isFalse();
  }

  @Test
  void manualIndexNameWorksForHashIndex() {
    // The fix lives at the schema/builder layer rather than inside any one index implementation,
    // so the same manual name plumbing must work for HASH (different toJSON/setMetadata code path)
    // as well as the default LSM_TREE family. Reload check included so the persistence channel is
    // exercised against HashIndex specifically.
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Abogado");
      type.createProperty("uuid", String.class);
    });

    try (final ResultSet rs = database.command("sql",
        "create index Abogado_uuid_hash if not exists on Abogado (uuid) UNIQUE_HASH")) {
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("Abogado_uuid_hash");
    }

    assertThat(database.getSchema().existsIndex("Abogado_uuid_hash")).isTrue();
    assertThat(database.getSchema().existsIndex("Abogado[uuid]")).isFalse();

    reopenDatabase();

    assertThat(database.getSchema().existsIndex("Abogado_uuid_hash")).isTrue();
    assertThat(database.getSchema().getType("Abogado").getIndexByProperties("uuid").getName())
        .isEqualTo("Abogado_uuid_hash");
  }

  @Test
  void manualIndexNameWorksForFullTextIndex() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Abogado");
      type.createProperty("uuid", String.class);
    });

    try (final ResultSet rs = database.command("sql",
        "create index Abogado_uuid_ft if not exists on Abogado (uuid) FULL_TEXT")) {
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("Abogado_uuid_ft");
    }

    assertThat(database.getSchema().existsIndex("Abogado_uuid_ft")).isTrue();
    assertThat(database.getSchema().existsIndex("Abogado[uuid]")).isFalse();

    reopenDatabase();
    assertThat(database.getSchema().existsIndex("Abogado_uuid_ft")).isTrue();
  }

  @Test
  void noManualNameStillUsesAutoDerivedName() {
    // Sanity check: the auto-derived path is unchanged when no manual name is supplied. Before
    // touching this path I want to confirm the existing default still produces the documented
    // {@code typeName[propertyName]} shape, which other tests and Studio rely on.
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Abogado");
      type.createProperty("uuid", String.class);
    });

    try (final ResultSet rs = database.command("sql",
        "create index if not exists on Abogado (uuid) unique NULL_STRATEGY SKIP")) {
      rs.hasNext();
    }

    assertThat(database.getSchema().existsIndex("Abogado[uuid]")).isTrue();
  }
}
