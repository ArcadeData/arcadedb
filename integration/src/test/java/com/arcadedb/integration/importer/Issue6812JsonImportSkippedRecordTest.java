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
package com.arcadedb.integration.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6812: {@code JSONImporterFormat.createRecord()} used the same return signal for two
 * opposite outcomes - "there is no mapping object for this record, save it as an anonymous document" and "this record
 * is rejected or resolves to an already existing one, drop it". {@code parseRecord()} collapsed both into
 * {@code attributes.map}, so {@code parseRecordsArray()} materialized a record the importer had just logged (and
 * counted) as skipped into {@code settings.documentTypeName}.
 * <p>
 * Every case below imports the same two-entry source; only the mapping changes, so each test isolates one of the
 * "skip" branches of {@code createRecord()}. The last test is the guard on the opposite side: with no mapping object
 * the anonymous save is the intended behaviour and must keep working.
 */
class Issue6812JsonImportSkippedRecordTest {

  private static final String SOURCE_URL = "file://src/test/resources/importer-6812-duplicate-ids.json";

  @Test
  void duplicateIdRecordIsSkippedInsteadOfSavedAsAnonymousDocument() {
    final String databasePath = "target/databases/test-import-6812-duplicate-id";

    // No "@strategy": the second entry has the same @id as the first, so it is looked up, found, and skipped.
    final Map<String, Object> result = importWithMapping(databasePath, """
        {
          "Users":[
            {
              "@cat":"v",
              "@type":"User",
              "@id":"id"
            }
          ]
        }""");

    assertThat(result.get("parsedRecords")).isEqualTo(2L);
    assertThat(result.get("createdVertices")).isEqualTo(1L);

    withDatabase(databasePath, db -> {
      assertThat(db.countType("User", true)).isEqualTo(1);
      // The skipped duplicate must not reappear under the default document type name.
      assertThat(db.getSchema().existsType("Document")).isFalse();
    });
  }

  @Test
  void mappingObjectWithoutCategoryIsSkippedInsteadOfSavedAsAnonymousDocument() {
    final String databasePath = "target/databases/test-import-6812-no-cat";

    final Map<String, Object> result = importWithMapping(databasePath, """
        {
          "Users":[
            {
              "@type":"User"
            }
          ]
        }""");

    // Both entries are rejected and already counted as errors: they must not be saved anywhere.
    assertThat(result.get("errors")).isEqualTo(2L);

    withDatabase(databasePath, db -> {
      assertThat(db.getSchema().existsType("Document")).isFalse();
      assertThat(db.getSchema().existsType("User")).isFalse();
    });
  }

  @Test
  void mappingObjectWithoutTypeIsSkippedInsteadOfSavedAsAnonymousDocument() {
    final String databasePath = "target/databases/test-import-6812-no-type";

    final Map<String, Object> result = importWithMapping(databasePath, """
        {
          "Users":[
            {
              "@cat":"v"
            }
          ]
        }""");

    assertThat(result.get("errors")).isEqualTo(2L);

    withDatabase(databasePath, db -> assertThat(db.getSchema().existsType("Document")).isFalse());
  }

  @Test
  void topLevelEdgeMappingIsSkippedInsteadOfSavedAsAnonymousDocument() {
    final String databasePath = "target/databases/test-import-6812-top-level-edge";

    // "@cat":"e" at the top level is explicitly ignored in this phase (edges are created while mapping a parent
    // record), so nothing at all must be persisted for these two entries.
    final Map<String, Object> result = importWithMapping(databasePath, """
        {
          "Users":[
            {
              "@cat":"e",
              "@type":"HAS_MANAGER"
            }
          ]
        }""");

    assertThat(result.get("createdEdges")).isNull();

    withDatabase(databasePath, db -> assertThat(db.getSchema().existsType("Document")).isFalse());
  }

  @Test
  void recordsWithoutAnyMappingObjectAreStillSavedAsAnonymousDocuments() {
    final String databasePath = "target/databases/test-import-6812-anonymous";

    // The opposite outcome, which shares the same return value in parseRecord(): an empty mapping array means there
    // is no mapping object for these records, and saving them as anonymous documents is exactly what must happen.
    final Map<String, Object> result = importWithMapping(databasePath, """
        {
          "Users":[]
        }""");

    assertThat(result.get("parsedRecords")).isEqualTo(2L);

    withDatabase(databasePath, db -> assertThat(db.countType("Document", true)).isEqualTo(2));
  }

  private Map<String, Object> importWithMapping(final String databasePath, final String mapping) {
    dropIfExists(databasePath);

    final Importer importer = new Importer(new String[] {
        "-url", SOURCE_URL,
        "-database", databasePath,
        "-forceDatabaseCreate", "true",
        "-mapping", mapping
    });

    return importer.load();
  }

  private void withDatabase(final String databasePath, final Consumer<Database> assertions) {
    try (final Database db = new DatabaseFactory(databasePath).open()) {
      assertions.accept(db);
    } finally {
      dropIfExists(databasePath);
    }
  }

  private void dropIfExists(final String databasePath) {
    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();
  }
}
