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

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract.NULL_STRATEGY;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JsonLImporterIT {

  private final static String DATABASE_PATH = "target/databases/arcadedb-jsonl-importer";

  @BeforeEach
  @AfterEach
  void cleanUp() {
    TestHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));

  }

  @Test
  void importDatabaseProgrammatically() {
    var databaseDirectory = new File(DATABASE_PATH);

    var inputFile = getClass().getClassLoader().getResource("arcadedb-export.jsonl.tgz");

    var importer = new Importer(
        ("-url " + inputFile.getFile() + " -database " + DATABASE_PATH + " -forceDatabaseCreate true").split(" "));
    Map<String, Object> loaded = importer.load();

    assertThat(databaseDirectory.exists()).isTrue();

    checkImportedDatabase();

  }

  @Test
  void importDatabaseBySql() {

    var databaseDirectory = new File(DATABASE_PATH);

    var inputFile = getClass().getClassLoader().getResource("arcadedb-export.jsonl.tgz");

    var db = new DatabaseFactory(DATABASE_PATH).create();

    db.command("sql", "import database file://" + inputFile.getFile());

    db.close();

    checkImportedDatabase();
  }

  @Test
  void importDatabaseWithErrorAbortMode() throws IOException {
    // A JSONL file with a malformed vertex (non-existent type) should fail in default "abort" mode (issue #6468)
    Path jsonlFile = Files.createTempFile("arcadedb-bad-import-", ".jsonl");
    try {
      Files.writeString(jsonlFile, ""
          + "{\"t\":\"info\",\"c\":{\"description\":\"test\",\"exporterVersion\":1,\"dbVersion\":\"25.1.1-SNAPSHOT\",\"dbBuild\":\"\",\"dbTimestamp\":\"\"}}\n"
          + "{\"t\":\"db\",\"c\":{\"name\":\"test\",\"executedOn\":\"2025-01-01\",\"executedOnTimestamp\":0}}\n"
          + "{\"t\":\"schema\",\"c\":{\"schemaVersion\":1,\"dbmsVersion\":\"25.1.1-SNAPSHOT\",\"dbmsBuild\":\"\",\"settings\":{\"zoneId\":\"UTC\",\"dateFormat\":\"yyyy-MM-dd\",\"dateTimeFormat\":\"yyyy-MM-dd HH:mm:ss\"},\"types\":{\"Person\":{\"type\":\"v\",\"parents\":[],\"buckets\":[\"Person_0\"],\"properties\":{\"id\":{\"type\":\"INTEGER\",\"custom\":{}}},\"indexes\":{},\"custom\":{}}}}}\n"
          + "{\"t\":\"v\",\"c\":{\"p\":{\"id\":1},\"r\":\"#6:0\",\"t\":\"Person\",\"o\":[],\"i\":[]}}\n"
          + "{\"t\":\"v\",\"c\":{\"p\":{\"id\":2},\"r\":\"#6:1\",\"t\":\"NonExistentType\",\"o\":[],\"i\":[]}}\n");

      var importer = new Importer(
          ("-url " + jsonlFile.toAbsolutePath() + " -database " + DATABASE_PATH + " -forceDatabaseCreate true").split(" "));

      assertThrows(ImportException.class, importer::load);
    } finally {
      Files.deleteIfExists(jsonlFile);
    }
  }

  @Test
  void importDatabaseWithErrorSkipMode() throws IOException {
    // With -onRowError skip, a malformed vertex should be skipped and counted (issue #6468)
    Path jsonlFile = Files.createTempFile("arcadedb-badskip-import-", ".jsonl");
    try {
      Files.writeString(jsonlFile, ""
          + "{\"t\":\"info\",\"c\":{\"description\":\"test\",\"exporterVersion\":1,\"dbVersion\":\"25.1.1-SNAPSHOT\",\"dbBuild\":\"\",\"dbTimestamp\":\"\"}}\n"
          + "{\"t\":\"db\",\"c\":{\"name\":\"test\",\"executedOn\":\"2025-01-01\",\"executedOnTimestamp\":0}}\n"
          + "{\"t\":\"schema\",\"c\":{\"schemaVersion\":1,\"dbmsVersion\":\"25.1.1-SNAPSHOT\",\"dbmsBuild\":\"\",\"settings\":{\"zoneId\":\"UTC\",\"dateFormat\":\"yyyy-MM-dd\",\"dateTimeFormat\":\"yyyy-MM-dd HH:mm:ss\"},\"types\":{\"Person\":{\"type\":\"v\",\"parents\":[],\"buckets\":[\"Person_0\"],\"properties\":{\"id\":{\"type\":\"INTEGER\",\"custom\":{}}},\"indexes\":{},\"custom\":{}}}}}\n"
          + "{\"t\":\"v\",\"c\":{\"p\":{\"id\":1},\"r\":\"#6:0\",\"t\":\"Person\",\"o\":[],\"i\":[]}}\n"
          + "{\"t\":\"v\",\"c\":{\"p\":{\"id\":2},\"r\":\"#6:1\",\"t\":\"NonExistentType\",\"o\":[],\"i\":[]}}\n");

      var importer = new Importer(
          ("-url " + jsonlFile.toAbsolutePath() + " -database " + DATABASE_PATH + " -forceDatabaseCreate true -onRowError skip").split(" "));
      Map<String, Object> result = importer.load();

      // Should succeed with the error counted and the valid vertex imported
      assertThat(result).containsEntry("errors", 1L);
      assertThat(result).containsEntry("createdVertices", 1L);
    } finally {
      Files.deleteIfExists(jsonlFile);
    }
  }

  @Test
  void importDatabaseSkipModeMalformedRidLeavesNoGhostRecord() throws IOException {
    // A vertex whose properties/type are otherwise valid but whose "r" (old RID) field is malformed used to fail
    // AFTER save(), leaving an orphaned record in the database that was never added to the RID map and never
    // counted - exactly the "ghost record" mechanism behind issue #6468's cascading edge loss. In skip mode this
    // must now roll back that record's own write instead of letting a later periodic commit persist it.
    Path jsonlFile = Files.createTempFile("arcadedb-ghostrecord-import-", ".jsonl");
    try {
      Files.writeString(jsonlFile, ""
          + "{\"t\":\"info\",\"c\":{\"description\":\"test\",\"exporterVersion\":1,\"dbVersion\":\"25.1.1-SNAPSHOT\",\"dbBuild\":\"\",\"dbTimestamp\":\"\"}}\n"
          + "{\"t\":\"db\",\"c\":{\"name\":\"test\",\"executedOn\":\"2025-01-01\",\"executedOnTimestamp\":0}}\n"
          + "{\"t\":\"schema\",\"c\":{\"schemaVersion\":1,\"dbmsVersion\":\"25.1.1-SNAPSHOT\",\"dbmsBuild\":\"\",\"settings\":{\"zoneId\":\"UTC\",\"dateFormat\":\"yyyy-MM-dd\",\"dateTimeFormat\":\"yyyy-MM-dd HH:mm:ss\"},\"types\":{\"Person\":{\"type\":\"v\",\"parents\":[],\"buckets\":[\"Person_0\"],\"properties\":{\"id\":{\"type\":\"INTEGER\",\"custom\":{}}},\"indexes\":{},\"custom\":{}}}}}\n"
          + "{\"t\":\"v\",\"c\":{\"p\":{\"id\":1},\"r\":\"#6:0\",\"t\":\"Person\",\"o\":[],\"i\":[]}}\n"
          + "{\"t\":\"v\",\"c\":{\"p\":{\"id\":2},\"r\":\"NOT-A-VALID-RID\",\"t\":\"Person\",\"o\":[],\"i\":[]}}\n"
          + "{\"t\":\"v\",\"c\":{\"p\":{\"id\":3},\"r\":\"#6:2\",\"t\":\"Person\",\"o\":[],\"i\":[]}}\n");

      var importer = new Importer(
          ("-url " + jsonlFile.toAbsolutePath() + " -database " + DATABASE_PATH + " -forceDatabaseCreate true -onRowError skip").split(" "));
      Map<String, Object> result = importer.load();

      assertThat(result).containsEntry("errors", 1L);
      assertThat(result).containsEntry("createdVertices", 2L);

      try (var db = new DatabaseFactory(DATABASE_PATH).open()) {
        // Ground truth: the malformed record must not be reachable in the database either, not just uncounted.
        assertThat(db.countType("Person", true)).isEqualTo(2L);
      }
    } finally {
      Files.deleteIfExists(jsonlFile);
    }
  }

  @Test
  void importDatabaseSkipModeFailedVertexDropsReferencingEdgeAsCountedError() throws IOException {
    // Reproduces the cascade from issue #6468: a vertex that fails to import leaves its old RID out of the RID
    // map, so an edge referencing it hits the "vertex not found" path. In skip mode both failures must now be
    // counted as errors and leave no partial edge behind, rather than being silently swallowed as before the fix.
    Path jsonlFile = Files.createTempFile("arcadedb-cascade-import-", ".jsonl");
    try {
      Files.writeString(jsonlFile, ""
          + "{\"t\":\"info\",\"c\":{\"description\":\"test\",\"exporterVersion\":1,\"dbVersion\":\"25.1.1-SNAPSHOT\",\"dbBuild\":\"\",\"dbTimestamp\":\"\"}}\n"
          + "{\"t\":\"db\",\"c\":{\"name\":\"test\",\"executedOn\":\"2025-01-01\",\"executedOnTimestamp\":0}}\n"
          + "{\"t\":\"schema\",\"c\":{\"schemaVersion\":1,\"dbmsVersion\":\"25.1.1-SNAPSHOT\",\"dbmsBuild\":\"\",\"settings\":{\"zoneId\":\"UTC\",\"dateFormat\":\"yyyy-MM-dd\",\"dateTimeFormat\":\"yyyy-MM-dd HH:mm:ss\"},"
          + "\"types\":{"
          + "\"Person\":{\"type\":\"v\",\"parents\":[],\"buckets\":[\"Person_0\"],\"properties\":{\"id\":{\"type\":\"INTEGER\",\"custom\":{}}},\"indexes\":{},\"custom\":{}},"
          + "\"Friend\":{\"type\":\"e\",\"parents\":[],\"buckets\":[\"Friend_0\"],\"properties\":{},\"indexes\":{},\"custom\":{}}"
          + "}}}\n"
          + "{\"t\":\"v\",\"c\":{\"p\":{\"id\":1},\"r\":\"#6:0\",\"t\":\"Person\",\"o\":[],\"i\":[]}}\n"
          + "{\"t\":\"v\",\"c\":{\"p\":{\"id\":2},\"r\":\"#6:1\",\"t\":\"NonExistentType\",\"o\":[],\"i\":[]}}\n"
          + "{\"t\":\"e\",\"c\":{\"p\":{},\"t\":\"Friend\",\"o\":\"#6:1\",\"i\":\"#6:0\"}}\n"
          + "{\"t\":\"v\",\"c\":{\"p\":{\"id\":3},\"r\":\"#6:2\",\"t\":\"Person\",\"o\":[],\"i\":[]}}\n");

      var importer = new Importer(
          ("-url " + jsonlFile.toAbsolutePath() + " -database " + DATABASE_PATH + " -forceDatabaseCreate true -onRowError skip").split(" "));
      Map<String, Object> result = importer.load();

      // One error for the failed vertex, one for the edge that could no longer resolve its out-vertex.
      assertThat(result).containsEntry("errors", 2L);
      assertThat(result).containsEntry("createdVertices", 2L);
      assertThat(result).doesNotContainKey("createdEdges");

      try (var db = new DatabaseFactory(DATABASE_PATH).open()) {
        assertThat(db.countType("Person", true)).isEqualTo(2L);
        assertThat(db.countType("Friend", true)).isEqualTo(0L);
      }
    } finally {
      Files.deleteIfExists(jsonlFile);
    }
  }

  private static void checkImportedDatabase() {
    try (var db = new DatabaseFactory(DATABASE_PATH).open()) {

      var schema = db.getSchema();

      //scheck schema
      assertThat(schema.getDateFormat()).isEqualTo("yyyy-MM-dd");
      assertThat(schema.getDateTimeFormat()).isEqualTo("yyyy-MM-dd HH:mm:ss");
      assertThat(schema.getZoneId()).isEqualTo(ZoneId.of("Europe/Rome"));

      //check types
      assertThat(schema.getTypes()).hasSize(2);
      assertThat(schema.getType("Person")).isNotNull()
          .satisfies(type -> {
            assertThat(type.getProperty("id").getType()).isEqualTo(Type.INTEGER);
            assertThat(type.getIndexesByProperties("id").get(0))
                .satisfies(index -> {
                  assertThat(index.getType()).isEqualTo(Schema.INDEX_TYPE.LSM_TREE);
                  assertThat(index.getNullStrategy()).isEqualTo(NULL_STRATEGY.SKIP);
                  assertThat(index.isUnique()).isTrue();
                });
          });
      assertThat(schema.getType("Friend")).isNotNull()
          .satisfies(type -> {
            assertThat(type.getProperty("id").getType()).isEqualTo(Type.INTEGER);
            assertThat(type.getIndexesByProperties("id").get(0))
                .satisfies(index -> {
                  assertThat(index.getType()).isEqualTo(Schema.INDEX_TYPE.LSM_TREE);
                  assertThat(index.getNullStrategy()).isEqualTo(NULL_STRATEGY.SKIP);
                  assertThat(index.isUnique()).isTrue();
                });
          });

      //check vertices
      assertThat(db.countType("Person", true)).isEqualTo(500);

      //check edges
      assertThat(db.countType("Friend", true)).isEqualTo(10000);

    }
  }



}