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
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract.NULL_STRATEGY;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

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
  void importDatabaseWithErrorAbortMode() throws Exception {
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

      assertThatExceptionOfType(ImportException.class).isThrownBy(importer::load);
    } finally {
      Files.deleteIfExists(jsonlFile);
    }
  }

  @Test
  void importDatabaseWithErrorSkipMode() throws Exception {
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
  void importDatabaseSkipModeMalformedRidLeavesNoGhostRecord() throws Exception {
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
  void importDatabaseSkipModeFailedVertexDropsReferencingEdgeAsCountedError() throws Exception {
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

  @Test
  void importDatabaseRemapsLinkTypedPropertyValues() throws Exception {
    // Issue #6460: a LINK-typed property (and a LIST-of-LINK one) must be remapped through the same old-RID -> new-RID
    // index edges already use, not passed through with the source database's RID. The source "r" fields below are
    // deliberately NOT #6:0, #6:1, ... : a fresh import always allocates sequential positions starting at #6:0 in an
    // empty bucket, so any "r" that doesn't already follow that sequence is guaranteed to land at a DIFFERENT RID on
    // import - exactly the "restore into fresh buckets" scenario the issue describes - which is what makes a
    // still-unremapped (still equal to the old "r") link value observable.
    //
    // Person#2.bestFriend -> Person#1 is a BACKWARD reference (Person#1 was already imported): resolved immediately.
    // Person#3.friends -> [Person#4] is a FORWARD reference (Person#4 is imported LATER in the stream): only the
    // reconciliation pass fixes this one up, once every RID mapping is known.
    Path jsonlFile = Files.createTempFile("arcadedb-link-remap-import-", ".jsonl");
    try {
      Files.writeString(jsonlFile, ""
          + "{\"t\":\"info\",\"c\":{\"description\":\"test\",\"exporterVersion\":1,\"dbVersion\":\"25.1.1-SNAPSHOT\",\"dbBuild\":\"\",\"dbTimestamp\":\"\"}}\n"
          + "{\"t\":\"db\",\"c\":{\"name\":\"test\",\"executedOn\":\"2025-01-01\",\"executedOnTimestamp\":0}}\n"
          + "{\"t\":\"schema\",\"c\":{\"schemaVersion\":1,\"dbmsVersion\":\"25.1.1-SNAPSHOT\",\"dbmsBuild\":\"\",\"settings\":{\"zoneId\":\"UTC\",\"dateFormat\":\"yyyy-MM-dd\",\"dateTimeFormat\":\"yyyy-MM-dd HH:mm:ss\"},"
          + "\"types\":{\"Person\":{\"type\":\"v\",\"parents\":[],\"buckets\":[\"Person_0\"],\"properties\":{"
          + "\"id\":{\"type\":\"INTEGER\",\"custom\":{}},"
          + "\"bestFriend\":{\"type\":\"LINK\",\"custom\":{}},"
          + "\"friends\":{\"type\":\"LIST\",\"of\":\"LINK\",\"custom\":{}}"
          + "},\"indexes\":{},\"custom\":{}}}}}\n"
          + "{\"t\":\"v\",\"c\":{\"p\":{\"id\":1},\"r\":\"#6:5\",\"t\":\"Person\",\"o\":[],\"i\":[]}}\n"
          + "{\"t\":\"v\",\"c\":{\"p\":{\"id\":2,\"bestFriend\":\"#6:5\"},\"r\":\"#6:7\",\"t\":\"Person\",\"o\":[],\"i\":[]}}\n"
          + "{\"t\":\"v\",\"c\":{\"p\":{\"id\":3,\"friends\":[\"#6:9\"]},\"r\":\"#6:8\",\"t\":\"Person\",\"o\":[],\"i\":[]}}\n"
          + "{\"t\":\"v\",\"c\":{\"p\":{\"id\":4},\"r\":\"#6:9\",\"t\":\"Person\",\"o\":[],\"i\":[]}}\n");

      var importer = new Importer(
          ("-url " + jsonlFile.toAbsolutePath() + " -database " + DATABASE_PATH + " -forceDatabaseCreate true").split(" "));
      Map<String, Object> result = importer.load();

      assertThat(result).containsEntry("createdVertices", 4L);
      assertThat(result).doesNotContainKey("errors");

      try (var db = new DatabaseFactory(DATABASE_PATH).open()) {
        assertThat(db.countType("Person", true)).isEqualTo(4L);

        final Map<Integer, Document> byId = new HashMap<>();
        for (final Iterator<Record> it = db.iterateType("Person", true); it.hasNext(); ) {
          final Document doc = it.next().asDocument(true);
          byId.put((Integer) doc.get("id"), doc);
        }

        final RID person1Rid = byId.get(1).getIdentity();
        final RID person4Rid = byId.get(4).getIdentity();

        // None of the freshly assigned RIDs coincide with the source "r" values used above - proof that a still-wrong
        // (unremapped) value in either assertion below could not pass by accident.
        assertThat(person1Rid.toString()).isNotIn("#6:5", "#6:7", "#6:8", "#6:9");
        assertThat(person4Rid.toString()).isNotIn("#6:5", "#6:7", "#6:8", "#6:9");

        // Backward reference: bestFriend must point at Person#1's NEW identity, not at the source RID "#6:5".
        assertThat(byId.get(2).get("bestFriend")).isEqualTo(person1Rid);

        // Forward reference, fixed up by the reconciliation pass: friends must point at Person#4's NEW identity,
        // not at the source RID "#6:9".
        final List<?> friends = (List<?>) byId.get(3).get("friends");
        assertThat(friends).hasSize(1);
        assertThat(friends.get(0)).isEqualTo(person4Rid);
      }
    } finally {
      Files.deleteIfExists(jsonlFile);
    }
  }

  @Test
  void importDatabaseRemapsMapOfLinkPropertyValues() throws Exception {
    // Issue #6460 follow-up (review on PR #6654): MAP-of-LINK is implemented by the same remapLinkProperties()/
    // reconcileUnresolvedLinks() code path as LIST-of-LINK, but was not exercised by any test. Same fixture shape
    // as importDatabaseRemapsLinkTypedPropertyValues: a backward reference resolved on first pass and a forward
    // reference fixed up only by the reconciliation pass.
    Path jsonlFile = Files.createTempFile("arcadedb-map-link-remap-import-", ".jsonl");
    try {
      Files.writeString(jsonlFile, ""
          + "{\"t\":\"info\",\"c\":{\"description\":\"test\",\"exporterVersion\":1,\"dbVersion\":\"25.1.1-SNAPSHOT\",\"dbBuild\":\"\",\"dbTimestamp\":\"\"}}\n"
          + "{\"t\":\"db\",\"c\":{\"name\":\"test\",\"executedOn\":\"2025-01-01\",\"executedOnTimestamp\":0}}\n"
          + "{\"t\":\"schema\",\"c\":{\"schemaVersion\":1,\"dbmsVersion\":\"25.1.1-SNAPSHOT\",\"dbmsBuild\":\"\",\"settings\":{\"zoneId\":\"UTC\",\"dateFormat\":\"yyyy-MM-dd\",\"dateTimeFormat\":\"yyyy-MM-dd HH:mm:ss\"},"
          + "\"types\":{\"Person\":{\"type\":\"v\",\"parents\":[],\"buckets\":[\"Person_0\"],\"properties\":{"
          + "\"id\":{\"type\":\"INTEGER\",\"custom\":{}},"
          + "\"relations\":{\"type\":\"MAP\",\"of\":\"LINK\",\"custom\":{}}"
          + "},\"indexes\":{},\"custom\":{}}}}}\n"
          + "{\"t\":\"v\",\"c\":{\"p\":{\"id\":1},\"r\":\"#6:5\",\"t\":\"Person\",\"o\":[],\"i\":[]}}\n"
          + "{\"t\":\"v\",\"c\":{\"p\":{\"id\":2,\"relations\":{\"parent\":\"#6:5\"}},\"r\":\"#6:7\",\"t\":\"Person\",\"o\":[],\"i\":[]}}\n"
          + "{\"t\":\"v\",\"c\":{\"p\":{\"id\":3,\"relations\":{\"child\":\"#6:9\"}},\"r\":\"#6:8\",\"t\":\"Person\",\"o\":[],\"i\":[]}}\n"
          + "{\"t\":\"v\",\"c\":{\"p\":{\"id\":4},\"r\":\"#6:9\",\"t\":\"Person\",\"o\":[],\"i\":[]}}\n");

      var importer = new Importer(
          ("-url " + jsonlFile.toAbsolutePath() + " -database " + DATABASE_PATH + " -forceDatabaseCreate true").split(" "));
      Map<String, Object> result = importer.load();

      assertThat(result).containsEntry("createdVertices", 4L);
      assertThat(result).doesNotContainKey("errors");

      try (var db = new DatabaseFactory(DATABASE_PATH).open()) {
        final Map<Integer, Document> byId = new HashMap<>();
        for (final Iterator<Record> it = db.iterateType("Person", true); it.hasNext(); ) {
          final Document doc = it.next().asDocument(true);
          byId.put((Integer) doc.get("id"), doc);
        }

        final RID person1Rid = byId.get(1).getIdentity();
        final RID person4Rid = byId.get(4).getIdentity();

        // Backward reference: resolved on first pass.
        final Map<?, ?> person2Relations = (Map<?, ?>) byId.get(2).get("relations");
        assertThat(person2Relations.get("parent")).isEqualTo(person1Rid);

        // Forward reference: fixed up only by the reconciliation pass.
        final Map<?, ?> person3Relations = (Map<?, ?>) byId.get(3).get("relations");
        assertThat(person3Relations.get("child")).isEqualTo(person4Rid);
      }
    } finally {
      Files.deleteIfExists(jsonlFile);
    }
  }

  @Test
  void importDatabaseLeavesNeverResolvedLinkUnchanged() throws Exception {
    // Issue #6460 follow-up (review on PR #6654): a LINK value that references a record never present in the
    // source stream at all (excluded via -includeTypes/-excludeTypes, or a genuinely dangling link) must be left
    // as-is - matching pre-fix behavior for that case - and must NOT be counted as an import error.
    Path jsonlFile = Files.createTempFile("arcadedb-link-never-resolves-import-", ".jsonl");
    try {
      Files.writeString(jsonlFile, ""
          + "{\"t\":\"info\",\"c\":{\"description\":\"test\",\"exporterVersion\":1,\"dbVersion\":\"25.1.1-SNAPSHOT\",\"dbBuild\":\"\",\"dbTimestamp\":\"\"}}\n"
          + "{\"t\":\"db\",\"c\":{\"name\":\"test\",\"executedOn\":\"2025-01-01\",\"executedOnTimestamp\":0}}\n"
          + "{\"t\":\"schema\",\"c\":{\"schemaVersion\":1,\"dbmsVersion\":\"25.1.1-SNAPSHOT\",\"dbmsBuild\":\"\",\"settings\":{\"zoneId\":\"UTC\",\"dateFormat\":\"yyyy-MM-dd\",\"dateTimeFormat\":\"yyyy-MM-dd HH:mm:ss\"},"
          + "\"types\":{\"Person\":{\"type\":\"v\",\"parents\":[],\"buckets\":[\"Person_0\"],\"properties\":{"
          + "\"id\":{\"type\":\"INTEGER\",\"custom\":{}},"
          + "\"bestFriend\":{\"type\":\"LINK\",\"custom\":{}}"
          + "},\"indexes\":{},\"custom\":{}}}}}\n"
          + "{\"t\":\"v\",\"c\":{\"p\":{\"id\":1,\"bestFriend\":\"#6:99\"},\"r\":\"#6:5\",\"t\":\"Person\",\"o\":[],\"i\":[]}}\n");

      var importer = new Importer(
          ("-url " + jsonlFile.toAbsolutePath() + " -database " + DATABASE_PATH + " -forceDatabaseCreate true").split(" "));
      Map<String, Object> result = importer.load();

      assertThat(result).containsEntry("createdVertices", 1L);
      assertThat(result).doesNotContainKey("errors");

      try (var db = new DatabaseFactory(DATABASE_PATH).open()) {
        final Document person1 = db.iterateType("Person", true).next().asDocument(true);
        // Never resolved: left as the original (now unreachable) source RID, exactly like pre-fix behavior.
        assertThat(person1.get("bestFriend")).isEqualTo(new RID("#6:99"));
      }
    } finally {
      Files.deleteIfExists(jsonlFile);
    }
  }

  @Test
  void importDatabaseRemapsLinkPropertyOnEdge() throws Exception {
    // Issue #6460 follow-up (review on PR #6654): loadVertex/loadDocument were both covered, but loadEdge wires
    // pendingLinkReconciliation through a different call site with the same shape - a LINK-typed property on a
    // (non-lightweight) edge must be remapped too, including the forward-reference case fixed up only by the
    // reconciliation pass.
    Path jsonlFile = Files.createTempFile("arcadedb-edge-link-remap-import-", ".jsonl");
    try {
      Files.writeString(jsonlFile, ""
          + "{\"t\":\"info\",\"c\":{\"description\":\"test\",\"exporterVersion\":1,\"dbVersion\":\"25.1.1-SNAPSHOT\",\"dbBuild\":\"\",\"dbTimestamp\":\"\"}}\n"
          + "{\"t\":\"db\",\"c\":{\"name\":\"test\",\"executedOn\":\"2025-01-01\",\"executedOnTimestamp\":0}}\n"
          + "{\"t\":\"schema\",\"c\":{\"schemaVersion\":1,\"dbmsVersion\":\"25.1.1-SNAPSHOT\",\"dbmsBuild\":\"\",\"settings\":{\"zoneId\":\"UTC\",\"dateFormat\":\"yyyy-MM-dd\",\"dateTimeFormat\":\"yyyy-MM-dd HH:mm:ss\"},"
          + "\"types\":{"
          + "\"Person\":{\"type\":\"v\",\"parents\":[],\"buckets\":[\"Person_0\"],\"properties\":{\"id\":{\"type\":\"INTEGER\",\"custom\":{}}},\"indexes\":{},\"custom\":{}},"
          + "\"Knows\":{\"type\":\"e\",\"parents\":[],\"buckets\":[\"Knows_0\"],\"properties\":{\"referrer\":{\"type\":\"LINK\",\"custom\":{}}},\"indexes\":{},\"custom\":{}}"
          + "}}}\n"
          + "{\"t\":\"v\",\"c\":{\"p\":{\"id\":1},\"r\":\"#6:5\",\"t\":\"Person\",\"o\":[],\"i\":[]}}\n"
          + "{\"t\":\"v\",\"c\":{\"p\":{\"id\":2},\"r\":\"#6:6\",\"t\":\"Person\",\"o\":[],\"i\":[]}}\n"
          + "{\"t\":\"e\",\"c\":{\"p\":{\"referrer\":\"#6:7\"},\"t\":\"Knows\",\"o\":\"#6:5\",\"i\":\"#6:6\"}}\n"
          + "{\"t\":\"v\",\"c\":{\"p\":{\"id\":3},\"r\":\"#6:7\",\"t\":\"Person\",\"o\":[],\"i\":[]}}\n");

      var importer = new Importer(
          ("-url " + jsonlFile.toAbsolutePath() + " -database " + DATABASE_PATH + " -forceDatabaseCreate true").split(" "));
      Map<String, Object> result = importer.load();

      assertThat(result).containsEntry("createdVertices", 3L);
      assertThat(result).containsEntry("createdEdges", 1L);
      assertThat(result).doesNotContainKey("errors");

      try (var db = new DatabaseFactory(DATABASE_PATH).open()) {
        Document person3 = null;
        for (final Iterator<Record> it = db.iterateType("Person", true); it.hasNext(); ) {
          final Document doc = it.next().asDocument(true);
          if (Integer.valueOf(3).equals(doc.get("id")))
            person3 = doc;
        }
        assertThat(person3).isNotNull();

        final var knowsEdge = db.iterateType("Knows", true).next().asEdge(true);
        // Forward reference (Person#3 imported after the edge): fixed up only by the reconciliation pass.
        assertThat(knowsEdge.get("referrer")).isEqualTo(person3.getIdentity());
      }
    } finally {
      Files.deleteIfExists(jsonlFile);
    }
  }

  /**
   * Issue #6460 follow-up (#6795, comment on the closed issue): {@code reconcileUnresolvedLinks} used to remap
   * EVERY RID element of a mixed-resolution LIST-of-LINK, not only the ones pass 1 actually left unresolved. On a
   * normal same-schema restore the source and target RID spaces overlap, so an already-correct element can equal
   * some OTHER record's source RID and get silently re-pointed a second time.
   * <p>
   * Person#1 (source "#1:20") is imported first and always lands at the fresh bucket's first position "#1:0" (a
   * database created for this schema - a single Person type and no other user type - always allocates its first
   * user bucket as bucket 1). Person#3's source "r" is deliberately set to that exact value "#1:0" - a coincidence
   * that is completely ordinary in a same-schema restore, since the source and target bucket sequences both start
   * counting from 0. Person#2.friends mixes a backward reference to Person#1 (resolved immediately on pass 1, to
   * "#1:0") with a forward reference to Person#4 (source "#1:99", unresolved until the reconciliation pass) in the
   * SAME list. The bug re-applies {@code ridIndex.get(...)} to the already-resolved "#1:0" element during
   * reconciliation, finds Person#3's mapping under that same key purely by coincidence, and silently replaces
   * Person#1's identity with Person#3's.
   */
  @Test
  void importDatabaseReconcileDoesNotRemapAlreadyResolvedListElement() throws Exception {
    Path jsonlFile = Files.createTempFile("arcadedb-6795-reconcile-overlap-", ".jsonl");
    try {
      Files.writeString(jsonlFile, ""
          + "{\"t\":\"info\",\"c\":{\"description\":\"test\",\"exporterVersion\":1,\"dbVersion\":\"25.1.1-SNAPSHOT\",\"dbBuild\":\"\",\"dbTimestamp\":\"\"}}\n"
          + "{\"t\":\"db\",\"c\":{\"name\":\"test\",\"executedOn\":\"2025-01-01\",\"executedOnTimestamp\":0}}\n"
          + "{\"t\":\"schema\",\"c\":{\"schemaVersion\":1,\"dbmsVersion\":\"25.1.1-SNAPSHOT\",\"dbmsBuild\":\"\",\"settings\":{\"zoneId\":\"UTC\",\"dateFormat\":\"yyyy-MM-dd\",\"dateTimeFormat\":\"yyyy-MM-dd HH:mm:ss\"},"
          + "\"types\":{\"Person\":{\"type\":\"v\",\"parents\":[],\"buckets\":[\"Person_0\"],\"properties\":{"
          + "\"id\":{\"type\":\"INTEGER\",\"custom\":{}},"
          + "\"friends\":{\"type\":\"LIST\",\"of\":\"LINK\",\"custom\":{}}"
          + "},\"indexes\":{},\"custom\":{}}}}}\n"
          + "{\"t\":\"v\",\"c\":{\"p\":{\"id\":1},\"r\":\"#1:20\",\"t\":\"Person\",\"o\":[],\"i\":[]}}\n"
          + "{\"t\":\"v\",\"c\":{\"p\":{\"id\":2,\"friends\":[\"#1:20\",\"#1:99\"]},\"r\":\"#1:21\",\"t\":\"Person\",\"o\":[],\"i\":[]}}\n"
          + "{\"t\":\"v\",\"c\":{\"p\":{\"id\":3},\"r\":\"#1:0\",\"t\":\"Person\",\"o\":[],\"i\":[]}}\n"
          + "{\"t\":\"v\",\"c\":{\"p\":{\"id\":4},\"r\":\"#1:99\",\"t\":\"Person\",\"o\":[],\"i\":[]}}\n");

      var importer = new Importer(
          ("-url " + jsonlFile.toAbsolutePath() + " -database " + DATABASE_PATH + " -forceDatabaseCreate true").split(" "));
      Map<String, Object> result = importer.load();

      assertThat(result).containsEntry("createdVertices", 4L);
      assertThat(result).doesNotContainKey("errors");

      try (var db = new DatabaseFactory(DATABASE_PATH).open()) {
        final Map<Integer, Document> byId = new HashMap<>();
        for (final Iterator<Record> it = db.iterateType("Person", true); it.hasNext(); ) {
          final Document doc = it.next().asDocument(true);
          byId.put((Integer) doc.get("id"), doc);
        }

        final RID person1Rid = byId.get(1).getIdentity();
        final RID person3Rid = byId.get(3).getIdentity();
        final RID person4Rid = byId.get(4).getIdentity();

        // Person#1 always lands at the fresh bucket's first position - the coincidence this test relies on.
        assertThat(person1Rid.toString()).isEqualTo("#1:0");

        final List<?> friends = (List<?>) byId.get(2).get("friends");
        assertThat(friends).hasSize(2);
        // Already resolved on pass 1 (backward reference): must stay Person#1's identity, not be silently
        // re-pointed at Person#3 just because Person#3's SOURCE rid happens to equal Person#1's TARGET rid.
        assertThat(friends.get(0)).isEqualTo(person1Rid);
        assertThat(friends.get(0)).isNotEqualTo(person3Rid);
        // Genuinely unresolved on pass 1 (forward reference): must still be fixed up by reconciliation.
        assertThat(friends.get(1)).isEqualTo(person4Rid);
      }
    } finally {
      Files.deleteIfExists(jsonlFile);
    }
  }

  /**
   * Issue #6561: {@code -onRowError skip} commits/rolls back per record, so - exactly like CSV/JSON (see
   * {@code Issue5968ImporterSkipOnRowErrorTest}) - it must own the transaction outright and reject an already-active
   * caller-managed transaction eagerly, rather than silently committing/discarding whatever the caller had pending
   * on the first record.
   */
  @Test
  void importDatabaseSkipModeRejectsInsideActiveTransaction() throws Exception {
    Path jsonlFile = Files.createTempFile("arcadedb-6561-skip-active-tx-", ".jsonl");
    try {
      Files.writeString(jsonlFile, singleVertexJsonl());

      var db = new DatabaseFactory(DATABASE_PATH).create();
      try {
        db.command("sql", "CREATE DOCUMENT TYPE CallerWork");

        // The caller starts their own transaction with unrelated pending work before handing the Database to the
        // importer, exactly like the CSV/JSON counterparts in Issue5968ImporterSkipOnRowErrorTest.
        db.begin();
        db.newDocument("CallerWork").set("name", "pre-existing").save();

        var importer = new Importer(db, jsonlFile.toAbsolutePath().toString());
        importer.settings.onRowError = "skip";

        assertThatThrownBy(importer::load).isInstanceOf(ImportException.class)
            .hasRootCauseInstanceOf(IllegalStateException.class);

        // The guard must fire before touching the database at all: the caller's own transaction and its pending
        // work must still be there for them to decide what to do with.
        assertThat(db.isTransactionActive()).isTrue();
        db.commit();
        assertThat(db.countType("CallerWork", true)).isEqualTo(1);
      } finally {
        db.drop();
      }
    } finally {
      Files.deleteIfExists(jsonlFile);
    }
  }

  /**
   * Issue #6561: default "abort" mode has no exclusive-transaction-ownership guard (only "skip" mode rejects an
   * already-active caller transaction, see {@link #importDatabaseSkipModeRejectsInsideActiveTransaction()}), so it
   * must run directly inside a caller's own transaction without ever committing it - on success, the transaction
   * must be left open for the caller to commit themselves, exactly like {@code CSVImporterFormat}/
   * {@code JSONImporterFormat} (see {@code csvDocumentImportOnAllSuccessLeavesCallersExternallyManagedTransactionOpenForThemToCommit}
   * in {@code Issue5968ImporterSkipOnRowErrorTest}).
   */
  @Test
  void importDatabaseAbortModeInsideActiveTransactionLeavesTransactionOpenOnSuccess() throws Exception {
    Path jsonlFile = Files.createTempFile("arcadedb-6561-abort-success-active-tx-", ".jsonl");
    try {
      Files.writeString(jsonlFile, singleVertexJsonl());

      var db = new DatabaseFactory(DATABASE_PATH).create();
      try {
        db.command("sql", "CREATE DOCUMENT TYPE CallerWork");

        db.begin();
        db.newDocument("CallerWork").set("name", "pre-existing").save();

        var importer = new Importer(db, jsonlFile.toAbsolutePath().toString());
        Map<String, Object> result = importer.load();
        assertThat(result).doesNotContainKey("errors");

        // The import succeeded, but the transaction was never ours to commit: it must still be active, with the
        // caller's own pre-existing work not yet durable, for the caller to commit (or roll back) themselves.
        assertThat(db.isTransactionActive()).isTrue();
        db.commit();

        assertThat(db.countType("CallerWork", true)).isEqualTo(1);
        assertThat(db.countType("Person", true)).isEqualTo(1);
      } finally {
        db.drop();
      }
    } finally {
      Files.deleteIfExists(jsonlFile);
    }
  }

  /**
   * Issue #6561, symmetric to {@link #importDatabaseAbortModeInsideActiveTransactionLeavesTransactionOpenOnSuccess()}
   * but for the failure path: a failing record must still abort the import, but must not discard the caller's own
   * pending work by committing OR rolling back a transaction this import never owned - it must be left exactly as
   * the caller left it, active, for them alone to decide what happens to it next.
   */
  @Test
  void importDatabaseAbortModeInsideActiveTransactionDoesNotCommitOrRollbackOnFailure() throws Exception {
    Path jsonlFile = Files.createTempFile("arcadedb-6561-abort-failure-active-tx-", ".jsonl");
    try {
      Files.writeString(jsonlFile, ""
          + "{\"t\":\"info\",\"c\":{\"description\":\"test\",\"exporterVersion\":1,\"dbVersion\":\"25.1.1-SNAPSHOT\",\"dbBuild\":\"\",\"dbTimestamp\":\"\"}}\n"
          + "{\"t\":\"db\",\"c\":{\"name\":\"test\",\"executedOn\":\"2025-01-01\",\"executedOnTimestamp\":0}}\n"
          + "{\"t\":\"schema\",\"c\":{\"schemaVersion\":1,\"dbmsVersion\":\"25.1.1-SNAPSHOT\",\"dbmsBuild\":\"\",\"settings\":{\"zoneId\":\"UTC\",\"dateFormat\":\"yyyy-MM-dd\",\"dateTimeFormat\":\"yyyy-MM-dd HH:mm:ss\"},\"types\":{\"Person\":{\"type\":\"v\",\"parents\":[],\"buckets\":[\"Person_0\"],\"properties\":{\"id\":{\"type\":\"INTEGER\",\"custom\":{}}},\"indexes\":{},\"custom\":{}}}}}\n"
          + "{\"t\":\"v\",\"c\":{\"p\":{\"id\":1},\"r\":\"#6:0\",\"t\":\"Person\",\"o\":[],\"i\":[]}}\n"
          + "{\"t\":\"v\",\"c\":{\"p\":{\"id\":2},\"r\":\"#6:1\",\"t\":\"NonExistentType\",\"o\":[],\"i\":[]}}\n");

      var db = new DatabaseFactory(DATABASE_PATH).create();
      try {
        db.command("sql", "CREATE DOCUMENT TYPE CallerWork");

        db.begin();
        db.newDocument("CallerWork").set("name", "pre-existing").save();

        var importer = new Importer(db, jsonlFile.toAbsolutePath().toString());

        assertThatThrownBy(importer::load).isInstanceOf(ImportException.class);

        // The import failed, but the caller's own transaction - and therefore their pre-existing pending work -
        // must still be there for them to decide what to do with: neither committed nor rolled back out from under
        // them by an import that never owned this transaction.
        assertThat(db.isTransactionActive()).isTrue();
        db.commit();

        assertThat(db.countType("CallerWork", true)).isEqualTo(1);
      } finally {
        db.drop();
      }
    } finally {
      Files.deleteIfExists(jsonlFile);
    }
  }

  /**
   * Issue #6561: the periodic {@code commit()}/{@code begin()} every 1000 records in default "abort" mode must not
   * run at all while inside a caller-managed transaction - it must not silently commit the caller's transaction (and
   * whatever unrelated work it holds) out from under them partway through a large import, replacing it with a fresh
   * one the caller never asked for.
   */
  @Test
  void importDatabaseAbortModeInsideActiveTransactionDoesNotCommitPeriodicallyOnLargeImport() throws Exception {
    Path jsonlFile = Files.createTempFile("arcadedb-6561-abort-periodic-active-tx-", ".jsonl");
    try {
      final int vertexCount = 1500; // exceeds the 1000-record periodic commit threshold
      Files.writeString(jsonlFile, manyVerticesJsonl(vertexCount));

      var db = new DatabaseFactory(DATABASE_PATH).create();
      try {
        db.command("sql", "CREATE DOCUMENT TYPE CallerWork");

        db.begin();
        db.newDocument("CallerWork").set("name", "pre-existing").save();

        var importer = new Importer(db, jsonlFile.toAbsolutePath().toString());
        Map<String, Object> result = importer.load();
        assertThat(result).doesNotContainKey("errors");
        assertThat(result).containsEntry("createdVertices", (long) vertexCount);

        // If the periodic commit had fired (unguarded), the caller's transaction would have been replaced partway
        // through - still "active" by the time load() returns, but a DIFFERENT, freshly-begun one that no longer
        // holds the caller's own pre-existing pending work. Asserting the pending work is still there proves the
        // ORIGINAL transaction survived intact, not just that some transaction happens to be active.
        assertThat(db.isTransactionActive()).isTrue();
        db.commit();

        assertThat(db.countType("CallerWork", true)).isEqualTo(1);
        assertThat(db.countType("Person", true)).isEqualTo(vertexCount);
      } finally {
        db.drop();
      }
    } finally {
      Files.deleteIfExists(jsonlFile);
    }
  }

  /**
   * Issue #6561 review follow-up: symmetric to
   * {@link #importDatabaseAbortModeInsideActiveTransactionDoesNotCommitPeriodicallyOnLargeImport()} but for
   * {@code reconcileUnresolvedLinks()}'s own periodic commit rather than the main loop's - the large-import fixture
   * used there carries no LINK properties, so its reconciliation pass is a no-op and never exercises this gate.
   * Every vertex here (bar the last) has a forward-referencing {@code next} LINK property pointing at the vertex
   * imported immediately after it, which is guaranteed unresolved on first pass (the referenced record hasn't been
   * imported yet) and so lands in {@code pendingLinkReconciliation}, exercising the reconciliation pass's own
   * periodic commit at the same >1000-entry scale.
   */
  @Test
  void importDatabaseAbortModeInsideActiveTransactionDoesNotCommitPeriodicallyDuringLinkReconciliation() throws Exception {
    Path jsonlFile = Files.createTempFile("arcadedb-6561-abort-periodic-reconcile-active-tx-", ".jsonl");
    try {
      final int vertexCount = 1500; // exceeds the 1000-record periodic commit threshold
      Files.writeString(jsonlFile, manyVerticesWithForwardLinksJsonl(vertexCount));

      var db = new DatabaseFactory(DATABASE_PATH).create();
      try {
        db.command("sql", "CREATE DOCUMENT TYPE CallerWork");

        db.begin();
        db.newDocument("CallerWork").set("name", "pre-existing").save();

        var importer = new Importer(db, jsonlFile.toAbsolutePath().toString());
        Map<String, Object> result = importer.load();
        assertThat(result).doesNotContainKey("errors");
        assertThat(result).containsEntry("createdVertices", (long) vertexCount);

        // Same reasoning as importDatabaseAbortModeInsideActiveTransactionDoesNotCommitPeriodicallyOnLargeImport:
        // asserting the caller's own pre-existing pending work is still there proves the ORIGINAL transaction
        // survived the reconciliation pass intact, not just that some transaction happens to be active.
        assertThat(db.isTransactionActive()).isTrue();
        db.commit();

        assertThat(db.countType("CallerWork", true)).isEqualTo(1);
        assertThat(db.countType("Person", true)).isEqualTo(vertexCount);
      } finally {
        db.drop();
      }
    } finally {
      Files.deleteIfExists(jsonlFile);
    }
  }

  private static String singleVertexJsonl() {
    return ""
        + "{\"t\":\"info\",\"c\":{\"description\":\"test\",\"exporterVersion\":1,\"dbVersion\":\"25.1.1-SNAPSHOT\",\"dbBuild\":\"\",\"dbTimestamp\":\"\"}}\n"
        + "{\"t\":\"db\",\"c\":{\"name\":\"test\",\"executedOn\":\"2025-01-01\",\"executedOnTimestamp\":0}}\n"
        + "{\"t\":\"schema\",\"c\":{\"schemaVersion\":1,\"dbmsVersion\":\"25.1.1-SNAPSHOT\",\"dbmsBuild\":\"\",\"settings\":{\"zoneId\":\"UTC\",\"dateFormat\":\"yyyy-MM-dd\",\"dateTimeFormat\":\"yyyy-MM-dd HH:mm:ss\"},\"types\":{\"Person\":{\"type\":\"v\",\"parents\":[],\"buckets\":[\"Person_0\"],\"properties\":{\"id\":{\"type\":\"INTEGER\",\"custom\":{}}},\"indexes\":{},\"custom\":{}}}}}\n"
        + "{\"t\":\"v\",\"c\":{\"p\":{\"id\":1},\"r\":\"#6:0\",\"t\":\"Person\",\"o\":[],\"i\":[]}}\n";
  }

  private static String manyVerticesJsonl(final int vertexCount) {
    final StringBuilder sb = new StringBuilder();
    sb.append(
        "{\"t\":\"info\",\"c\":{\"description\":\"test\",\"exporterVersion\":1,\"dbVersion\":\"25.1.1-SNAPSHOT\",\"dbBuild\":\"\",\"dbTimestamp\":\"\"}}\n");
    sb.append("{\"t\":\"db\",\"c\":{\"name\":\"test\",\"executedOn\":\"2025-01-01\",\"executedOnTimestamp\":0}}\n");
    sb.append(
        "{\"t\":\"schema\",\"c\":{\"schemaVersion\":1,\"dbmsVersion\":\"25.1.1-SNAPSHOT\",\"dbmsBuild\":\"\",\"settings\":{\"zoneId\":\"UTC\",\"dateFormat\":\"yyyy-MM-dd\",\"dateTimeFormat\":\"yyyy-MM-dd HH:mm:ss\"},\"types\":{\"Person\":{\"type\":\"v\",\"parents\":[],\"buckets\":[\"Person_0\"],\"properties\":{\"id\":{\"type\":\"INTEGER\",\"custom\":{}}},\"indexes\":{},\"custom\":{}}}}}\n");
    for (int i = 0; i < vertexCount; ++i)
      sb.append("{\"t\":\"v\",\"c\":{\"p\":{\"id\":").append(i).append("},\"r\":\"#6:").append(i)
          .append("\",\"t\":\"Person\",\"o\":[],\"i\":[]}}\n");
    return sb.toString();
  }

  private static String manyVerticesWithForwardLinksJsonl(final int vertexCount) {
    final StringBuilder sb = new StringBuilder();
    sb.append(
        "{\"t\":\"info\",\"c\":{\"description\":\"test\",\"exporterVersion\":1,\"dbVersion\":\"25.1.1-SNAPSHOT\",\"dbBuild\":\"\",\"dbTimestamp\":\"\"}}\n");
    sb.append("{\"t\":\"db\",\"c\":{\"name\":\"test\",\"executedOn\":\"2025-01-01\",\"executedOnTimestamp\":0}}\n");
    sb.append(
        "{\"t\":\"schema\",\"c\":{\"schemaVersion\":1,\"dbmsVersion\":\"25.1.1-SNAPSHOT\",\"dbmsBuild\":\"\",\"settings\":{\"zoneId\":\"UTC\",\"dateFormat\":\"yyyy-MM-dd\",\"dateTimeFormat\":\"yyyy-MM-dd HH:mm:ss\"},"
            + "\"types\":{\"Person\":{\"type\":\"v\",\"parents\":[],\"buckets\":[\"Person_0\"],\"properties\":{"
            + "\"id\":{\"type\":\"INTEGER\",\"custom\":{}},"
            + "\"next\":{\"type\":\"LINK\",\"custom\":{}}"
            + "},\"indexes\":{},\"custom\":{}}}}}\n");
    for (int i = 0; i < vertexCount; ++i) {
      // Every vertex but the last points forward at the NEXT one in the stream (r="#6:(i+1)"), which hasn't been
      // imported yet at the point this line is processed - guaranteed unresolved on first pass, so it lands in
      // pendingLinkReconciliation for reconcileUnresolvedLinks() to fix up afterwards.
      final String next = i < vertexCount - 1 ? ",\"next\":\"#6:" + (i + 1) + "\"" : "";
      sb.append("{\"t\":\"v\",\"c\":{\"p\":{\"id\":").append(i).append(next).append("},\"r\":\"#6:").append(i)
          .append("\",\"t\":\"Person\",\"o\":[],\"i\":[]}}\n");
    }
    return sb.toString();
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
