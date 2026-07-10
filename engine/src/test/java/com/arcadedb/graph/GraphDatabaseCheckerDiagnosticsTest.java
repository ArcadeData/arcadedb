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
package com.arcadedb.graph;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.engine.DatabaseChecker;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers the CHECK DATABASE diagnostic improvements:
 * 1. an exception with a null message is reported by its class name instead of an undiagnosable "error: null";
 * 2. the millions-of-edges fan-out onto a single missing target collapses into one per-target summary line;
 * 3. the distinct set of missing targets is surfaced explicitly.
 */
class GraphDatabaseCheckerDiagnosticsTest {

  @Test
  void describeFallsBackToClassNameWhenMessageNull() {
    // A NullPointerException with no explicit message is exactly the prod case behind "error: null".
    assertThat(GraphDatabaseChecker.describe(new NullPointerException())).isEqualTo("NullPointerException");
    // When a message is present it is preserved verbatim.
    assertThat(GraphDatabaseChecker.describe(new RuntimeException("boom"))).isEqualTo("boom");
  }

  @Test
  void fanOutOntoSingleMissingTargetCollapsesToOneSummaryLine() {
    final String dbPath = "./target/databases/GraphCheckerDiagnostics";
    final DatabaseFactory factory = new DatabaseFactory(dbPath);
    if (factory.exists())
      factory.open().drop();

    final Database db = factory.create();
    try {
      db.getSchema().createVertexType("Node");
      db.getSchema().createEdgeType("Link");

      // One shared target referenced by many sources: the supernode pattern from Johan's report (#28:1).
      final int sources = 50;
      final MutableVertex[] target = new MutableVertex[1];
      db.transaction(() -> target[0] = db.newVertex("Node").set("name", "target").save());

      db.transaction(() -> {
        for (int i = 0; i < sources; i++) {
          final MutableVertex src = db.newVertex("Node").set("i", i).save();
          src.newEdge("Link", target[0]);
        }
      });

      final RID targetRid = target[0].getIdentity();

      // Delete the single shared target at low level, leaving every edge dangling on its incoming side.
      db.transaction(() -> db.getSchema().getBucketById(targetRid.getBucketId()).deleteRecord(targetRid));

      final Map<String, Object> result = new DatabaseChecker(db)
          .setVerboseLevel(0)
          .check();

      // Exactly one distinct missing target despite the many dangling edges.
      assertThat((Long) result.get("distinctMissingReferences")).isEqualTo(1L);

      final List<String> top = (List<String>) result.get("topMissingReferences");
      assertThat(top).hasSize(1);

      final String summary = top.getFirst();
      assertThat(summary).contains(targetRid.toString());
      assertThat(summary).contains("referenced by");
      assertThat(summary).contains("edge(s)");

      // The raw per-edge warnings are still produced (capped), so detail is not lost.
      assertThat((Long) result.get("totalWarnings")).isGreaterThan(0L);
    } finally {
      db.drop();
    }
  }

  @Test
  void noMissingReferencesOnCleanDatabase() {
    final String dbPath = "./target/databases/GraphCheckerDiagnosticsClean";
    final DatabaseFactory factory = new DatabaseFactory(dbPath);
    if (factory.exists())
      factory.open().drop();

    final Database db = factory.create();
    try {
      db.getSchema().createVertexType("Node");
      db.getSchema().createEdgeType("Link");

      final MutableVertex[] hub = new MutableVertex[1];
      db.transaction(() -> hub[0] = db.newVertex("Node").save());
      db.transaction(() -> {
        for (int i = 0; i < 5; i++)
          hub[0].newEdge("Link", db.newVertex("Node").set("i", i).save());
      });

      final Map<String, Object> result = new DatabaseChecker(db).setVerboseLevel(0).check();

      assertThat((Long) result.get("distinctMissingReferences")).isEqualTo(0L);
      assertThat((List<String>) result.get("topMissingReferences")).isEmpty();
    } finally {
      db.drop();
    }
  }
}
