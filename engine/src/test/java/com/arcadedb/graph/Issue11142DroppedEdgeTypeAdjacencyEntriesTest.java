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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * https://github.com/ArcadeData/arcadedb/issues/11142
 * <p>
 * An edge type dropped (UNSAFE, or by a version before the #10923 guard) while lightweight edges were still in the
 * vertices' adjacency lists leaves entries naming a bucket id the schema no longer resolves. CHECK DATABASE must report
 * them and CHECK DATABASE FIX must prune them, so the vertices can be deleted again.
 */
class Issue11142DroppedEdgeTypeAdjacencyEntriesTest extends TestHelper {

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // The stranded entries are injected on purpose.
    return false;
  }

  private MutableVertex[] strandEntries(final String edgeDdl, final String edgeType) {
    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", edgeDdl);
    final MutableVertex[] v = new MutableVertex[2];
    database.transaction(() -> {
      v[0] = database.newVertex("V").save();
      v[1] = database.newVertex("V").save();
      v[0].newLightEdge(edgeType, v[1]);
    });
    database.command("sql", "DROP TYPE " + edgeType + " UNSAFE");
    return v;
  }

  private Map<String, Object> check(final boolean fix) {
    return new GraphDatabaseChecker((DatabaseInternal) database).checkVertices("V", fix, 0);
  }

  @Test
  void checkReportsAndFixPrunesEntriesOfADroppedEdgeType() {
    final MutableVertex[] v = strandEntries("CREATE EDGE TYPE EL LIGHTWEIGHT", "EL");

    final Map<String, Object> report = check(false);
    assertThat(((Number) report.get("invalidLinks")).longValue()).as("report: %s", report).isGreaterThan(0L);

    final Map<String, Object> fixed = check(true);
    assertThat(((Number) fixed.get("prunedDanglingEntries")).longValue()).as("report: %s", fixed).isGreaterThan(0L);

    final Map<String, Object> after = check(false);
    assertThat(((Number) after.get("invalidLinks")).longValue()).as("report: %s", after).isZero();

    database.transaction(() -> database.lookupByRID(v[0].getIdentity(), true).asVertex().delete());
    database.transaction(() -> database.lookupByRID(v[1].getIdentity(), true).asVertex().delete());
    assertThat(database.countType("V", false)).isZero();
  }

  @Test
  void sqlCheckDatabaseFixPrunesUnidirectionalEntries() {
    final MutableVertex[] v = strandEntries("CREATE EDGE TYPE EU UNIDIRECTIONAL LIGHTWEIGHT", "EU");

    try (final ResultSet rs = database.command("sql", "CHECK DATABASE FIX")) {
      final Result r = rs.next();
      assertThat(((Number) r.getProperty("prunedDanglingEntries")).longValue()).as("result: %s", r.toJSON()).isGreaterThan(0L);
    }

    database.transaction(() -> database.lookupByRID(v[0].getIdentity(), true).asVertex().delete());
    assertThat(database.countType("V", false)).isEqualTo(1);
  }

  @Test
  void fixKeepsLightweightEdgesOfTheTypesThatStillExist() {
    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", "CREATE EDGE TYPE KEEP LIGHTWEIGHT");
    database.command("sql", "CREATE EDGE TYPE GONE LIGHTWEIGHT");
    final MutableVertex[] v = new MutableVertex[2];
    database.transaction(() -> {
      v[0] = database.newVertex("V").save();
      v[1] = database.newVertex("V").save();
      v[0].newLightEdge("KEEP", v[1]);
      v[0].newLightEdge("GONE", v[1]);
    });
    database.command("sql", "DROP TYPE GONE UNSAFE");

    final Map<String, Object> fixed = check(true);
    // one stale entry in the OUT list of v0 and one in the IN list of v1
    assertThat(((Number) fixed.get("prunedDanglingEntries")).longValue()).as("report: %s", fixed).isEqualTo(2L);

    assertThat(database.lookupByRID(v[0].getIdentity(), true).asVertex().countEdges(Vertex.DIRECTION.OUT, "KEEP")).isEqualTo(1);
    assertThat(((Number) check(false).get("invalidLinks")).longValue()).isZero();
  }
}
