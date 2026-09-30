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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8695: a chained MATCH / OPTIONAL MATCH whose inline equality is row-dependent and has no
 * index re-scanned the whole type once per outer row (rows x records). It now reads the type once into a transient hash.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class OpenCypherUnindexedChainedMatchIssue8695Test extends TestHelper {

  @Test
  void hashedLookupReturnsExactlyWhatTheScanReturns() {
    database.getSchema().createVertexType("Conn");
    database.getSchema().createVertexType("Ref");
    database.transaction(() -> {
      database.newVertex("Conn").set("name", "a", "kind", "X", "n", 1).save();
      database.newVertex("Conn").set("name", "a", "kind", "Y", "n", 1).save();
      database.newVertex("Conn").set("name", "b", "kind", "X", "n", 2L).save();
      database.newVertex("Conn").set("name", "c", "kind", "X", "n", 3.0d).save();
      database.newVertex("Conn").set("name", "d", "kind", "X", "n", 4.5d).save();
      database.newVertex("Conn").set("kind", "X").save(); // no name, no n
      for (final Object v : new Object[] { "a", "b", "zzz", null, 2, 3L, 4.5d, 1.0d, true })
        database.newVertex("Ref").set("v", v).save();
    });

    // the expected answer comes from the same filter written as a WHERE on a cartesian product: no inline map involved
    for (final String prop : new String[] { "name", "n" }) {
      final Map<Object, Integer> inline = counts("MATCH (r:Ref) OPTIONAL MATCH (c:Conn {" + prop + ": r.v, kind: 'X'}) RETURN r.v AS v, count(c) AS c");
      final Map<Object, Integer> where = counts(
          "MATCH (r:Ref) OPTIONAL MATCH (c:Conn) WHERE c." + prop + " = r.v AND c.kind = 'X' RETURN r.v AS v, count(c) AS c");
      assertThat(inline).as(prop).isEqualTo(where);
    }
    assertThat(counts("MATCH (r:Ref) OPTIONAL MATCH (c:Conn {n: r.v}) RETURN r.v AS v, count(c) AS c")).containsEntry(3L, 1)
        .containsEntry(2, 1).containsEntry(4.5d, 1);
  }

  @Test
  void unindexedChainedMatchDoesNotRescanTheTypePerRow() {
    database.getSchema().createVertexType("Asset");
    database.getSchema().createVertexType("Seed");
    final int n = 15_000;
    database.transaction(() -> {
      for (int i = 0; i < n; i++) {
        database.newVertex("Asset").set("name", "n" + i).save();
        database.newVertex("Seed").set("ref", "n" + i).save();
      }
    });

    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    int rows = 0;
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (s:Seed) OPTIONAL MATCH (a:Asset {name: s.ref}) RETURN s.ref AS r, a.name AS a")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat((String) row.getProperty("a")).isEqualTo(row.getProperty("r"));
        rows++;
      }
    }
    assertThat(rows).isEqualTo(n);
    // 15000 x 15000 record reads is tens of seconds; one scan plus a hash is well under a second
    watch.assertGaveUpWithin(15_000, "one scan plus hashed lookups vs a full type scan per outer row");
  }

  private Map<Object, Integer> counts(final String query) {
    final Map<Object, Integer> out = new HashMap<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        out.merge(row.getProperty("v"), ((Number) row.getProperty("c")).intValue(), Integer::sum);
      }
    }
    return out;
  }

  /** A write upstream of the chained match must stay visible to it: the hash is only for read-only statements. */
  @Test
  void recordsCreatedUpstreamStayVisibleToTheChainedMatch() {
    database.getSchema().createVertexType("Asset");
    database.getSchema().createVertexType("Seed");
    database.transaction(() -> {
      for (int i = 0; i < 5; i++)
        database.newVertex("Seed").set("ref", "n" + i).save();
    });

    int found = 0;
    try (final ResultSet rs = database.command("opencypher",
        "MATCH (s:Seed) CREATE (:Asset {name: s.ref}) WITH s MATCH (a:Asset {name: s.ref}) RETURN s.ref AS r, a.name AS a")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat((String) row.getProperty("a")).isEqualTo(row.getProperty("r"));
        found++;
      }
    }
    assertThat(found).isEqualTo(5);
  }

  /** The first properties of the map may not be hashable (null, a list): the lookup still answers through the plain scan. */
  @Test
  void unsupportedLookupValuesFallBackToTheScan() {
    database.getSchema().createVertexType("Conn");
    database.getSchema().createVertexType("Ref");
    database.transaction(() -> {
      for (int i = 0; i < 12; i++)
        database.newVertex("Conn").set("name", "n" + i).save();
      for (int i = 0; i < 12; i++)
        database.newVertex("Ref").set("v", i % 2 == 0 ? null : "n" + i).save();
    });
    final Map<Object, Integer> c = counts("MATCH (r:Ref) OPTIONAL MATCH (c:Conn {name: r.v}) RETURN r.v AS v, count(c) AS c");
    assertThat(c.get("n1")).isEqualTo(1);
    assertThat(c.get(null)).isEqualTo(0);
  }
}
