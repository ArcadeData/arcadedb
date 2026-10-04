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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9236: an equality on the first property of a composite HASH index made the seek read a prefix range of an index that
 * cannot be read in key order, and threw "does not support ordered iterations".
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9236CompositeHashPrefixTest extends TestHelper {

  private List<Long> ps(final String query, final Map<String, Object> params) {
    final List<Long> got = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      while (rs.hasNext())
        got.add(rs.next().<Number>getProperty("p").longValue());
    }
    return got;
  }

  private void create(final String type, final String indexKind) {
    database.command("sql", "CREATE VERTEX TYPE " + type);
    database.command("sql", "CREATE PROPERTY " + type + ".p INTEGER");
    database.command("sql", "CREATE PROPERTY " + type + ".q INTEGER");
    database.command("sql", "CREATE INDEX ON " + type + " (p, q) " + indexKind);
    database.transaction(() -> {
      database.newVertex(type).set("p", 1, "q", 5).save();
      database.newVertex(type).set("p", 2, "q", 6).save();
    });
  }

  @Test
  void prefixOfCompositeHashIndexFallsBackToTheScan() {
    for (final String[] t : new String[][] { { "HashT", "NOTUNIQUE_HASH" }, { "UniqueHashT", "UNIQUE_HASH" } }) {
      create(t[0], t[1]);
      assertThat(ps("MATCH (n:" + t[0] + ") WHERE n.p = 1 RETURN n.p AS p", Map.of())).containsExactly(1L);
      assertThat(ps("MATCH (n:" + t[0] + " {p: 1}) RETURN n.p AS p", Map.of())).containsExactly(1L);
      assertThat(ps("MATCH (n:" + t[0] + ") WHERE n.p IN [1, 2] RETURN n.p AS p ORDER BY p", Map.of())).containsExactly(1L, 2L);
      assertThat(ps("MATCH (n:" + t[0] + ") WHERE n.p = 1 OR n.p = 2 RETURN n.p AS p ORDER BY p", Map.of())).containsExactly(1L, 2L);
      assertThat(ps("MATCH (n:" + t[0] + ") WHERE n.p = 1 AND n.q = 5 RETURN n.p AS p", Map.of())).containsExactly(1L);
    }
  }

  @Test
  void everyColumnPinnedStillSeeksTheCompositeHashIndex() {
    create("HashAll", "NOTUNIQUE_HASH");
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN MATCH (n:HashAll) WHERE n.p = 1 AND n.q = 5 RETURN n.p AS p")) {
      assertThat(rs.next().toJSON().toString()).contains("NodeIndexSeek");
    }
  }

  @Test
  void inListAndOrOverAHashIndexKeepTheIndexSeek() {
    database.command("sql", "CREATE VERTEX TYPE HashOne");
    database.command("sql", "CREATE PROPERTY HashOne.p INTEGER");
    database.command("sql", "CREATE INDEX ON HashOne (p) UNIQUE_HASH");
    database.transaction(() -> {
      database.newVertex("HashOne").set("p", 1).save();
      database.newVertex("HashOne").set("p", 2).save();
      database.newVertex("HashOne").set("p", 3).save();
    });
    for (final String where : new String[] { "n.p IN [1, 2]", "n.p = 1 OR n.p = 2" }) {
      final String query = "MATCH (n:HashOne) WHERE " + where + " RETURN n.p AS p ORDER BY p";
      assertThat(ps(query, Map.of())).as(where).containsExactly(1L, 2L);
      try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
        assertThat(rs.next().toJSON().toString()).as(where).contains("NodeIndexSeek");
      }
    }

    create("HashPair", "NOTUNIQUE_HASH");
    final String pair = "MATCH (n:HashPair) WHERE n.p IN [1, 2] AND n.q = 5 RETURN n.p AS p ORDER BY p";
    assertThat(ps(pair, Map.of())).containsExactly(1L);
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + pair)) {
      assertThat(rs.next().toJSON().toString()).contains("NodeIndexSeek");
    }
  }

  @Test
  void nullParameterForTheSecondPropertyMatchesNothing() {
    create("HashN", "NOTUNIQUE_HASH");
    final Map<String, Object> params = new HashMap<>();
    params.put("p", 1);
    params.put("q", null);
    assertThat(ps("MATCH (n:HashN) WHERE n.p = $p AND n.q = $q RETURN n.p AS p", params)).isEmpty();
  }

  @Test
  void nullParameterForTheSecondPropertyOfAUniqueHashIndexMatchesNothing() {
    create("UniqueHashN", "UNIQUE_HASH");
    final Map<String, Object> params = new HashMap<>();
    params.put("p", 1);
    params.put("q", null);
    assertThat(ps("MATCH (n:UniqueHashN) WHERE n.p = $p AND n.q = $q RETURN n.p AS p", params)).isEmpty();
    assertThat(ps("MATCH (n:UniqueHashN) WHERE n.p = $p RETURN n.p AS p", params)).containsExactly(1L);
  }

  @Test
  void nullParameterForTheSecondPropertyOfAnOrderedCompositeIndexMatchesNothing() {
    create("LsmN", "NOTUNIQUE");
    final Map<String, Object> params = new HashMap<>();
    params.put("p", 1);
    params.put("q", null);
    assertThat(ps("MATCH (n:LsmN) WHERE n.p = $p AND n.q = $q RETURN n.p AS p", params)).isEmpty();
  }

  @Test
  void orderedCompositeIndexStillAnswersAPrefix() {
    create("LsmT", "NOTUNIQUE");
    assertThat(ps("MATCH (n:LsmT) WHERE n.p = 1 RETURN n.p AS p", Map.of())).containsExactly(1L);
    assertThat(ps("MATCH (n:LsmT) WHERE n.p IN [1, 2] RETURN n.p AS p ORDER BY p", Map.of())).containsExactly(1L, 2L);
  }
}
