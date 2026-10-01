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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8666: {@code STARTS WITH} and {@code min()} / {@code max()} over an indexed property scanned the whole label
 * while the index answers both. {@code STARTS WITH} now bounds an index range scan by the prefix (the predicate stays
 * as a filter, so the answer never depends on the index's collation), and a bare {@code min} / {@code max} reads one end
 * of the index.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8666CypherPrefixAndMinMaxIndexTest extends TestHelper {
  private static final int VERTICES = 3_000;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE D");
    database.command("sql", "CREATE PROPERTY D.x INTEGER");
    database.command("sql", "CREATE PROPERTY D.s STRING");
    database.command("sql", "CREATE PROPERTY D.u STRING");
    database.command("sql", "CREATE INDEX ON D (x) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON D (s) NOTUNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < VERTICES; i++) {
        // every 50th vertex holds neither key: a null is in neither the index nor the min / max
        if (i % 50 == 0)
          database.newVertex("D").set("u", "v" + i).save();
        else
          database.newVertex("D").set("x", 100 + (i * 7) % 1000).set("s", String.format("%04x", i)).set("u", "v" + i).save();
      }
      database.newVertex("D").set("s", "ab\\c%").set("x", -5).save();
      database.newVertex("D").set("s", "ab\uD7FF").save();
      database.newVertex("D").set("s", "ab\uD7FFz").save();
      database.newVertex("D").set("s", "\uFFFF").save();
      database.newVertex("D").set("s", "\uFFFFq").save();
      database.newVertex("D").set("s", "\uD83D\uDE00").save();
    });
  }

  private long expectedPrefixCount(final String prefix) {
    long count = 0;
    for (int i = 0; i < VERTICES; i++)
      if (i % 50 != 0 && String.format("%04x", i).startsWith(prefix))
        count++;
    for (final String extra : new String[] { "ab\\c%", "ab\uD7FF", "ab\uD7FFz", "\uFFFF", "\uFFFFq", "\uD83D\uDE00" })
      if (extra.startsWith(prefix))
        count++;
    return count;
  }

  @Test
  void startsWithUsesTheIndexRange() {
    final String query = "MATCH (d:D) WHERE d.s STARTS WITH 'a' RETURN count(*) AS n";
    assertThat(scalar(query, Map.of(), "n")).isEqualTo(expectedPrefixCount("a"));
    assertThat(profile(query, Map.of())).contains("NodeIndexRangeScan").doesNotContain("NodeByLabelScan");
  }

  @Test
  void startsWithAParameterUsesTheIndexRange() {
    final String query = "MATCH (d:D) WHERE d.s STARTS WITH $p RETURN count(*) AS n";
    for (final String prefix : new String[] { "0", "0a", "0a1", "ff", "zzz", "ab\\", "ab\\c%", "ab", "ab\uD7FF", "\uFFFF", "\uFFFFq" }) {
      assertThat(scalar(query, Map.of("p", prefix), "n")).as("prefix '%s'", prefix).isEqualTo(expectedPrefixCount(prefix));
    }
    assertThat(profile(query, Map.of("p", "0a"))).contains("NodeIndexRangeScan");
  }

  @Test
  void startsWithOnTheEndOfTheKeySpace() {
    // No key sorts after a run of U+FFFF: the range has no upper end
    final String query = "MATCH (d:D) WHERE d.s STARTS WITH $p RETURN count(*) AS n";
    assertThat(scalar(query, Map.of("p", "\uFFFF"), "n")).isEqualTo(2L);
    assertThat(scalar(query, Map.of("p", "\uFFFFq"), "n")).isEqualTo(1L);
    assertThat(scalar(query, Map.of("p", "ab\uD7FF"), "n")).isEqualTo(2L);
  }

  @Test
  void startsWithAnEmptyOrNullPrefix() {
    assertThat(scalar("MATCH (d:D) WHERE d.s STARTS WITH '' RETURN count(*) AS n", Map.of(), "n"))
        .isEqualTo(expectedPrefixCount(""));
    final Map<String, Object> nullParam = new HashMap<>();
    nullParam.put("p", null);
    assertThat(scalar("MATCH (d:D) WHERE d.s STARTS WITH $p RETURN count(*) AS n", nullParam, "n")).isEqualTo(0L);
  }

  @Test
  void startsWithKeepsTheOtherPredicates() {
    assertThat(scalar("MATCH (d:D) WHERE d.s STARTS WITH '0a' AND d.x > 500 RETURN count(*) AS n", Map.of(), "n"))
        .isEqualTo(countWhere("0a", 500));
    assertThat(scalar("MATCH (d:D) WHERE d.s STARTS WITH '0a' AND d.u ENDS WITH '3' RETURN count(*) AS n", Map.of(), "n"))
        .isEqualTo(countEndingWith("0a", '3'));
  }

  @Test
  void startsWithOnANonStringIndexFallsBackToTheFilter() {
    // A string prefix against an INTEGER key: no row qualifies, and the index must not be asked to compare them
    assertThat(scalar("MATCH (d:D) WHERE d.x STARTS WITH '1' RETURN count(*) AS n", Map.of(), "n")).isEqualTo(0L);
  }

  @Test
  void startsWithOnAnUnindexedPropertyStillWorks() {
    assertThat(scalar("MATCH (d:D) WHERE d.u STARTS WITH 'v12' RETURN count(*) AS n", Map.of(), "n")).isEqualTo(countU("v12"));
  }

  @Test
  void minAndMaxAreReadFromTheIndex() {
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    for (int i = 0; i < VERTICES; i++)
      if (i % 50 != 0) {
        min = Math.min(min, 100 + (i * 7) % 1000);
        max = Math.max(max, 100 + (i * 7) % 1000);
      }
    min = Math.min(min, -5);

    assertThat(scalar("MATCH (d:D) RETURN min(d.x) AS v", Map.of(), "v")).isEqualTo((int) min);
    assertThat(scalar("MATCH (d:D) RETURN max(d.x) AS v", Map.of(), "v")).isEqualTo((int) max);
    assertThat(scalar("MATCH (d:D) RETURN max(d.x)", Map.of(), "max(d.x)")).isEqualTo((int) max);
    assertThat(profile("MATCH (d:D) RETURN min(d.x) AS v", Map.of())).contains("MIN FROM INDEX");
    assertThat(profile("MATCH (d:D) RETURN max(d.x) AS v", Map.of())).contains("MAX FROM INDEX");
  }

  @Test
  void minAndMaxOfAStringKey() {
    assertThat(scalar("MATCH (d:D) RETURN min(d.s) AS v", Map.of(), "v")).isEqualTo("0001");
    // U+FFFF is above every BMP character in UTF-16 and the 4-byte emoji above it in UTF-8, where the index orders the keys
    assertThat(scalar("MATCH (d:D) RETURN max(d.s) AS v", Map.of(), "v")).isEqualTo("\uD83D\uDE00");
  }

  @Test
  void minAndMaxSkipAndLimit() {
    assertThat(scalar("MATCH (d:D) RETURN max(d.x) AS v LIMIT 1", Map.of(), "v")).isNotNull();
    try (final ResultSet rs = database.query("opencypher", "MATCH (d:D) RETURN max(d.x) AS v SKIP 1")) {
      assertThat(rs.hasNext()).isFalse();
    }
    try (final ResultSet rs = database.query("opencypher", "MATCH (d:D) RETURN max(d.x) AS v LIMIT 0")) {
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void minAndMaxOfAnEmptyLabelAreNull() {
    database.command("sql", "CREATE VERTEX TYPE Empty");
    database.command("sql", "CREATE PROPERTY Empty.x INTEGER");
    database.command("sql", "CREATE INDEX ON Empty (x) NOTUNIQUE");
    try (final ResultSet rs = database.query("opencypher", "MATCH (e:Empty) RETURN min(e.x) AS lo, max(e.x) AS hi")) {
      assertThat(rs.hasNext()).isTrue();
    }
    try (final ResultSet rs = database.query("opencypher", "MATCH (e:Empty) RETURN min(e.x) AS v")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat((Object) rs.next().getProperty("v")).isNull();
      assertThat(rs.hasNext()).isFalse();
    }
    database.transaction(() -> database.newVertex("Empty").save());
    assertThat(scalar("MATCH (e:Empty) RETURN max(e.x) AS v", Map.of(), "v")).isNull();
  }

  @Test
  void minAndMaxSeeTheOpenTransaction() {
    database.begin();
    try {
      database.newVertex("D").set("x", 99_999).save();
      database.newVertex("D").set("x", -99_999).save();
      assertThat(scalar("MATCH (d:D) RETURN max(d.x) AS v", Map.of(), "v")).isEqualTo(99_999);
      assertThat(scalar("MATCH (d:D) RETURN min(d.x) AS v", Map.of(), "v")).isEqualTo(-99_999);
    } finally {
      database.rollback();
    }
    assertThat(scalar("MATCH (d:D) RETURN max(d.x) AS v", Map.of(), "v")).isNotEqualTo(99_999);
  }

  @Test
  void minAndMaxWithAnotherFilterOrAnUnindexedKeyKeepTheScan() {
    assertThat(scalar("MATCH (d:D) WHERE d.x > 200 RETURN min(d.x) AS v", Map.of(), "v")).isEqualTo(201);
    // a range of the same property is answered from the index too, since #8812; anything else in the WHERE keeps the scan
    assertThat(scalar("MATCH (d:D) WHERE d.x > 200 AND d.u <> 'zz' RETURN min(d.x) AS v", Map.of(), "v")).isEqualTo(201);
    assertThat(profile("MATCH (d:D) WHERE d.x > 200 AND d.u <> 'zz' RETURN min(d.x) AS v", Map.of())).doesNotContain("MIN FROM INDEX");
    assertThat(profile("MATCH (d:D) WHERE d.x = 200 RETURN min(d.x) AS v", Map.of())).doesNotContain("MIN FROM INDEX");
    assertThat(profile("MATCH (d:D) RETURN min(d.u) AS v", Map.of())).doesNotContain("MIN FROM INDEX");
    assertThat(scalar("MATCH (d:D) RETURN min(d.u) AS v", Map.of(), "v")).isEqualTo("v0");
    assertThat(profile("MATCH (d:D) RETURN min(d.x) AS lo, count(*) AS n", Map.of())).doesNotContain("MIN FROM INDEX");
    assertThat(profile("MATCH (d:D) RETURN DISTINCT min(d.x) AS v", Map.of())).doesNotContain("MIN FROM INDEX");
  }

  @Test
  void minAndMaxWithNullsIndexedKeepTheScan() {
    database.command("sql", "CREATE VERTEX TYPE N");
    database.command("sql", "CREATE PROPERTY N.x INTEGER");
    database.command("sql", "CREATE INDEX ON N (x) NOTUNIQUE NULL_STRATEGY INDEX");
    database.transaction(() -> {
      database.newVertex("N").set("x", 5).save();
      database.newVertex("N").save();
    });
    assertThat(scalar("MATCH (n:N) RETURN min(n.x) AS v", Map.of(), "v")).isEqualTo(5);
    assertThat(profile("MATCH (n:N) RETURN min(n.x) AS v", Map.of())).doesNotContain("MIN FROM INDEX");
  }

  @Test
  void minAndMaxOverASubTypeHierarchy() {
    database.command("sql", "CREATE VERTEX TYPE DChild EXTENDS D");
    database.transaction(() -> {
      database.newVertex("DChild").set("x", 50_000).save();
      database.newVertex("DChild").set("x", -50_000).save();
    });
    assertThat(scalar("MATCH (d:D) RETURN max(d.x) AS v", Map.of(), "v")).isEqualTo(50_000);
    assertThat(scalar("MATCH (d:D) RETURN min(d.x) AS v", Map.of(), "v")).isEqualTo(-50_000);
    assertThat(scalar("MATCH (d:DChild) RETURN max(d.x) AS v", Map.of(), "v")).isEqualTo(50_000);
  }

  @Test
  void startsWithOnACaseInsensitiveIndexMissesNothing() {
    // The keys of a case-insensitive index are lower-cased: 'AZ' bumped to 'A[' and lower-cased is 'a[', which sorts
    // below 'azb', so a range built from it would drop a row the predicate accepts
    database.command("sql", "CREATE VERTEX TYPE Ci");
    database.command("sql", "CREATE PROPERTY Ci.s STRING");
    database.command("sql", "CREATE INDEX ON Ci (s COLLATE ci) NOTUNIQUE");
    database.transaction(() -> {
      for (final String v : new String[] { "AZb", "AZ", "azc", "Az", "AY", "B", "a[" })
        database.newVertex("Ci").set("s", v).save();
    });
    for (final String prefix : new String[] { "AZ", "Az", "az", "A", "a" }) {
      long expected = 0;
      for (final String v : new String[] { "AZb", "AZ", "azc", "Az", "AY", "B", "a[" })
        if (v.startsWith(prefix))
          expected++;
      assertThat(scalar("MATCH (c:Ci) WHERE c.s STARTS WITH $p RETURN count(*) AS n", Map.of("p", prefix), "n"))
          .as("prefix '%s'", prefix).isEqualTo(expected);
    }
    assertThat(scalar("MATCH (c:Ci) WHERE c.s STARTS WITH 'AZ' RETURN count(*) AS n", Map.of(), "n")).isEqualTo(2L);
    assertThat(profile("MATCH (c:Ci) WHERE c.s STARTS WITH 'AZ' RETURN count(*) AS n", Map.of())).doesNotContain("next(");
  }

  @Test
  void startsWithANonStringParameter() {
    for (final Object param : new Object[] { 5, 5.5, true, List.of("0a"), Map.of("a", 1) })
      assertThat(scalar("MATCH (d:D) WHERE d.s STARTS WITH $p RETURN count(*) AS n", Map.of("p", param), "n")).as(String.valueOf(param))
          .isEqualTo(0L);
    assertThat(scalar("MATCH (d:D) WHERE d.x STARTS WITH $p RETURN count(*) AS n", Map.of("p", 5), "n")).isEqualTo(0L);
  }

  @Test
  void minAndMaxSeeADeleteInTheOpenTransaction() {
    final int max = (Integer) scalar("MATCH (d:D) RETURN max(d.x) AS v", Map.of(), "v");
    database.begin();
    try {
      database.command("sql", "DELETE FROM D WHERE x = " + max);
      final Object afterDelete = scalar("MATCH (d:D) WHERE d.x IS NOT NULL RETURN max(d.x) AS v", Map.of(), "v");
      assertThat(scalar("MATCH (d:D) RETURN max(d.x) AS v", Map.of(), "v")).isEqualTo(afterDelete);
      assertThat(afterDelete).isNotEqualTo(max);
    } finally {
      database.rollback();
    }
    assertThat(scalar("MATCH (d:D) RETURN max(d.x) AS v", Map.of(), "v")).isEqualTo(max);
  }

  private long countWhere(final String prefix, final int above) {
    long count = 0;
    for (int i = 0; i < VERTICES; i++)
      if (i % 50 != 0 && String.format("%04x", i).startsWith(prefix) && 100 + (i * 7) % 1000 > above)
        count++;
    return count;
  }

  private long countEndingWith(final String prefix, final char last) {
    long count = 0;
    for (int i = 0; i < VERTICES; i++)
      if (i % 50 != 0 && String.format("%04x", i).startsWith(prefix) && ("v" + i).endsWith(String.valueOf(last)))
        count++;
    return count;
  }

  private long countU(final String prefix) {
    long count = 0;
    for (int i = 0; i < VERTICES; i++)
      if (("v" + i).startsWith(prefix))
        count++;
    return count;
  }

  private Object scalar(final String query, final Map<String, Object> params, final String column) {
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      assertThat(rs.hasNext()).isTrue();
      return rs.next().getProperty(column);
    }
  }

  private String profile(final String query, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("opencypher", "PROFILE " + query, params)) {
      while (rs.hasNext())
        rs.next();
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }
}
