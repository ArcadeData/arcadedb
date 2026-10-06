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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #9291: {@code CREATE INDEX} can ask for the sorted build with {@code METADATA {"buildMode": "SORTED"}},
 * and the sorted build orders a run by the rank of its distinct keys when keys repeat (low-cardinality column) instead of comparing
 * every pair of entries. Whatever the cardinality, both builds must hold the same entries in the same order.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9291SortedBuildTest extends TestHelper {
  private static final int ROWS = 60_000;

  @Test
  void sqlBuildModeSortedMatchesDefaultOnLowAndHighCardinality() {
    database.command("sql", "CREATE DOCUMENT TYPE Sorted");
    database.command("sql", "CREATE PROPERTY Sorted.low STRING");
    database.command("sql", "CREATE PROPERTY Sorted.high LONG");
    database.command("sql", "CREATE DOCUMENT TYPE Plain");
    database.command("sql", "CREATE PROPERTY Plain.low STRING");
    database.command("sql", "CREATE PROPERTY Plain.high LONG");
    for (final String type : new String[] { "Sorted", "Plain" }) {
      database.begin();
      for (int i = 0; i < ROWS; i++) {
        database.newDocument(type).set("low", "k" + (i * 7919 % 37)).set("high", (long) (i * 31 % 40_009)).save();
        if (i % 10_000 == 9_999) {
          database.commit();
          database.begin();
        }
      }
      database.commit();
    }

    database.command("sql", "CREATE INDEX ON Sorted (low) NOTUNIQUE METADATA {\"buildMode\": \"SORTED\"}");
    database.command("sql", "CREATE INDEX ON Sorted (high) NOTUNIQUE METADATA {\"buildMode\": \"sorted\"}");
    database.command("sql", "CREATE INDEX ON Plain (low) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON Plain (high) NOTUNIQUE");

    for (final String property : new String[] { "low", "high" }) {
      final TypeIndex sorted = database.getSchema().getType("Sorted").getPolymorphicIndexByProperties(property);
      final TypeIndex plain = database.getSchema().getType("Plain").getPolymorphicIndexByProperties(property);
      assertThat(sorted.countEntries()).isEqualTo(ROWS);
      assertThat(plain.countEntries()).isEqualTo(ROWS);
      final List<String> sortedEntries = entries(sorted);
      final List<String> plainEntries = entries(plain);
      // the key order is the index's contract; the order of the rids within one key is not
      assertThat(keysOnly(sortedEntries)).as(property + " key order").isEqualTo(keysOnly(plainEntries));
      assertThat(sortedEntries.stream().sorted().toList()).as(property + " entries").isEqualTo(plainEntries.stream().sorted().toList());
    }

    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Sorted WHERE low = 'k5'")) {
      final long expected = countRows("Plain", "low = 'k5'");
      assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(expected).isPositive();
    }
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Sorted WHERE high = 123")) {
      assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(countRows("Plain", "high = 123"));
    }
  }

  @Test
  void sqlBuildModeDefaultAndInvalidValues() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.p STRING");
    database.command("sql", "CREATE PROPERTY T.q STRING");
    database.transaction(() -> database.newDocument("T").set("p", "a").set("q", "b").save());

    database.command("sql", "CREATE INDEX ON T (p) NOTUNIQUE METADATA {\"buildMode\": \"DEFAULT\"}");
    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON T (q) NOTUNIQUE METADATA {\"buildMode\": \"FAST\"}"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("buildMode");
    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON T (q) FULL_TEXT METADATA {\"buildMode\": \"SORTED\"}"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("SORTED");
    // an invalid directive fails whether or not the index already exists
    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX IF NOT EXISTS ON T (p) NOTUNIQUE METADATA {\"buildMode\": \"FAST\"}"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("buildMode");
    // the directive never reaches the index configuration, so a statement that only carries it is not "METADATA on an index that takes none"
    database.command("sql", "CREATE INDEX ON T (q) UNIQUE METADATA {\"buildMode\": \"SORTED\"}");
    assertThat(database.getSchema().getType("T").getPolymorphicIndexByProperties("q").countEntries()).isEqualTo(1);
  }

  @Test
  void caseInsensitiveCollationKeysShareAGroupAndStayOrdered() {
    for (final String type : new String[] { "CiSorted", "CiPlain" }) {
      database.command("sql", "CREATE DOCUMENT TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".p STRING");
      database.transaction(() -> {
        for (int i = 0; i < 5_000; i++)
          database.newDocument(type).set("p", (i % 2 == 0 ? "Key" : "kEY") + (i % 13)).save();
      });
    }
    database.command("sql", "CREATE INDEX ON CiSorted (p COLLATE ci) NOTUNIQUE METADATA {\"buildMode\": \"SORTED\"}");
    database.command("sql", "CREATE INDEX ON CiPlain (p COLLATE ci) NOTUNIQUE");

    final List<String> sorted = entries(database.getSchema().getType("CiSorted").getPolymorphicIndexByProperties("p"));
    final List<String> plain = entries(database.getSchema().getType("CiPlain").getPolymorphicIndexByProperties("p"));
    assertThat(sorted).hasSize(5_000);
    assertThat(keysOnly(sorted)).isEqualTo(keysOnly(plain));
    assertThat(sorted.stream().sorted().toList()).isEqualTo(plain.stream().sorted().toList());
  }

  @Test
  void decimalKeysOfDifferentScaleAreOneKeyAndStayOrderedByRid() {
    for (final String type : new String[] { "DecSorted", "DecPlain" }) {
      database.command("sql", "CREATE DOCUMENT TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".d DECIMAL");
      database.transaction(() -> {
        for (int i = 0; i < 3_000; i++)
          database.newDocument(type).set("d", i % 2 == 0 ? new BigDecimal("1.0") : new BigDecimal("1.00")).save();
      });
    }
    database.command("sql", "CREATE INDEX ON DecSorted (d) NOTUNIQUE METADATA {\"buildMode\": \"SORTED\"}");
    database.command("sql", "CREATE INDEX ON DecPlain (d) NOTUNIQUE");
    final List<String> sorted = entries(database.getSchema().getType("DecSorted").getPolymorphicIndexByProperties("d"));
    final List<String> plain = entries(database.getSchema().getType("DecPlain").getPolymorphicIndexByProperties("d"));
    assertThat(sorted).hasSize(3_000);
    assertThat(sorted.stream().sorted().toList()).isEqualTo(plain.stream().sorted().toList());
  }

  @Test
  void uniqueSortedBuildStillDetectsDuplicatesOfRepeatedKeys() {
    database.command("sql", "CREATE DOCUMENT TYPE U");
    database.command("sql", "CREATE PROPERTY U.p STRING");
    database.transaction(() -> {
      for (int i = 0; i < 1_000; i++)
        database.newDocument("U").set("p", "dup" + (i % 10)).save();
    });
    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON U (p) UNIQUE METADATA {\"buildMode\": \"SORTED\"}"))
        .isInstanceOf(RuntimeException.class);
    assertThat(database.getSchema().getType("U").getPolymorphicIndexByProperties("p")).isNull();
  }

  private long countRows(final String type, final String where) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + type + " WHERE " + where)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private static List<String> keysOnly(final List<String> entries) {
    return entries.stream().map(e -> e.substring(0, e.indexOf('@'))).toList();
  }

  /** every (key, rid-position) of the index in cursor order, with the key as text */
  private List<String> entries(final TypeIndex index) {
    final List<String> out = new ArrayList<>();
    final IndexCursor cursor = index.iterator(true);
    while (cursor.hasNext()) {
      final RID rid = cursor.next().getIdentity();
      out.add(Arrays.toString(cursor.getKeys()) + "@" + rid.getPosition());
    }
    return out;
  }
}
