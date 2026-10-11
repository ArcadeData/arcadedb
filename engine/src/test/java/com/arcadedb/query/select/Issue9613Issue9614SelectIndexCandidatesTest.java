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
package com.arcadedb.query.select;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The native {@code select()} must answer what the unindexed type answers, whatever index serves it.
 * <ul>
 *   <li>#9613: a {@code COLLATE ci} index answers an equality with every record that differs from the operand only in case,
 *   which the residual filter then discards. The candidate cap of #6565 stopped after {@code skip + limit} of those, so the
 *   records that matched were never reached: {@code .limit(1)} answered nothing.</li>
 *   <li>#9614: an {@code IN} list probed the index once per item, repeated items included, and the merge of the probes
 *   returned each record once per probe that found it.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9613Issue9614SelectIndexCandidatesTest extends TestHelper {

  private void load(final String type, final String index, final String[][] rows) {
    database.command("sql", "CREATE DOCUMENT TYPE " + type);
    database.command("sql", "CREATE PROPERTY " + type + ".id STRING");
    database.command("sql", "CREATE PROPERTY " + type + ".s STRING");
    database.command("sql", "CREATE PROPERTY " + type + ".t STRING");
    database.command("sql", "CREATE PROPERTY " + type + ".n INTEGER");
    if (index != null)
      for (final String statement : index.replace("%T", type).split(";"))
        database.command("sql", statement);
    database.transaction(() -> {
      for (final String[] row : rows) {
        final var doc = database.newDocument(type).set("id", row[0]).set("s", row[1]);
        if (row.length > 2)
          doc.set("t", row[2]);
        if (row.length > 3)
          doc.set("n", Integer.parseInt(row[3]));
        doc.save();
      }
    });
  }

  private static List<String> ids(final SelectWhereAfterBlock query) {
    return query.documents().toList().stream().map((final Document d) -> d.getString("id")).sorted().toList();
  }

  private static List<String> ids(final Select query) {
    return query.documents().toList().stream().map((final Document d) -> d.getString("id")).sorted().toList();
  }

  private SelectWhereLeftBlock where(final String type) {
    return database.select().fromType(type).where();
  }

  // ---------------------------------------------------------------- #9613

  private static final String[][] CASED = { { "s1", "Smith" }, { "x1", "SMITH" }, { "x2", "smith" }, { "s2", "Smith" },
      { "x3", "sMITH" }, { "x4", "SmitH" } };

  @Test
  void limitOnAFoldedIndexReachesTheMatchesBehindTheFoldedOnes() {
    load("Ci", "CREATE INDEX ON %T (s COLLATE ci) NOTUNIQUE", CASED);
    load("Plain", null, CASED);

    assertThat(ids(where("Ci").property("s").eq().value("Smith"))).containsExactly("s1", "s2");
    for (int limit = 1; limit <= 6; limit++) {
      final int expected = Math.min(limit, 2);
      assertThat(where("Plain").property("s").eq().value("Smith").limit(limit).documents().toList()).hasSize(expected);
      assertThat(where("Ci").property("s").eq().value("Smith").limit(limit).documents().toList()).as("limit " + limit)
          .hasSize(expected);
      assertThat(where("Ci").property("s").eq().value("Smith").limit(limit).count()).as("count limit " + limit)
          .isEqualTo(expected);
      assertThat(where("Ci").property("s").in().value(List.of("Smith")).limit(limit).documents().toList())
          .as("in limit " + limit).hasSize(expected);
    }

    // paging: the two pages hold the two matches, one each
    final List<String> pages = new ArrayList<>();
    pages.addAll(ids(where("Ci").property("s").eq().value("Smith").skip(0).limit(1)));
    pages.addAll(ids(where("Ci").property("s").eq().value("Smith").skip(1).limit(1)));
    assertThat(pages).containsExactlyInAnyOrder("s1", "s2");
  }

  @Test
  void limitOnAFoldedCompositeIndexReachesTheMatchesBehindTheFoldedOnes() {
    final String[][] rows = { { "s1", "Smith", "UK" }, { "x1", "SMITH", "uk" }, { "x2", "smith", "UK" }, { "s2", "Smith", "UK" },
        { "x3", "Smith", "Uk" } };
    load("Ci", "CREATE INDEX ON %T (s COLLATE ci, t COLLATE ci) NOTUNIQUE", rows);
    load("Plain", null, rows);

    for (int limit = 1; limit <= 5; limit++) {
      final int expected = Math.min(limit, 2);
      assertThat(where("Plain").property("s").eq().value("Smith").and().property("t").eq().value("UK").limit(limit).documents()
          .toList()).hasSize(expected);
      assertThat(where("Ci").property("s").eq().value("Smith").and().property("t").eq().value("UK").limit(limit).documents()
          .toList()).as("limit " + limit).hasSize(expected);
      assertThat(where("Ci").property("s").eq().value("Smith").and().property("t").eq().value("UK").limit(limit).count())
          .as("count limit " + limit).isEqualTo(expected);
    }
  }

  @Test
  void limitOnACaseSensitiveIndexStillCapsTheCandidates() {
    // the cap stays armed where the probe is exact: the cursor pulls skip + limit candidates and no more
    load("Cs", "CREATE INDEX ON %T (s) NOTUNIQUE", CASED);
    load("Ci", "CREATE INDEX ON %T (s COLLATE ci) NOTUNIQUE", CASED);
    assertThat(candidateLimit("Cs")).isEqualTo(1);
    // ... and is never armed over folded keys
    assertThat(candidateLimit("Ci")).isEqualTo(-1);
  }

  private int candidateLimit(final String type) {
    final Select select = where(type).property("s").eq().value("Smith").limit(1);
    select.compile();
    final SelectExecutor executor = new SelectExecutor(select);
    executor.lookForIndexes().close();
    return executor.indexCandidateLimit;
  }

  // ---------------------------------------------------------------- #9614

  private static final String[][] LETTERS = { { "a1", "a", "p", "1" }, { "a2", "a", "q", "1" }, { "b1", "b", "p", "2" },
      { "b2", "b", "r", "2" }, { "c1", "c", "r", "3" } };

  @Test
  void repeatedValueInAnInListReturnsEachRecordOnce() {
    load("Idx", "CREATE INDEX ON %T (s) NOTUNIQUE;CREATE INDEX ON %T (t) NOTUNIQUE;CREATE INDEX ON %T (id) UNIQUE", LETTERS);
    load("Plain", null, LETTERS);

    for (final List<String> list : List.of(List.of("a"), List.of("a", "a"), List.of("a", "a", "b"), List.of("a", "b", "c", "a"),
        List.of("a", "a", "a"))) {
      final List<String> expected = ids(where("Plain").property("s").in().value(list));
      assertThat(ids(where("Idx").property("s").in().value(list))).as("IN " + list).isEqualTo(expected);
      assertThat(where("Idx").property("s").in().value(list).count()).as("count IN " + list).isEqualTo(expected.size());
      assertThat(where("Idx").property("s").in().value(list).limit(2).documents().toList()).as("limit IN " + list)
          .hasSize(Math.min(2, expected.size()));
    }

    // a UNIQUE index and the same key twice: one record, once
    assertThat(ids(where("Idx").property("id").in().value(List.of("a1", "a1")))).containsExactly("a1");

    // a sibling that contributes nothing must not change the answer
    assertThat(ids(where("Idx").property("s").in().value(List.of("a", "a")).or().property("t").eq().value("zz")))//
        .isEqualTo(ids(where("Plain").property("s").in().value(List.of("a", "a")).or().property("t").eq().value("zz")));
    assertThat(ids(where("Idx").property("s").in().value(List.of("a", "a")).and().property("t").eq().value("q")))//
        .containsExactly("a2");

    // the SQL control agrees
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Idx WHERE s IN ['a', 'a']")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(2L);
    }
  }

  @Test
  void valuesThatAreOneKeyToTheIndexReturnEachRecordOnce() {
    // different values the index converts or folds onto one key: 1 and 1L and "1" on an INTEGER key, 'A' and 'a' on a folded one
    load("Num", "CREATE INDEX ON %T (n) NOTUNIQUE", LETTERS);
    final List<Object> numbers = List.of(1, 1L, "1");
    assertThat(ids(where("Num").property("n").in().value(numbers))).containsExactly("a1", "a2");
    assertThat(where("Num").property("n").in().value(numbers).count()).isEqualTo(2);
    assertThat(where("Num").property("n").in().value(numbers).limit(2).documents().toList()).hasSize(2);
    // a mixed list is merged in key order instead of raising a ClassCastException, a value the key type cannot read matches
    // nothing, and the answer is the unindexed one
    load("PlainNum", null, LETTERS);
    final List<Object> mixed = List.of("2", 1, "abc", 3L);
    assertThat(ids(where("Num").property("n").in().value(mixed))).isEqualTo(ids(where("PlainNum").property("n").in().value(mixed)))
        .contains("a1", "a2", "c1");
    assertThat(where("Num").property("n").in().value(List.of("abc")).count()).isZero();

    load("Hash", "CREATE INDEX ON %T (n) NOTUNIQUE_HASH", LETTERS);
    assertThat(ids(where("Hash").property("n").in().value(List.of(1, 1L, "1", 2)))).containsExactly("a1", "a2", "b1", "b2");

    load("Ci", "CREATE INDEX ON %T (s COLLATE ci) NOTUNIQUE", LETTERS);
    // the residual filter is case sensitive: 'A' matches nothing, 'a' its two records, once each
    assertThat(ids(where("Ci").property("s").in().value(List.of("A", "a")))).containsExactly("a1", "a2");
    assertThat(where("Ci").property("s").in().value(List.of("A", "a")).count()).isEqualTo(2);
  }

  @Test
  void decimalsOfOneValueInTwoScalesAreOneKey() {
    // 1.0 and 1.00 differ by equals() but are one key: probed once, each record returned once, on both index kinds
    for (final String kind : List.of("NOTUNIQUE", "NOTUNIQUE_HASH")) {
      final String type = "Dec" + kind.replace("_", "");
      database.command("sql", "CREATE DOCUMENT TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".d DECIMAL");
      database.command("sql", "CREATE INDEX ON " + type + " (d) " + kind);
      database.transaction(() -> {
        database.newDocument(type).set("id", "x1").set("d", new BigDecimal("1.0")).save();
        database.newDocument(type).set("id", "x2").set("d", new BigDecimal("1")).save();
        database.newDocument(type).set("id", "y1").set("d", new BigDecimal("2.5")).save();
      });
      final List<Object> values = List.of(new BigDecimal("1.0"), new BigDecimal("1.00"), new BigDecimal("1"));
      assertThat(ids(where(type).property("d").in().value(values))).as(kind).containsExactly("x1", "x2");
      assertThat(where(type).property("d").in().value(values).count()).as(kind).isEqualTo(2);
      assertThat(where(type).property("d").in().value(values).limit(2).documents().toList()).as(kind).hasSize(2);
    }
  }

  @Test
  void zeroAndNegativeZeroAreOneKey() {
    database.command("sql", "CREATE DOCUMENT TYPE Dbl");
    database.command("sql", "CREATE PROPERTY Dbl.d DOUBLE");
    database.command("sql", "CREATE INDEX ON Dbl (d) NOTUNIQUE");
    database.transaction(() -> {
      database.newDocument("Dbl").set("id", "z1").set("d", 0.0d).save();
      database.newDocument("Dbl").set("id", "z2").set("d", -0.0d).save();
      database.newDocument("Dbl").set("id", "o1").set("d", 1.0d).save();
    });
    final List<Object> values = List.of(0.0d, -0.0d);
    assertThat(ids(where("Dbl").property("d").in().value(values))).containsExactly("z1", "z2");
    assertThat(where("Dbl").property("d").in().value(values).count()).isEqualTo(2);
  }

  @Test
  void anUnreadableDateDoesNotProbeTheNullKey() {
    // a DATETIME key reads 'abc' as null: the rows without a date must not answer it, with or without a limit
    database.command("sql", "CREATE DOCUMENT TYPE Dt");
    database.command("sql", "CREATE PROPERTY Dt.t DATETIME");
    database.command("sql", "CREATE INDEX ON Dt (t) NOTUNIQUE NULL_STRATEGY INDEX");
    final LocalDateTime when = LocalDateTime.of(2026, 1, 1, 10, 0);
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        database.newDocument("Dt").set("id", "n" + i).save();
      database.newDocument("Dt").set("id", "d1").set("t", when).save();
    });
    final List<Object> values = List.of("abc", when);
    assertThat(ids(where("Dt").property("t").in().value(values))).containsExactly("d1");
    assertThat(ids(where("Dt").property("t").in().value(values).limit(1))).containsExactly("d1");
    assertThat(where("Dt").property("t").in().value(List.of("abc")).count()).isZero();
  }

  @Test
  void emptyAndUnreadableInListsMatchNothingOnEveryIndexKind() {
    load("Lsm", "CREATE INDEX ON %T (n) NOTUNIQUE", LETTERS);
    load("Hash", "CREATE INDEX ON %T (n) NOTUNIQUE_HASH", LETTERS);
    for (final String type : List.of("Lsm", "Hash")) {
      assertThat(where(type).property("n").in().value(List.of()).count()).as(type + " IN []").isZero();
      assertThat(where(type).property("n").in().value(List.of()).documents().toList()).as(type + " IN []").isEmpty();
      assertThat(where(type).property("n").in().value(List.of("abc", "x1")).count()).as(type + " unreadable").isZero();
      assertThat(where(type).property("n").in().value(List.of("abc", "x1")).limit(1).documents().toList()).as(type + " unreadable")
          .isEmpty();
      // an unreadable value next to readable ones only drops itself
      assertThat(ids(where(type).property("n").in().value(List.of("abc", 3)))).as(type + " mixed").containsExactly("c1");
    }
  }

  @Test
  void bytesKeysThatAreEqualReturnEachRecordOnce() {
    // a byte[] key has an identity equals(), so two equal arrays are two probes of one key: the merge returns each record once,
    // and a duplicate does not count against the limit
    for (final String type : List.of("BinIdx", "BinPlain")) {
      database.getSchema().createDocumentType(type).createProperty("b", Type.BINARY);
      database.transaction(() -> {
        database.newDocument(type).set("id", "x1").set("b", new byte[] { 1, 2 }).save();
        database.newDocument(type).set("id", "x2").set("b", new byte[] { 1, 2 }).save();
        database.newDocument(type).set("id", "y1").set("b", new byte[] { 3 }).save();
      });
    }
    database.getSchema().getType("BinIdx").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "b");

    final List<Object> values = List.of(new byte[] { 1, 2 }, new byte[] { 1, 2 }, new byte[] { 3 });
    final List<String> expected = ids(where("BinPlain").property("b").in().value(values));
    assertThat(expected).containsExactly("x1", "x2", "y1");
    assertThat(ids(where("BinIdx").property("b").in().value(values))).isEqualTo(expected);
    assertThat(where("BinIdx").property("b").in().value(values).count()).isEqualTo(3);
    assertThat(where("BinIdx").property("b").in().value(values).limit(3).documents().toList()).hasSize(3);
  }
}
