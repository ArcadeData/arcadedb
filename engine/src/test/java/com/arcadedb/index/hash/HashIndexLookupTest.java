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
package com.arcadedb.index.hash;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Hash index lookup, delete and null strategy tests (#8829, #9074, #9169).
 */
class HashIndexLookupTest extends TestHelper {
  // ===== Issue #8829 =====
  /*
   * Regression for issue #8829: deleting records of a type with a NOTUNIQUE_HASH index failed at commit once a key held a
   * few dozen records, because removing one RID from a long entry rewrote the whole entry at the end of the bucket page
   * and, when it did not fit, compacted a page that still held the old entry, so the rewrite could run past the page.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Test
  @DisplayName("#8829 delete from long entries commits")
  void deleteFromLongEntriesCommits() {
    deleteAndVerify(30_000, 100, 10);
  }

  @Test
  @DisplayName("#8829 delete from short entries commits")
  void deleteFromShortEntriesCommits() {
    deleteAndVerify(30_000, 10_000, 10);
  }

  @Test
  @DisplayName("#8829 delete everything leaves an empty index")
  void deleteEverythingLeavesAnEmptyIndex() {
    deleteAndVerify(6_000, 20, 100);
  }

  private void deleteAndVerify(final int records, final int keys, final int deletePercent) {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE H");
      database.command("sql", "CREATE PROPERTY H.g LONG");
      database.command("sql", "CREATE INDEX ON H (g) NOTUNIQUE_HASH");
    });

    final Random rnd = new Random(71);
    final List<RID> rids = new ArrayList<>();
    final int[] expected = new int[keys];
    database.begin();
    for (int i = 0; i < records; i++) {
      final int g = rnd.nextInt(keys);
      final var doc = database.newDocument("H").set("g", (long) g);
      doc.save();
      rids.add(doc.getIdentity());
      expected[g]++;
      if ((i + 1) % 1000 == 0) {
        database.commit();
        database.begin();
      }
    }
    database.commit();

    final List<RID> shuffled = new ArrayList<>(rids);
    Collections.shuffle(shuffled, rnd);
    final int toDelete = (int) ((long) records * deletePercent / 100);

    database.begin();
    for (int i = 0; i < toDelete; i++) {
      final RID rid = shuffled.get(i);
      final int g = ((Number) rid.asDocument().get("g")).intValue();
      rid.asDocument().delete();
      expected[g]--;
      if ((i + 1) % 1000 == 0 || i == toDelete - 1) {
        database.commit();
        database.begin();
      }
    }
    database.commit();

    for (int g = 0; g < Math.min(keys, 50); g++) {
      assertThat(count("SELECT count(*) AS c FROM H WHERE g = " + g)).as("index count for g=" + g).isEqualTo(expected[g]);
      assertThat(count("SELECT count(*) AS c FROM H WHERE g + 0 = " + g)).as("scan count for g=" + g).isEqualTo(expected[g]);
    }
    assertThat(count("SELECT count(*) AS c FROM H")).isEqualTo(records - toDelete);

    // the buckets now hold dead space: inserting into them again must reclaim it and stay consistent
    database.transaction(() -> {
      for (int i = 0; i < records / 2; i++)
        database.newDocument("H").set("g", (long) (i % keys)).save();
    });
    for (int g = 0; g < Math.min(keys, 50); g++) {
      final int extra = records / 2 / keys + (g < (records / 2) % keys ? 1 : 0);
      assertThat(count("SELECT count(*) AS c FROM H WHERE g = " + g)).as("index count after reinsert for g=" + g)
          .isEqualTo(expected[g] + extra);
      assertThat(count("SELECT count(*) AS c FROM H WHERE g + 0 = " + g)).as("scan count after reinsert for g=" + g)
          .isEqualTo(expected[g] + extra);
    }
  }

  private long count(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  // ===== Issue #9074 =====
  /*
   * Regression for issue #9074: a HASH index created with NULL_STRATEGY ERROR accepted records whose indexed property was
   * null or missing, while an LSM index refuses them at commit. The strategy was stored on the bucket but never checked.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @ParameterizedTest
  @ValueSource(strings = { "NOTUNIQUE_HASH", "UNIQUE_HASH", "NOTUNIQUE", "UNIQUE" })
  @DisplayName("#9074 error strategy rejects null and missing keys")
  void errorStrategyRejectsNullAndMissingKeys(final String indexType) {
    createType(indexType, "ERROR");

    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO T SET b = 1").close()))
        .isInstanceOf(TransactionException.class);
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO T SET b = 2, a = null").close()))
        .isInstanceOf(TransactionException.class);
    assertThat(countAsLong("SELECT count(*) AS c FROM T")).isZero();

    database.transaction(() -> database.command("sql", "INSERT INTO T SET b = 3, a = 'x'").close());
    assertThat(countAsLong("SELECT count(*) AS c FROM T WHERE a = 'x'")).isEqualTo(1);
  }

  @ParameterizedTest
  @ValueSource(strings = { "NOTUNIQUE_HASH", "UNIQUE_HASH" })
  @DisplayName("#9074 error strategy rejects an update to null")
  void errorStrategyRejectsAnUpdateToNull(final String indexType) {
    createType(indexType, "ERROR");
    database.transaction(() -> database.command("sql", "INSERT INTO T SET b = 3, a = 'x'").close());

    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "UPDATE T SET a = null WHERE b = 3").close()))
        .isInstanceOf(TransactionException.class);
    assertThat(countAsLong("SELECT count(*) AS c FROM T WHERE a = 'x'")).isEqualTo(1);
  }

  @ParameterizedTest
  @ValueSource(strings = { "NOTUNIQUE_HASH", "UNIQUE_HASH" })
  @DisplayName("#9074 skip strategy keeps accepting nulls")
  void skipStrategyKeepsAcceptingNulls(final String indexType) {
    createType(indexType, "SKIP");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET b = 1").close();
      database.command("sql", "INSERT INTO T SET b = 2, a = null").close();
      database.command("sql", "INSERT INTO T SET b = 3, a = 'x'").close();
    });
    assertThat(countAsLong("SELECT count(*) AS c FROM T")).isEqualTo(3);
    assertThat(countAsLong("SELECT count(*) AS c FROM T WHERE a = 'x'")).isEqualTo(1);
  }

  @ParameterizedTest
  @ValueSource(strings = { "NOTUNIQUE_HASH", "UNIQUE_HASH" })
  @DisplayName("#9074 index strategy keeps accepting nulls")
  void indexStrategyKeepsAcceptingNulls(final String indexType) {
    createType(indexType, "INDEX");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET b = 1").close();
      database.command("sql", "INSERT INTO T SET b = 3, a = 'x'").close();
    });
    assertThat(countAsLong("SELECT count(*) AS c FROM T")).isEqualTo(2);
    assertThat(countAsLong("SELECT count(*) AS c FROM T WHERE a IS NULL")).isEqualTo(1);
  }

  /** The non transactional put of the index API: the one branch the SQL statements do not reach. */
  @ParameterizedTest
  @ValueSource(strings = { "NOTUNIQUE_HASH", "UNIQUE_HASH" })
  @DisplayName("#9074 error strategy rejects a direct put of a null key")
  void errorStrategyRejectsADirectPutOfANullKey(final String indexType) {
    createType(indexType, "ERROR");
    final IndexInternal index = database.getSchema().getType("T").getAllIndexes(false).iterator().next().getSubIndexes().get(0);

    assertThatThrownBy(() -> index.put(new Object[] { null }, new RID[] { new RID(1, 0) })).isInstanceOf(
        IllegalArgumentException.class).hasMessageContaining("cannot be NULL");
  }

  /** Rows already indexed under a null key (the strategy used not to be enforced) must stay deletable after the upgrade. */
  @ParameterizedTest
  @ValueSource(strings = { "NOTUNIQUE_HASH", "UNIQUE_HASH" })
  @DisplayName("#9074 rows already indexed under a null key can still be deleted")
  void rowsAlreadyIndexedUnderANullKeyCanStillBeDeleted(final String indexType) {
    createType(indexType, "INDEX");
    database.transaction(() -> database.command("sql", "INSERT INTO T SET b = 1").close());
    final IndexInternal index = database.getSchema().getType("T").getAllIndexes(false).iterator().next().getSubIndexes().get(0);
    index.setNullStrategy(LSMTreeIndexAbstract.NULL_STRATEGY.ERROR);

    database.transaction(() -> database.command("sql", "DELETE FROM T WHERE b = 1").close());
    assertThat(countAsLong("SELECT count(*) AS c FROM T")).isZero();
  }

  /** Building an ERROR index over rows that already hold nulls fails the same way for a hash and for an LSM index. */
  @ParameterizedTest
  @ValueSource(strings = { "NOTUNIQUE_HASH", "NOTUNIQUE" })
  @DisplayName("#9074 building over existing nulls fails like lsm")
  void buildingOverExistingNullsFailsLikeLsm(final String indexType) {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.command("sql", "CREATE PROPERTY T.a STRING");
      database.command("sql", "INSERT INTO T SET b = 1").close();
    });

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON T (a) " + indexType + " NULL_STRATEGY ERROR").close())
        .hasStackTraceContaining("cannot be NULL");
  }

  private void createType(final String indexType, final String nullStrategy) {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.command("sql", "CREATE PROPERTY T.a STRING");
      database.command("sql", "CREATE INDEX ON T (a) " + indexType + " NULL_STRATEGY " + nullStrategy).close();
    });
  }

  private long countAsLong(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().<Long>getProperty("c");
    }
  }

  // ===== Issue #9169 =====
  /*
   * Issue #9169 asks whether an id that is only read, updated and deleted by equality should use UNIQUE_HASH. This pins the
   * part the answer depends on: every one of those statements, from SQL and from openCypher, is answered by the hash index
   * (and not by a scan) and gives the right result, including the delete, for the unique key of a graph and of a table.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static final int ROWS = 2_000;

  @Test
  @DisplayName("#9169 sql equality statements use the hash index")
  void sqlEqualityStatementsUseTheHashIndex() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Crud");
      database.command("sql", "CREATE PROPERTY Crud.ckey LONG");
      database.command("sql", "CREATE INDEX ON Crud (ckey) UNIQUE_HASH").close();
      for (int i = 0; i < ROWS; i++)
        database.command("sql", "INSERT INTO Crud SET ckey = ?, pkey = ?, qty = ?", i, i * 2, i % 7).close();
    });

    assertThat(plan("sql", "EXPLAIN SELECT ckey, pkey, qty FROM Crud WHERE ckey = 42")).contains("FETCH FROM INDEX Crud[ckey]");
    assertThat(plan("sql", "EXPLAIN DELETE FROM Crud WHERE ckey = 42")).contains("FETCH FROM INDEX Crud[ckey]");

    try (final ResultSet rs = database.query("sql", "SELECT ckey, pkey, qty FROM Crud WHERE ckey = :c", Map.of("c", 42))) {
      assertThat(rs.next().<Integer>getProperty("pkey")).isEqualTo(84);
      assertThat(rs.hasNext()).isFalse();
    }

    database.transaction(() -> database.command("sql", "DELETE FROM Crud WHERE ckey = :c", Map.of("c", 42)).close());
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Crud WHERE ckey = 42")) {
      assertThat(rs.next().<Long>getProperty("c")).isZero();
    }
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Crud")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(ROWS - 1);
    }
  }

  @Test
  @DisplayName("#9169 cypher equality statements use the hash index")
  void cypherEqualityStatementsUseTheHashIndex() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Person");
      database.command("sql", "CREATE PROPERTY Person.id LONG");
      database.command("sql", "CREATE INDEX ON Person (id) UNIQUE_HASH").close();
      for (int i = 0; i < ROWS; i++)
        database.command("opencypher", "CREATE (:Person {id: $id, name: 'n' + $id, age: $age})",
            Map.of("id", i, "age", i % 90)).close();
    });

    try (final ResultSet rs = database.query("opencypher", "PROFILE MATCH (p:Person) WHERE p.id = 42 RETURN p.name AS name")) {
      rs.stream().count();
      assertThat(rs.getExecutionPlan()).isPresent();
      assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).contains("NodeIndexSeek");
    }

    try (final ResultSet rs = database.query("opencypher", "MATCH (p:Person) WHERE p.id = $id RETURN p.name AS name, p.age AS age",
        Map.of("id", 42))) {
      final Result row = rs.next();
      assertThat(row.<String>getProperty("name")).isEqualTo("n42");
      assertThat(rs.hasNext()).isFalse();
    }

    database.transaction(() -> database.command("opencypher", "MATCH (q:Person) WHERE q.id = $id DETACH DELETE q",
        Map.of("id", 42)).close());
    try (final ResultSet rs = database.query("opencypher", "MATCH (p:Person) WHERE p.id = 42 RETURN count(p) AS c")) {
      assertThat(rs.next().<Long>getProperty("c")).isZero();
    }
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Person")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(ROWS - 1);
    }
  }

  private String plan(final String language, final String statement) {
    try (final ResultSet rs = database.command(language, statement)) {
      final StringBuilder sb = new StringBuilder();
      while (rs.hasNext()) {
        final Result r = rs.next();
        sb.append(r.toJSON());
      }
      return sb.toString();
    }
  }
}
