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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10346, against a model: random transactions of statements, some of which fail midway, are run on the database and on a plain
 * in-memory model, and after every statement what the transaction holds has to be exactly what the model says - the records, their
 * unique and plain indexes, the edges and the degrees of the vertices at both ends, the counters. A statement that fails must leave
 * the transaction as it found it, and one that succeeds must leave it as the model has it.
 * <p>
 * A transaction the engine cannot take a statement back in (it read a record an earlier statement had updated and not written yet) is
 * refused at commit, which the model follows by dropping the transaction. The run checks that this stays the exception: most
 * transactions that saw a failed statement commit.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class StatementSavepointRandomTest extends TestHelper {
  private static final int TRANSACTIONS = 60;

  /** The vertices: id to group. */
  private TreeMap<Long, Integer>  vertices = new TreeMap<>();
  /** The edges: weight (unique) to {from, to}. */
  private TreeMap<Integer, long[]> edges    = new TreeMap<>();

  private final StringBuilder history = new StringBuilder();
  /** The id of a vertex the delete listener refuses, -1 for none. */
  private       long          poisonId  = -1L;
  private long nextFreshId;
  private int  nextWeight;
  private int  failedStatements;
  private int  committedTransactionsWithAFailure;
  private int  refusedTransactionsWithAFailure;

  @BeforeEach
  void createSchema() {
    database.command("sql", "CREATE VERTEX TYPE V").close();
    database.command("sql", "CREATE PROPERTY V.id LONG").close();
    database.command("sql", "CREATE PROPERTY V.grp INTEGER").close();
    database.command("sql", "CREATE INDEX ON V (id) UNIQUE").close();
    database.command("sql", "CREATE INDEX ON V (grp) NOTUNIQUE").close();
    database.command("sql", "CREATE EDGE TYPE E").close();
    database.command("sql", "CREATE PROPERTY E.w INTEGER").close();
    database.command("sql", "CREATE INDEX ON E (w) UNIQUE").close();
    registerDeleteListener();
  }

  /** The listeners belong to the open database: a reopen drops them. */
  private void registerDeleteListener() {
    database.getSchema().getType("V").getEvents().registerListener((BeforeRecordDeleteListener) record -> {
      if (poisonId >= 0L && ((Document) record).getLong("id") == poisonId)
        throw new IllegalStateException("refused");
      return true;
    });
  }

  @ParameterizedTest
  @ValueSource(longs = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 })
  void randomTransactionsMatchTheModel(final long seed) {
    final Random random = new Random(seed);
    nextFreshId = 1000;
    nextWeight = 1;

    for (int round = 0; round < TRANSACTIONS; round++) {
      final TreeMap<Long, Integer> verticesBefore = new TreeMap<>(vertices);
      final TreeMap<Integer, long[]> edgesBefore = copy(edges);
      boolean failed = false;
      history.setLength(0);

      database.begin();
      // THE IDS A TRANSACTION CREATES COME FROM A RANGE OF ITS OWN: A CLASH WITH A ROW OF ANOTHER TRANSACTION IS RAISED AT COMMIT, NOT
      // BY THE STATEMENT, AND THE ONLY CLASHES THIS TEST WANTS ARE THE ONES THE STATEMENT ITSELF RAISES
      final long base = nextFreshId;
      nextFreshId += 40;

      for (int i = 0, statements = 2 + random.nextInt(8); i < statements; i++) {
        failed |= runRandomStatement(random, base);
        assertViewMatchesTheModel("seed " + seed + " round " + round + " statement " + i);
      }

      boolean committed;
      try {
        database.commit();
        committed = true;
      } catch (final TransactionException | DuplicatedKeyException e) {
        // REFUSED: THE TRANSACTION IS DROPPED, AND THE MODEL WITH IT
        if (database.isTransactionActive())
          database.rollback();
        committed = false;
      }

      if (!committed) {
        vertices = verticesBefore;
        edges = edgesBefore;
        if (failed)
          ++refusedTransactionsWithAFailure;
      } else if (failed)
        ++committedTransactionsWithAFailure;

      assertViewMatchesTheModel("seed " + seed + " round " + round + " after the commit");

      if (round % 20 == 19) {
        reopenDatabase();
        registerDeleteListener();
        assertViewMatchesTheModel("seed " + seed + " round " + round + " after the reopen");
      }
    }

    try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
      final Result result = rs.next();
      assertThat(result.<Number>getProperty("totalErrors").intValue()).as("CHECK DATABASE: %s", result).isZero();
    }

    assertThat(failedStatements).as("failed statements").isGreaterThan(20);
    // THE REFUSAL IS THE EXCEPTION: A TRANSACTION THAT SAW A FAILED STATEMENT MOSTLY COMMITS
    assertThat(committedTransactionsWithAFailure).as("transactions with a failed statement that committed (refused: %d)",
        refusedTransactionsWithAFailure).isGreaterThan(refusedTransactionsWithAFailure);
  }

  /** @return true when the statement failed */
  private boolean runRandomStatement(final Random random, final long base) {
    return switch (random.nextInt(6)) {
      case 0, 1 -> insertRows(random, base);
      case 2 -> updateGroup(random);
      case 3 -> shiftIds(random, base);
      case 4 -> deleteVertices(random);
      default -> createEdges(random);
    };
  }

  private boolean insertRows(final Random random, final long base) {
    final int rows = 1 + random.nextInt(6);
    final List<long[]> inserted = new ArrayList<>();
    final StringBuilder sql = new StringBuilder("INSERT INTO V (id, grp) VALUES ");
    boolean conflict = false;
    final TreeSet<Long> seen = new TreeSet<>();
    for (int i = 0; i < rows; i++) {
      final long id = base + random.nextInt(14);
      final int grp = random.nextInt(4);
      inserted.add(new long[] { id, grp });
      sql.append(i > 0 ? ", " : "").append('(').append(id).append(", ").append(grp).append(')');
      conflict |= vertices.containsKey(id) || !seen.add(id);
    }

    history.append("\n").append(sql);
    try {
      database.command("sql", sql.toString()).close();
    } catch (final DuplicatedKeyException e) {
      history.append("   -> FAILED");
      assertThat(conflict).as("%s failed with %s", sql, e.getMessage()).isTrue();
      ++failedStatements;
      return true;
    }
    assertThat(conflict).as("%s succeeded", sql).isFalse();
    for (final long[] row : inserted)
      vertices.put(row[0], (int) row[1]);
    return false;
  }

  private boolean updateGroup(final Random random) {
    if (vertices.isEmpty())
      return false;
    final List<Long> keys = new ArrayList<>(vertices.keySet());
    final List<Long> chosen = new ArrayList<>();
    for (int i = 0, n = 1 + random.nextInt(4); i < n; i++)
      chosen.add(keys.get(random.nextInt(keys.size())));
    final int grp = random.nextInt(4);
    history.append("\nUPDATE V SET grp = " + grp + " WHERE id IN " + chosen);
    database.command("sql", "UPDATE V SET grp = " + grp + " WHERE id IN " + chosen).close();
    for (final Long id : chosen)
      vertices.put(id, grp);
    return false;
  }

  private boolean shiftIds(final Random random, final long base) {
    final long from = base + random.nextInt(10);
    final long to = from + random.nextInt(5);
    final String sql = "UPDATE V SET id = id + 1 WHERE id >= " + from + " AND id <= " + to;
    history.append("\n").append(sql);
    try {
      database.command("sql", sql).close();
    } catch (final DuplicatedKeyException e) {
      history.append("   -> FAILED");
      ++failedStatements;
      return true;
    }
    final TreeMap<Long, Integer> shifted = new TreeMap<>();
    for (final Map.Entry<Long, Integer> entry : vertices.entrySet())
      shifted.put(entry.getKey() >= from && entry.getKey() <= to ? entry.getKey() + 1 : entry.getKey(), entry.getValue());
    assertThat(shifted).as("%s succeeded, so the ids stay unique", sql).hasSize(vertices.size());
    final TreeMap<Long, Long> moved = new TreeMap<>();
    for (final Long id : vertices.keySet())
      if (id >= from && id <= to)
        moved.put(id, id + 1);
    // THE EDGES FOLLOW THEIR VERTICES: THEY POINT AT THE RECORDS, NOT AT THE IDS
    for (final long[] edge : edges.values()) {
      if (moved.containsKey(edge[0]))
        edge[0] = moved.get(edge[0]);
      if (moved.containsKey(edge[1]))
        edge[1] = moved.get(edge[1]);
    }
    vertices = shifted;
    return false;
  }

  private boolean deleteVertices(final Random random) {
    if (vertices.isEmpty())
      return false;
    final List<Long> keys = new ArrayList<>(vertices.keySet());
    final List<Long> chosen = new ArrayList<>();
    for (int i = 0, n = 1 + random.nextInt(3); i < n; i++)
      chosen.add(keys.get(random.nextInt(keys.size())));
    // A THIRD OF THE DELETES MEETS A VERTEX THE LISTENER REFUSES: THE ONES BEFORE IT IN THE SCAN ARE ALREADY DELETED
    final boolean poisoned = random.nextInt(3) == 0;
    poisonId = poisoned ? chosen.get(random.nextInt(chosen.size())) : -1L;
    final String sql = "DELETE VERTEX FROM V WHERE id IN " + chosen;
    history.append("\n").append(sql).append(poisoned ? "   (refuses " + poisonId + ")" : "");
    try {
      database.command("sql", sql).close();
    } catch (final RuntimeException e) {
      history.append("   -> FAILED");
      assertThat(poisoned).as("%s failed with %s", sql, e.getMessage()).isTrue();
      ++failedStatements;
      return true;
    } finally {
      poisonId = -1L;
    }
    assertThat(poisoned).as("%s succeeded", sql).isFalse();
    for (final Long id : chosen)
      vertices.remove(id);
    edges.values().removeIf(edge -> chosen.contains(edge[0]) || chosen.contains(edge[1]));
    return false;
  }

  private boolean createEdges(final Random random) {
    if (vertices.isEmpty())
      return false;
    final List<Long> keys = new ArrayList<>(vertices.keySet());
    final long from = keys.get(random.nextInt(keys.size()));
    final TreeSet<Long> targets = new TreeSet<>();
    for (int i = 0, n = 1 + random.nextInt(3); i < n; i++)
      targets.add(keys.get(random.nextInt(keys.size())));
    final int w = nextWeight++;
    final String sql = "CREATE EDGE E FROM (SELECT FROM V WHERE id = " + from + ") TO (SELECT FROM V WHERE id IN " + targets
        + ") SET w = " + w;
    history.append("\n").append(sql);
    try {
      database.command("sql", sql).close();
    } catch (final DuplicatedKeyException e) {
      history.append("   -> FAILED");
      // THE SAME WEIGHT FOR SEVERAL EDGES: THE FIRST ONE IS WRITTEN WHEN THE SECOND IS REFUSED
      assertThat(targets.size()).as("%s failed with %s", sql, e.getMessage()).isGreaterThan(1);
      ++failedStatements;
      return true;
    }
    assertThat(targets.size()).as("%s succeeded", sql).isEqualTo(1);
    edges.put(w, new long[] { from, targets.first() });
    return false;
  }

  // ------------------------------------------------------------------------------------------------------------------------------

  private static TreeMap<Integer, long[]> copy(final TreeMap<Integer, long[]> source) {
    final TreeMap<Integer, long[]> copy = new TreeMap<>();
    for (final Map.Entry<Integer, long[]> entry : source.entrySet())
      copy.put(entry.getKey(), entry.getValue().clone());
    return copy;
  }

  private void assertViewMatchesTheModel(final String where) {
    // THE RECORDS, WITH THE DEGREES OF THE EDGES AT BOTH ENDS
    final TreeMap<Long, String> actual = new TreeMap<>();
    try (final ResultSet rs = database.query("sql", "SELECT id, grp, outE().size() AS o, inE().size() AS i FROM V")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        actual.put(row.<Number>getProperty("id").longValue(),
            row.getProperty("grp") + "/" + row.getProperty("o") + "/" + row.getProperty("i"));
      }
    }
    final TreeMap<Long, String> expected = new TreeMap<>();
    for (final Map.Entry<Long, Integer> entry : vertices.entrySet()) {
      int out = 0;
      int in = 0;
      for (final long[] edge : edges.values()) {
        if (edge[0] == entry.getKey())
          ++out;
        if (edge[1] == entry.getKey())
          ++in;
      }
      expected.put(entry.getKey(), entry.getValue() + "/" + out + "/" + in);
    }
    assertThat(actual).as("%s: vertices with their degrees%s", where, history).isEqualTo(expected);

    // THE COUNTERS
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM V")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).as("%s: count(*) of V", where).isEqualTo(vertices.size());
    }
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM E")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).as("%s: count(*) of E", where).isEqualTo(edges.size());
    }

    // THE EDGES THEMSELVES
    final TreeSet<Integer> actualWeights = new TreeSet<>();
    try (final ResultSet rs = database.query("sql", "SELECT w FROM E")) {
      while (rs.hasNext())
        actualWeights.add(rs.next().<Number>getProperty("w").intValue());
    }
    assertThat(actualWeights).as("%s: edges", where).isEqualTo(new TreeSet<>(edges.keySet()));

    // THE INDEXES: A LOOKUP BY THE UNIQUE KEY, BY THE PLAIN ONE, AND OF A WEIGHT
    for (final Map.Entry<Long, Integer> entry : vertices.entrySet())
      try (final ResultSet rs = database.query("sql", "SELECT grp FROM V WHERE id = ?", entry.getKey())) {
        assertThat(rs.hasNext()).as("%s: index lookup of id %d", where, entry.getKey()).isTrue();
        assertThat(rs.next().<Number>getProperty("grp").intValue()).as("%s: grp of id %d", where, entry.getKey())
            .isEqualTo(entry.getValue());
      }
    for (int grp = 0; grp < 4; grp++) {
      final int g = grp;
      final long expectedCount = vertices.values().stream().filter(value -> value == g).count();
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM V WHERE grp = ?", grp)) {
        assertThat(rs.next().<Number>getProperty("c").longValue()).as("%s: index count of grp %d", where, grp).isEqualTo(expectedCount);
      }
    }
    for (final Integer w : edges.keySet())
      try (final ResultSet rs = database.query("sql", "SELECT w FROM E WHERE w = ?", w)) {
        assertThat(rs.hasNext()).as("%s: index lookup of weight %d", where, w).isTrue();
      }
  }
}
