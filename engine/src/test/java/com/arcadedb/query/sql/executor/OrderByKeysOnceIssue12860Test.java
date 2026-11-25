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
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.java.JavaClassFunctionLibraryDefinition;
import com.arcadedb.query.sql.parser.OrderBy;
import com.arcadedb.query.sql.parser.OrderByItem;
import com.arcadedb.serializer.BinaryComparator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12860: the SQL ORDER BY extracted each sort key inside the comparator, about 2 * log2(n) times per row. For a whole
 * record that meant locating and deserializing the property in the record's binary at every comparison. Each row's keys are now
 * extracted once, when the row enters the sort (full sort and top-k heap), and the sort compares the stored keys.
 */
class OrderByKeysOnceIssue12860Test extends TestHelper {
  private static final int ROWS = 1_000;

  /** A sort key that counts how many times it is evaluated. */
  public static class CountingKey {
    static final AtomicInteger CALLS = new AtomicInteger();

    public static long key(final long value) {
      CALLS.incrementAndGet();
      return value % 13;
    }

    /** A key far larger than the row it sorts: about 2 MB of characters. */
    public static String big(final long value) {
      return "x".repeat(2_000_000) + value;
    }
  }

  /** A projected row that counts how many times the sort reads its {@code v} column. */
  private static final class CountingRow extends ResultInternal {
    private final AtomicInteger reads;

    CountingRow(final AtomicInteger reads, final int v, final int w, final int arrival) {
      this.reads = reads;
      setProperty("v", v);
      setProperty("w", w);
      setProperty("arrival", arrival);
    }

    @Override
    public <T> T getProperty(final String name) {
      if ("v".equals(name))
        reads.incrementAndGet();
      return super.getProperty(name);
    }
  }

  private static OrderByItem item(final String alias, final String direction) {
    final OrderByItem item = new OrderByItem();
    item.setAlias(alias);
    item.setType(direction);
    return item;
  }

  /** Sorts {@code rows} through an {@link OrderByStep} fed by a plain source step, as the planner chains it. */
  private List<Result> sortThroughStep(final List<Result> rows, final Integer maxResults, final OrderByItem... items) {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(database);
    final OrderBy orderBy = new OrderBy();
    orderBy.setItems(List.of(items));
    final AbstractExecutionStep source = new AbstractExecutionStep(context) {
      private int next = 0;

      @Override
      public ResultSet syncPull(final CommandContext ctx, final int nRecords) {
        final InternalResultSet result = new InternalResultSet();
        for (int i = 0; i < nRecords && next < rows.size(); i++)
          result.add(rows.get(next++));
        return result;
      }
    };
    final OrderByStep step = new OrderByStep(orderBy, maxResults, context, -1);
    step.setPrevious(source);
    final List<Result> sorted = new ArrayList<>();
    final ResultSet rs = step.syncPull(context, rows.size() + 1);
    while (rs.hasNext())
      sorted.add(rs.next());
    step.close();
    return sorted;
  }

  private static List<Result> countingRows(final AtomicInteger reads) {
    final List<Result> rows = new ArrayList<>(ROWS);
    for (int i = 0; i < ROWS; i++)
      rows.add(new CountingRow(reads, (i * 7919) % 97, i % 3, i));
    return rows;
  }

  private static int intOf(final Result row, final String name) {
    return ((ResultInternal) row).content.get(name) instanceof Number n ? n.intValue() : -1;
  }

  /**
   * The direct statement of the fix: the sort reads each row's key column exactly once, for the full sort, for several keys, and
   * for the top-k heap including the rows it rejects. Before, the comparator read it at every comparison.
   */
  @Test
  void theSortReadsEachRowsKeyOnce() {
    final AtomicInteger reads = new AtomicInteger();

    List<Result> sorted = sortThroughStep(countingRows(reads), null, item("v", OrderByItem.ASC));
    assertThat(reads.get()).isEqualTo(ROWS);
    assertThat(sorted).hasSize(ROWS);
    for (int i = 1; i < ROWS; i++) {
      final int c = Integer.compare(intOf(sorted.get(i - 1), "v"), intOf(sorted.get(i), "v"));
      // equal keys keep their arrival order: the sort is stable
      assertThat(c < 0 || (c == 0 && intOf(sorted.get(i - 1), "arrival") < intOf(sorted.get(i), "arrival"))).as("row " + i).isTrue();
    }

    reads.set(0);
    sorted = sortThroughStep(countingRows(reads), null, item("w", OrderByItem.DESC), item("v", OrderByItem.ASC));
    assertThat(reads.get()).isEqualTo(ROWS);
    assertThat(sorted).hasSize(ROWS);
    assertThat(intOf(sorted.getFirst(), "w")).isEqualTo(2);
    assertThat(intOf(sorted.getFirst(), "v")).isZero();

    reads.set(0);
    sorted = sortThroughStep(countingRows(reads), 10, item("v", OrderByItem.DESC), item("w", OrderByItem.ASC));
    assertThat(reads.get()).isEqualTo(ROWS);
    assertThat(sorted).hasSize(10);
    assertThat(sorted).extracting(r -> intOf(r, "v")).containsOnly(96);
  }

  /** A SQL expression key over whole records is evaluated once per row, by the full sort and by the top-k heap. */
  @Test
  void anExpressionKeyIsEvaluatedOncePerRow() throws Exception {
    database.getSchema().registerFunctionLibrary(new JavaClassFunctionLibraryDefinition("counting", CountingKey.class));
    database.transaction(() -> {
      database.getSchema().createDocumentType("Counted");
      for (int i = 1; i <= 500; i++)
        database.newDocument("Counted").set("v", i).save();
    });

    CountingKey.CALLS.set(0);
    assertThat(ids("SELECT FROM Counted ORDER BY `counting.key`(v), v", "v")).hasSize(500).startsWith(13, 26, 39);
    assertThat(CountingKey.CALLS.get()).isEqualTo(500);

    CountingKey.CALLS.set(0);
    assertThat(ids("SELECT FROM Counted ORDER BY `counting.key`(v), v LIMIT 3", "v")).containsExactly(13, 26, 39);
    assertThat(CountingKey.CALLS.get()).isEqualTo(500);
  }

  /** A key that changes at every evaluation no longer breaks the comparator contract: each row draws it once. */
  @Test
  void aRandomKeySortsEveryRow() {
    createMixedData();
    final List<Integer> all = ids("SELECT FROM T", "id");
    for (int run = 0; run < 20; run++)
      assertThat(ids("SELECT FROM T ORDER BY randomInt(1000000)", "id")).containsExactlyInAnyOrderElementsOf(all);
  }

  /**
   * The keys the sort holds are charged to the heap budget with their rows: each of 10 small records is sorted by a 2 MB
   * string it does not hold, kept until the sort ends (the whole input) or for the rows the heap keeps (with LIMIT).
   */
  @Test
  void theKeysTheSortHoldsAreChargedToTheHeapBudget() throws Exception {
    database.getSchema().registerFunctionLibrary(new JavaClassFunctionLibraryDefinition("counting", CountingKey.class));
    database.transaction(() -> {
      database.getSchema().createDocumentType("Big");
      for (int i = 0; i < 10; i++)
        database.newDocument("Big").set("v", i).save();
    });
    final String sortAll = "SELECT FROM Big ORDER BY `counting.big`(v) DESC";
    final String topK = sortAll + " LIMIT 5";
    assertThat(ids(sortAll, "v")).containsExactly(9, 8, 7, 6, 5, 4, 3, 2, 1, 0);
    assertThat(ids(topK, "v")).containsExactly(9, 8, 7, 6, 5);

    final long previous = GlobalConfiguration.QUERY_MAX_HEAP_RAM.getValueAsLong();
    try {
      GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(QueryHeapBudget.getReservedBytes() / (1024 * 1024) + 4);
      for (final String query : List.of(sortAll, topK))
        assertThatThrownBy(() -> ids(query, "v")).as(query)
            .isInstanceOf(CommandExecutionException.class)
            .hasMessageContaining(GlobalConfiguration.QUERY_MAX_HEAP_RAM.getKey());
    } finally {
      GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(previous);
    }
  }

  /**
   * A column key is charged unless the row's own content holds that column: the row's estimate counts it already. A row that
   * keeps its record and projects a column ({@code SELECT *, s AS k}) holds the column; a plain record row deserializes the
   * property from its binary, which nothing else counts.
   */
  @Test
  void aColumnKeyIsChargedUnlessTheRowsContentHoldsIt() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Held");
      database.newDocument("Held").set("s", "x".repeat(100_000)).set("v", 1).save();
    });
    final long holder = 64L;
    final List<OrderByItem> byK = List.of(item("k", null));
    final List<OrderByItem> byS = List.of(item("s", null));

    try (final ResultSet rs = database.query("sql", "SELECT *, s AS k FROM Held")) {
      final Result projected = rs.next();
      assertThat(projected.isElement()).isTrue();
      final Object key = projected.getProperty("k");
      assertThat(OrderByStep.keyBytes(byK, projected, key, holder)).as("element row projecting the key column").isEqualTo(holder);
      assertThat(OrderByStep.keyBytes(List.of(item("k", null), item("v", null)), projected, new Object[] { key, 1 }, holder))
          .as("the same column among several keys, with the one the record holds")
          .isEqualTo(holder + HeapEstimator.OBJECT_BYTES + 2L * HeapEstimator.REFERENCE_BYTES);
    }

    try (final ResultSet rs = database.query("sql", "SELECT FROM Held")) {
      final Result plain = rs.next();
      final Object key = plain.getProperty("s");
      assertThat(OrderByStep.keyBytes(byS, plain, key, holder)).as("plain record row, key deserialized from the record")
          .isEqualTo(holder + HeapEstimator.estimate(key)).isGreaterThan(holder + 50_000);
    }

    try (final ResultSet rs = database.query("sql", "SELECT *, !v FROM Held")) {
      final Result excluding = rs.next();
      final Object key = excluding.getProperty("s");
      // THE STAR OF THE PROJECTION PUT THE PROPERTIES IN THE ROW'S CONTENT: THE ROW HOLDS THE KEY AND ITS ESTIMATE COUNTS IT
      assertThat(OrderByStep.keyBytes(byS, excluding, key, holder)).as("element row whose content holds the column").isEqualTo(holder);
      assertThat(HeapEstimator.estimate(excluding)).as("the row's estimate counts it").isGreaterThan(50_000);
    }
  }

  /**
   * The order is the one the per-comparison extraction gave: nulls first, mixed numeric types by value, strings, DESC, a
   * modifier, a direction parameter, several keys, ties in scan order, and the top-k heap with LIMIT and SKIP.
   */
  @Test
  void theOrderIsUnchanged() {
    createMixedData();
    final List<Result> scan = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT FROM T")) {
      rs.forEachRemaining(scan::add);
    }

    final Function<Result, Object> k = r -> r.getProperty("k");
    final Function<Result, Object> s = r -> r.getProperty("s");
    final Function<Result, Object> g = r -> r.getProperty("g");
    final Function<Result, Object> id = r -> r.getProperty("id");
    final Function<Result, Object> lower = r -> r.getProperty("s") == null ? null : r.<String>getProperty("s").toLowerCase();
    final Function<Result, Object> rid = r -> r.getIdentity().orElseThrow();

    check(scan, "SELECT FROM T ORDER BY k", Map.of(), by(k, false));
    check(scan, "SELECT id FROM T ORDER BY k", Map.of(), by(k, false));
    check(scan, "SELECT FROM T ORDER BY k DESC, id", Map.of(), by(k, true).thenComparing(by(id, false)));
    check(scan, "SELECT FROM T ORDER BY s", Map.of(), by(s, false));
    check(scan, "SELECT FROM T ORDER BY s.toLowerCase(), g DESC", Map.of(), by(lower, false).thenComparing(by(g, true)));
    check(scan, "SELECT FROM T ORDER BY g", Map.of(), by(g, false));
    check(scan, "SELECT FROM T ORDER BY g DESC, k", Map.of(), by(g, true).thenComparing(by(k, false)));
    check(scan, "SELECT FROM T ORDER BY @rid DESC", Map.of(), by(rid, true));
    check(scan, "SELECT id, k AS x FROM T ORDER BY x DESC, id", Map.of(), by(k, true).thenComparing(by(id, false)));
    check(scan, "SELECT FROM T ORDER BY k :dir", Map.of("dir", "DESC"), by(k, true));
    check(scan, "SELECT FROM T ORDER BY g :dir, k", Map.of("dir", true), by(g, false).thenComparing(by(k, false)));
  }

  /** Checks {@code query} against the stable sort of the scan by {@code expected}, whole and through the top-k heap. */
  private void check(final List<Result> scan, final String query, final Map<String, Object> params, final Comparator<Result> expected) {
    final List<Integer> expectedIds = new ArrayList<>();
    final List<Result> sorted = new ArrayList<>(scan);
    sorted.sort(expected);
    for (final Result r : sorted)
      expectedIds.add(r.getProperty("id"));

    assertThat(ids(query, "id", params)).as(query).containsExactlyElementsOf(expectedIds);
    assertThat(ids(query + " LIMIT 7", "id", params)).as(query + " LIMIT 7").containsExactlyElementsOf(expectedIds.subList(0, 7));
    assertThat(ids(query + " SKIP 23 LIMIT 31", "id", params)).as(query + " SKIP 23 LIMIT 31")
        .containsExactlyElementsOf(expectedIds.subList(23, 54));
  }

  /** The comparison rules of an ORDER BY item: nulls first, then the typed comparison, then the string fallback. */
  private static Comparator<Result> by(final Function<Result, Object> key, final boolean descending) {
    return (a, b) -> {
      final Object x = key.apply(a);
      final Object y = key.apply(b);
      int result;
      if (x == null)
        result = y == null ? 0 : -1;
      else if (y == null)
        result = 1;
      else {
        try {
          result = BinaryComparator.compareTo(x, y);
        } catch (final Exception e) {
          result = x.toString().compareTo(y.toString());
        }
      }
      result = Integer.signum(result);
      return descending ? -result : result;
    };
  }

  /** 300 rows: {@code k} mixes nulls, integers, longs and doubles, {@code s} mixes nulls and case, {@code g} has many ties. */
  private void createMixedData() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("T");
      for (int i = 0; i < 300; i++) {
        final Object k = switch (i % 5) {
          case 0 -> null;
          case 1 -> (i * 37) % 50;
          case 2 -> (long) ((i * 37) % 50);
          case 3 -> (i * 37) % 50 + 0.5;
          default -> (float) ((i * 37) % 50) - 0.25F;
        };
        final String s = i % 7 == 0 ? null : (i % 2 == 0 ? "NAME" : "name") + ((i * 13) % 20);
        database.newDocument("T").set("id", i).set("k", k).set("s", s).set("g", i % 3).save();
      }
    });
  }

  private List<Integer> ids(final String query, final String property) {
    return ids(query, property, Map.of());
  }

  /** Through command(): a user library function is not known to be read-only, so query() refuses it. */
  private List<Integer> ids(final String query, final String property, final Map<String, Object> params) {
    final List<Integer> ids = new ArrayList<>();
    try (final ResultSet rs = database.command("sql", query, params)) {
      rs.forEachRemaining(r -> ids.add(r.<Number>getProperty(property).intValue()));
    }
    return ids;
  }
}
