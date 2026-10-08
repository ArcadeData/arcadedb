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
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.ImmutableDocument;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Dictionary;
import com.arcadedb.function.sql.math.SQLFunctionAverage;
import com.arcadedb.function.sql.math.SQLFunctionSum;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9496: a GROUP BY cost about 170 ns of CPU per aggregate per row. The aggregation now evaluates the projection
 * computing the aggregates' arguments into an array bound to the aggregates once per query, reads every property of a
 * record once per row, keeps the running sums of {@code sum()} and {@code avg()} unboxed, keys an integral GROUP BY value
 * as a Long rather than a BigDecimal, and no longer asks the command context about the columns the planner generates. These
 * tests pin that every one of those changes answers what the code before them answered - in the parallel aggregation and
 * in the sequential one - and the bug found on the way: a database global variable named like a generated column
 * shadowed it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9496GroupByAggregateTest extends TestHelper {
  private static final int      ROWS  = 20_000;
  private static final String[] FLAGS = { "A", "N", "R" };

  // EVERY VALUE IS A MULTIPLE OF 1/4 WELL UNDER 2^40, SO EVERY SUM IS EXACT IN WHATEVER ORDER THE WORKERS ADD
  private static final String Q1 =
      "SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base, "
          + "sum(l_extendedprice * (1 - l_discount)) AS sum_disc, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, "
          + "avg(l_quantity) AS avg_qty, avg(l_extendedprice) AS avg_price, avg(l_discount) AS avg_disc, count(*) AS n "
          + "FROM LineItem WHERE l_shipdate <= '1998-09-02' GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus";

  @Override
  protected void beginTest() {
    // SMALL UNITS, SO THE FIXTURE'S FEW HUNDRED PAGES ARE CUT IN MANY OF THEM AND THE AGGREGATION RUNS IN THE WORKERS
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN_PAGES_PER_UNIT, 2);

    final DocumentType type = database.getSchema().buildDocumentType().withName("LineItem").withTotalBuckets(3).create();
    type.createProperty("l_partkey", Type.LONG);
    type.createProperty("l_quantity", Type.DOUBLE);
    type.createProperty("l_extendedprice", Type.DOUBLE);
    type.createProperty("l_discount", Type.DOUBLE);
    type.createProperty("l_tax", Type.DOUBLE);
    type.createProperty("l_count", Type.INTEGER);
    type.createProperty("l_returnflag", Type.STRING);
    type.createProperty("l_linestatus", Type.STRING);
    type.createProperty("l_shipdate", Type.STRING);

    final Random rnd = new Random(9496);
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++) {
        final MutableDocument doc = database.newDocument("LineItem").set("l_partkey", (long) (1 + rnd.nextInt(700)),
            "l_quantity", (double) (1 + rnd.nextInt(50)), "l_extendedprice", rnd.nextInt(400_000) / 4.0,
            "l_discount", rnd.nextInt(4) / 4.0, "l_tax", rnd.nextInt(2) / 2.0, "l_count", rnd.nextInt(1000),
            "l_returnflag", FLAGS[rnd.nextInt(FLAGS.length)], "l_linestatus", rnd.nextBoolean() ? "O" : "F",
            "l_shipdate", String.format("199%d-%02d-%02d", 2 + rnd.nextInt(7), 1 + rnd.nextInt(12), 1 + rnd.nextInt(28)));
        // ONE ROW IN TEN HAS NO DISCOUNT AT ALL, ONE IN TEN A NULL TAX: THE AGGREGATES SKIP NULLS, THE ARITHMETIC PROPAGATES THEM
        if (i % 10 == 3)
          doc.remove("l_discount");
        if (i % 10 == 7)
          doc.set("l_tax", null);
        doc.save();
      }
    });
  }

  /** TPC-H Q1 as in the issue: every group checked against sums computed here, in the parallel and the sequential path. */
  @Test
  void q1MatchesTheExpectedAggregatesInBothPaths() {
    final Map<String, double[]> expected = new HashMap<>();
    try (final ResultSet rs = database.query("sql", "SELECT FROM LineItem")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        if (row.<String>getProperty("l_shipdate").compareTo("1998-09-02") > 0)
          continue;
        final String key = row.getProperty("l_returnflag") + "|" + row.getProperty("l_linestatus");
        final double[] acc = expected.computeIfAbsent(key, k -> new double[9]);
        final double quantity = row.getProperty("l_quantity");
        final double price = row.getProperty("l_extendedprice");
        final Double discount = row.getProperty("l_discount");
        final Double tax = row.getProperty("l_tax");
        // IN ARCADEDB SQL A NULL OPERAND OF + AND - IS IGNORED: 1 - null IS 1
        final double discounted = discount == null ? 1 : 1 - discount;
        final double taxed = tax == null ? 1 : 1 + tax;
        acc[0] += quantity;
        acc[1] += price;
        acc[2] += price * discounted;
        acc[3] += price * discounted * taxed;
        if (discount != null) {
          acc[6] += discount;
          acc[7]++;
        }
        acc[4]++;
      }
    }

    for (final boolean parallel : new boolean[] { true, false }) {
      final List<Result> rows = query(Q1, parallel);
      assertThat(rows).hasSize(expected.size());
      for (final Result row : rows) {
        final double[] acc = expected.get(row.getProperty("l_returnflag") + "|" + row.getProperty("l_linestatus"));
        assertThat(acc).isNotNull();
        assertThat(row.<Double>getProperty("sum_qty")).isEqualTo(acc[0]);
        assertThat(row.<Double>getProperty("sum_base")).isEqualTo(acc[1]);
        assertThat(row.<Double>getProperty("sum_disc")).isEqualTo(acc[2]);
        assertThat(row.<Double>getProperty("sum_charge")).isEqualTo(acc[3]);
        assertThat(row.<Long>getProperty("n")).isEqualTo((long) acc[4]);
        assertThat(row.<Double>getProperty("avg_qty")).isEqualTo(acc[0] / acc[4]);
        assertThat(row.<Double>getProperty("avg_price")).isEqualTo(acc[1] / acc[4]);
        assertThat(row.<Double>getProperty("avg_disc")).isEqualTo(acc[6] / acc[7]);
      }
    }
  }

  /** The shapes the evaluator binds to columns and the ones it evaluates on the row: the paths agree on every one. */
  @Test
  void everyAggregationShapeAnswersTheSameInBothPaths() {
    final String[] queries = {
        "SELECT l_partkey, sum(l_extendedprice * (1 - l_discount)) AS rev FROM LineItem GROUP BY l_partkey ORDER BY rev DESC, l_partkey LIMIT 10",
        "SELECT l_returnflag, sum(l_count) AS s, avg(l_count) AS a, min(l_count) AS mi, max(l_count) AS ma, count(l_discount) AS c "
            + "FROM LineItem GROUP BY l_returnflag ORDER BY l_returnflag",
        "SELECT l_partkey % 7 AS pk7, count(*) AS n, sum(l_quantity) AS q FROM LineItem GROUP BY l_partkey % 7 ORDER BY pk7",
        "SELECT sum(l_quantity) AS q, count(*) AS n, avg(l_tax) AS t FROM LineItem WHERE l_discount > 0.25",
        "SELECT l_linestatus, sum($current.l_quantity) AS q, count(*) AS n FROM LineItem GROUP BY l_linestatus ORDER BY l_linestatus",
        "SELECT l_returnflag, l_returnflag AS again, max(l_shipdate) AS last, min(l_extendedprice) AS cheapest FROM LineItem "
            + "GROUP BY l_returnflag ORDER BY l_returnflag",
        "SELECT l_returnflag, sum(l_quantity) + count(*) AS mixed FROM LineItem GROUP BY l_returnflag ORDER BY mixed",
        "SELECT l_returnflag, count(*) AS n FROM LineItem GROUP BY l_returnflag ORDER BY count(*) DESC, l_returnflag",
        "SELECT l_linestatus, count(DISTINCT l_returnflag) AS flags, sum(DISTINCT l_tax) AS taxes FROM LineItem "
            + "GROUP BY l_linestatus ORDER BY l_linestatus",
        "SELECT l_returnflag, l_linestatus, count(*) AS n FROM LineItem GROUP BY l_returnflag, l_linestatus LIMIT 3",
        "SELECT l_returnflag, sum(missing) AS nothing, count(missing) AS none, avg(missing) AS noavg FROM LineItem "
            + "GROUP BY l_returnflag ORDER BY l_returnflag" };
    for (final String query : queries)
      assertThat(render(query(query, true))).as(query).isNotEmpty().isEqualTo(render(query(query, false)));
  }

  /**
   * Schema-less records whose values change type from row to row inside one group - Integer, Long, Double and Short - plus
   * a list and an embedded document: the running sums widen the way Type.increment does, the cache decodes the list and
   * the embedded document again on every read, and both paths agree. The doubles are halves and every value differs from
   * every other, so no sum depends on the order the workers add it in and no min() or max() picks between equal values.
   */
  @Test
  void schemaLessValuesOfMixedTypesListsAndEmbeddedDocumentsAggregateAlikeInBothPaths() {
    database.getSchema().createDocumentType("Mixed", 2);
    database.getSchema().createDocumentType("Emb");
    final Map<String, Double> expectedSums = new HashMap<>();
    database.transaction(() -> {
      for (int i = 0; i < 5_000; i++) {
        final Number v = switch (i % 4) {
          case 0 -> i;
          case 1 -> (long) i;
          case 2 -> i + 0.5;
          default -> (short) i;
        };
        final String k = "g" + (i % 5);
        expectedSums.merge(k, v.doubleValue(), Double::sum);
        final MutableDocument doc = database.newDocument("Mixed").set("k", k, "v", v, "tags", List.of(i % 3, i % 7));
        doc.newEmbeddedDocument("Emb", "emb").set("x", i % 11);
        doc.save();
      }
    });

    for (final boolean parallel : new boolean[] { true, false })
      for (final Result row : query("SELECT k, sum(v) AS s FROM Mixed GROUP BY k ORDER BY k", parallel))
        assertThat(row.<Number>getProperty("s").doubleValue()).isEqualTo(expectedSums.get(row.<String>getProperty("k")));

    final String[] queries = {
        "SELECT k, sum(v) AS s, avg(v) AS a, min(v) AS mi, max(v) AS ma, count(v) AS c FROM Mixed GROUP BY k ORDER BY k",
        "SELECT k, sum(tags) AS st, max(emb.x) AS mx, sum(emb.x) AS sx, count(DISTINCT emb.x) AS dx FROM Mixed GROUP BY k ORDER BY k",
        "SELECT k, list(ifnull(v, 0)).size() AS n, sum(ifnull(v, 0)) AS s FROM Mixed GROUP BY k ORDER BY k",
        "SELECT tags.size() AS sz, count(*) AS n FROM Mixed GROUP BY tags.size() ORDER BY sz",
        "SELECT emb.x AS x, sum(v) AS s, count(*) AS n FROM Mixed GROUP BY emb.x ORDER BY x",
        "SELECT v % 3 AS r, count(*) AS n, sum(v) AS s FROM Mixed GROUP BY v % 3 ORDER BY r" };
    for (final String query : queries)
      assertThat(render(query(query, true))).as(query).isNotEmpty().isEqualTo(render(query(query, false)));
  }

  /**
   * The property cache of a row holds 64 names: an aggregation reading more properties than that reads the others off the
   * row, as before, and still sees every value.
   */
  @Test
  void anAggregationReadingMorePropertiesThanTheCacheHoldsStillReadsThemAll() {
    database.getSchema().createDocumentType("Wide", 2);
    final int properties = 80;
    database.transaction(() -> {
      for (int i = 0; i < 2_000; i++) {
        final MutableDocument doc = database.newDocument("Wide").set("k", i % 3);
        for (int p = 0; p < properties; p++)
          doc.set("p" + p, p);
        doc.save();
      }
    });

    final StringBuilder sum = new StringBuilder();
    for (int p = 0; p < properties; p++)
      sum.append(p == 0 ? "" : " + ").append("p").append(p);
    final String query = "SELECT k, sum(" + sum + ") AS s, count(*) AS n FROM Wide GROUP BY k ORDER BY k";
    final long perRow = (long) properties * (properties - 1) / 2;
    for (final boolean parallel : new boolean[] { true, false })
      for (final Result row : query(query, parallel))
        assertThat(row.<Number>getProperty("s").longValue()).isEqualTo(perRow * row.<Long>getProperty("n"));
  }

  /**
   * Integers, longs, doubles, decimals, floats and nulls in one column: every aggregate answers the same number in both
   * paths, a null key is one group, and the boxed sums (a float or a decimal joining) widen as Type.increment does. The
   * values are small quarters, exact in every type the sums pass through in any order; numbers are compared by value,
   * since a decimal sum's scale follows the order it was added in.
   */
  @Test
  void decimalsFloatsAndNullsAggregateAlikeInBothPaths() {
    database.getSchema().createDocumentType("Decimals", 2);
    database.transaction(() -> {
      for (int i = 0; i < 3_000; i++) {
        final Object w = switch (i % 6) {
          case 0 -> i % 97;
          case 1 -> (long) (i % 89);
          case 2 -> (i % 83) + 0.25;
          case 3 -> BigDecimal.valueOf(i % 79).add(new BigDecimal("0.50"));
          case 4 -> (i % 73) + 0.75f;
          default -> null;
        };
        database.newDocument("Decimals").set("k", "g" + (i % 4), "w", w).save();
      }
    });

    final String query = "SELECT k, sum(w) AS s, avg(w) AS a, min(w) AS mi, max(w) AS ma, count(w) AS c, count(*) AS n "
        + "FROM Decimals GROUP BY k ORDER BY k";
    final List<Result> parallel = query(query, true);
    final List<Result> sequential = query(query, false);
    assertThat(parallel).hasSize(4).hasSameSizeAs(sequential);
    for (int i = 0; i < parallel.size(); i++)
      for (final String column : new String[] { "s", "a", "mi", "ma", "c", "n" })
        assertThat(new BigDecimal(parallel.get(i).getProperty(column).toString()))
            .as("%s of %s", column, parallel.get(i).<String>getProperty("k"))
            .isEqualByComparingTo(new BigDecimal(sequential.get(i).getProperty(column).toString()));

    final String byValue = "SELECT w, count(*) AS n FROM Decimals GROUP BY w ORDER BY n DESC, w LIMIT 5";
    assertThat(render(query(byValue, true))).isEqualTo(render(query(byValue, false)));
    for (final boolean inParallel : new boolean[] { true, false })
      assertThat(query("SELECT count(*) AS n FROM Decimals WHERE w IS NULL GROUP BY w", inParallel).getFirst().<Long>getProperty("n"))
          .isEqualTo(500L);
  }

  /** A parallel aggregation whose filter keeps no row answers no group, or the one empty group of an aggregation without GROUP BY. */
  @Test
  void aParallelAggregationOverNoRowAnswersAsTheSequentialOne() {
    for (final boolean parallel : new boolean[] { true, false }) {
      assertThat(query("SELECT l_returnflag, count(*) AS n, sum(l_quantity) AS q FROM LineItem WHERE l_quantity < 0 GROUP BY l_returnflag",
          parallel)).isEmpty();
      final List<Result> single = query("SELECT count(*) AS n, sum(l_quantity) AS q FROM LineItem WHERE l_quantity < 0", parallel);
      assertThat(single).hasSize(1);
      assertThat(single.getFirst().<Long>getProperty("n")).isEqualTo(0L);
      assertThat(single.getFirst().<Object>getProperty("q")).isNull();
    }
  }

  /** An aggregate that keeps every value, which only the sequential path runs, still sees every row. */
  @Test
  void aggregatesThatKeepEveryValueStillSeeEveryRow() {
    final List<Result> rows = query(
        "SELECT l_returnflag, list(l_count).size() AS listed, count(*) AS n FROM LineItem GROUP BY l_returnflag", true);
    assertThat(rows).hasSize(FLAGS.length);
    for (final Result row : rows)
      assertThat(row.<Number>getProperty("listed").longValue()).isEqualTo(row.<Long>getProperty("n"));
  }

  /**
   * The planner names the columns an aggregate is split across {@code _$$$OALIAS$$$_n}, and the aggregate read them
   * back through the context first, down to the database's global variables: a global variable of that name answered
   * for the column on every row.
   */
  @Test
  void aGlobalVariableNamedLikeAGeneratedColumnDoesNotShadowIt() {
    final String query = "SELECT l_returnflag, sum(l_count) AS s FROM LineItem GROUP BY l_returnflag ORDER BY l_returnflag";
    final List<String> before = render(query(query, false));

    // sum(l_count) IS SPLIT INTO l_count AS _$$$OALIAS$$$_1 AND sum(_$$$OALIAS$$$_1) AS _$$$OALIAS$$$_0
    ((DatabaseInternal) database).setGlobalVariable("_$$$OALIAS$$$_1", 1_000_000);
    ((DatabaseInternal) database).setGlobalVariable("_$$$OALIAS$$$_0", -1);
    try {
      assertThat(render(query(query, false))).isEqualTo(before);
      assertThat(render(query(query, true))).isEqualTo(before);
    } finally {
      ((DatabaseInternal) database).setGlobalVariable("_$$$OALIAS$$$_1", null);
      ((DatabaseInternal) database).setGlobalVariable("_$$$OALIAS$$$_0", null);
    }
  }

  /** The running sum of sum() and avg() answers what Type.increment() folded over the same values answers, type included. */
  @Test
  void runningSumsAnswerWhatTypeIncrementAnswers() {
    final Random rnd = new Random(42);
    for (int round = 0; round < 2_000; round++) {
      final List<Number> values = new ArrayList<>();
      final int size = rnd.nextInt(12);
      for (int i = 0; i < size; i++)
        values.add(randomNumber(rnd));

      final SQLFunctionSum sum = new SQLFunctionSum();
      final SQLFunctionSum firstHalf = new SQLFunctionSum();
      final SQLFunctionSum secondHalf = new SQLFunctionSum();
      final SQLFunctionAverage avg = new SQLFunctionAverage();
      Number folded = null;
      Number foldedFirst = null;
      Number foldedSecond = null;
      for (int i = 0; i < values.size(); i++) {
        final Number value = values.get(i);
        sum.aggregate(null, new Object[] { value }, null);
        avg.aggregate(null, new Object[] { value }, null);
        (i < size / 2 ? firstHalf : secondHalf).aggregate(null, new Object[] { value }, null);
        if (value != null) {
          folded = folded == null ? value : Type.increment(folded, value);
          if (i < size / 2)
            foldedFirst = foldedFirst == null ? value : Type.increment(foldedFirst, value);
          else
            foldedSecond = foldedSecond == null ? value : Type.increment(foldedSecond, value);
        }
      }
      assertThat(sum.getResult()).as("sum of %s", values).isEqualTo(folded);
      if (folded != null)
        assertThat(sum.getResult().getClass()).as("type of the sum of %s", values).isEqualTo(folded.getClass());

      // A MERGE OF TWO PARTIAL SUMS IS THEIR Type.increment(), AS IT ALWAYS WAS
      firstHalf.mergePartial(secondHalf);
      final Number merged = foldedFirst == null ? foldedSecond : foldedSecond == null ? foldedFirst : Type.increment(foldedFirst, foldedSecond);
      assertThat(firstHalf.getResult()).as("merged sum of %s", values).isEqualTo(merged);

      final long count = values.stream().filter(v -> v != null).count();
      if (count == 0)
        assertThat(avg.getResult()).isNull();
      else if (!(folded instanceof BigDecimal))
        assertThat(avg.getResult()).as("avg of %s", values).isEqualTo(folded.doubleValue() / count);
    }
  }

  /** Integers sum to an Integer, then to a Long past the int range, then to a BigDecimal past the long one. */
  @Test
  void integralSumsWidenAsBefore() {
    final SQLFunctionSum sum = new SQLFunctionSum();
    sum.aggregate(null, new Object[] { Integer.MAX_VALUE - 1 }, null);
    sum.aggregate(null, new Object[] { 1 }, null);
    assertThat(sum.getResult()).isEqualTo(Integer.MAX_VALUE);
    sum.aggregate(null, new Object[] { 1 }, null);
    assertThat(sum.getResult()).isEqualTo((long) Integer.MAX_VALUE + 1);
    sum.aggregate(null, new Object[] { Long.MAX_VALUE }, null);
    assertThat(sum.getResult()).isEqualTo(BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.valueOf((long) Integer.MAX_VALUE + 1)));
    sum.aggregate(null, new Object[] { 0.5 }, null);
    assertThat(sum.getResult()).isInstanceOf(BigDecimal.class);

    final SQLFunctionSum doubles = new SQLFunctionSum();
    doubles.aggregate(null, new Object[] { 3 }, null);
    doubles.aggregate(null, new Object[] { 0.5 }, null);
    doubles.aggregate(null, new Object[] { 2L }, null);
    assertThat(doubles.getResult()).isEqualTo(5.5);

    // A DOUBLE MEETING A DECIMAL AND A FLOAT: THE BOXED SUM CARRIES ON AS Type.increment() DOES
    final SQLFunctionSum mixed = new SQLFunctionSum();
    final Number[] values = { 0.5, new BigDecimal("0.25"), 0.25f, 2, (short) 1, 3L };
    Number folded = null;
    for (final Number value : values) {
      mixed.aggregate(null, new Object[] { value }, null);
      folded = folded == null ? value : Type.increment(folded, value);
    }
    assertThat(mixed.getResult()).isInstanceOf(BigDecimal.class).isEqualTo(folded);

    final SQLFunctionSum floats = new SQLFunctionSum();
    floats.aggregate(null, new Object[] { 2 }, null);
    floats.aggregate(null, new Object[] { 0.25f }, null);
    floats.aggregate(null, new Object[] { (short) 3 }, null);
    assertThat(floats.getResult()).isInstanceOf(Float.class).isEqualTo(Type.increment(Type.increment(2, 0.25f), (short) 3));
  }

  /**
   * The positions a header walk found belong to the content it walked: once the record is reloaded with another content -
   * here a concurrent update that added a property before the ones located - a read with them looks the property up again
   * and answers the new content, never bytes of the old layout.
   */
  @Test
  void aPropertyLocatedBeforeAReloadIsReadFromTheReloadedContent() {
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("LineItem").set("l_quantity", 1.0, "l_returnflag", "A").save().getIdentity());

    final ImmutableDocument document = (ImmutableDocument) database.lookupByRID(rid[0], true);
    final Dictionary dictionary = database.getSchema().getDictionary();
    final int quantityId = dictionary.getIdByName("l_quantity", false);
    final int flagId = dictionary.getIdByName("l_returnflag", false);
    final int[] slotByNameId = new int[Math.max(quantityId, flagId) + 1];
    Arrays.fill(slotByNameId, -1);
    slotByNameId[quantityId] = 0;
    slotByNameId[flagId] = 1;
    final int[] positions = new int[2];

    final Binary located = document.locateProperties(slotByNameId, positions, 2);
    assertThat(located).isNotNull();
    assertThat(document.getPropertyAt(located, "l_quantity", positions[0], null)).isEqualTo(1.0);

    database.transaction(() -> database.lookupByRID(rid[0], true).asDocument().modify().set("l_partkey", 7L, "l_quantity", 2.0).save());
    document.reload();

    assertThat(document.getPropertyAt(located, "l_quantity", positions[0], null)).isEqualTo(2.0);
    assertThat(document.getPropertyAt(located, "l_returnflag", positions[1], null)).isEqualTo("A");
  }

  /** A record whose header does not read answers through the cache exactly what it answers through its row. */
  @Test
  void aDamagedRecordAnswersThroughTheCacheAsThroughItsRow() {
    // A HEADER END OFFSET PAST THE END OF THE CONTENT
    final Binary header = new Binary(new byte[] { 0x7F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x01 });
    assertThat(((DatabaseInternal) database).getSerializer().locateProperties(header, new int[] { 0 }, new int[1], 1)).isFalse();

    final Binary content = new Binary(new byte[] { Document.RECORD_TYPE, 0x7F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x01, 0x02 });
    final ImmutableDocument damaged = (ImmutableDocument) ((DatabaseInternal) database).getRecordFactory()
        .newImmutableRecord(database, database.getSchema().getType("LineItem"), new RID(1, 0L), content, null);
    final Object absent = new Object();
    final Result cached = new PropertyCachingResult(database).of(new ResultInternal(damaged));
    assertThat(cached).isInstanceOf(PropertyCachingResult.class);
    assertThat(cached.getPropertyIfPresent("l_quantity", absent)).isSameAs(new ResultInternal(damaged).getPropertyIfPresent("l_quantity", absent));
  }

  /**
   * A row carrying a temporary property is read as it is, never through the cache: SuffixIdentifier finds a temporary
   * property on a ResultInternal only, so through the view it would read as missing.
   */
  @Test
  void aRowWithATemporaryPropertyIsNotReadThroughTheCache() {
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("LineItem").set("l_quantity", 3.0).save().getIdentity());
    final ResultInternal plain = new ResultInternal(database.lookupByRID(rid[0], true));
    final PropertyCachingResult cache = new PropertyCachingResult(database);
    assertThat(cache.of(plain)).isSameAs(cache);

    final ResultInternal withTemporary = new ResultInternal(database.lookupByRID(rid[0], true));
    withTemporary.setTemporaryProperty("_$$$OALIAS$$$_1", 42);
    assertThat(cache.of(withTemporary)).isSameAs(withTemporary);
  }

  /**
   * The partition of a group in a parallel aggregation is taken from the top bits of its spread hash, for any number of
   * workers: every partition is in range and they share the keys evenly, also with as many workers as no power of two.
   */
  @Test
  void parallelGroupsSpreadEvenlyOverAnyNumberOfPartitions() {
    for (final int partitions : new int[] { 1, 2, 3, 5, 6, 7, 12 }) {
      final int[] counts = new int[partitions];
      for (long key = 0; key < 120_000; key++)
        counts[AggregateProjectionCalculationStep.partitionOf(Arrays.hashCode(new Object[] { key }), partitions)]++;
      for (final int count : counts)
        assertThat(count).as("%d partitions: %s", partitions, Arrays.toString(counts)).isBetween(120_000 / partitions * 9 / 10,
            120_000 / partitions * 11 / 10);
    }
  }

  /**
   * A numeric GROUP BY / DISTINCT key is a Long for an integer in the long range instead of a stripped BigDecimal, which cost
   * a BigDecimal and its divisions on every row: two numbers must still share a key exactly when their stripped
   * BigDecimals - the key before - are equal, with equal hashes.
   */
  @Test
  void numericKeysMeetExactlyWhenTheirDecimalFormsDo() {
    final List<Number> values = new ArrayList<>(List.of(0, 0L, -0.0, 0.0f, 1, 1L, (short) 1, (byte) 1, 1.0, 1.0f, new BigDecimal("1.00"),
        BigInteger.ONE, 100, 100.0, new BigDecimal("1E+2"), 0.5, 0.05f, 0.05, new BigDecimal("0.050"), Long.MAX_VALUE, Long.MIN_VALUE,
        (double) Long.MAX_VALUE, 9007199254740992.0, 9007199254740994.0, -9007199254740992.0, 1152921504606846976.0,
        1152921504606846980L, 1152921504606846976L, new BigDecimal("9223372036854775808"), new BigInteger("9223372036854775808"),
        new BigDecimal("-9223372036854775809"), 1e20, -1e20, 123456789.0, 123456789L, 1.5e15, 1500000000000000L, 16777217f, 16777216f,
        16777216L, 16777217L, -0.0f, 3.4e38f, 9007199254740993L, -9007199254740994.0, Double.NaN,
        Double.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, new AtomicLong(42), 42));
    final Random rnd = new Random(9496);
    for (int i = 0; i < 300; i++) {
      final long l = rnd.nextInt(5) == 0 ? rnd.nextLong() : rnd.nextInt(2_000) - 1_000;
      values.add(l);
      values.add((double) l);
      values.add(BigDecimal.valueOf(l, rnd.nextInt(3)));
      values.add(rnd.nextInt(2_000) / 8.0);
    }

    for (final Number a : values)
      for (final Number b : values) {
        final boolean before = decimalKey(a).equals(decimalKey(b));
        final Object keyA = Type.normalizeNumberForKey(a);
        final Object keyB = Type.normalizeNumberForKey(b);
        assertThat(keyA.equals(keyB)).as("%s (%s) and %s (%s)", a, a.getClass().getSimpleName(), b, b.getClass().getSimpleName())
            .isEqualTo(before);
        if (before)
          assertThat(keyA.hashCode()).isEqualTo(keyB.hashCode());
      }
  }

  /** The key of a number before issue #9496: its stripped BigDecimal, or the value itself when it has none. */
  private static Object decimalKey(final Number value) {
    if (value instanceof BigDecimal decimal)
      return decimal.stripTrailingZeros();
    if (value instanceof BigInteger integer)
      return new BigDecimal(integer).stripTrailingZeros();
    if (value instanceof Double || value instanceof Float) {
      final double d = value instanceof Float f ? Type.widenFloat(f) : value.doubleValue();
      if (Double.isNaN(d) || Double.isInfinite(d))
        return value;
      return BigDecimal.valueOf(d).stripTrailingZeros();
    }
    return BigDecimal.valueOf(value.longValue()).stripTrailingZeros();
  }

  private static Number randomNumber(final Random rnd) {
    return switch (rnd.nextInt(9)) {
      case 0 -> null;
      case 1 -> rnd.nextInt();
      case 2 -> rnd.nextInt(100);
      case 3 -> rnd.nextLong();
      case 4 -> (long) rnd.nextInt(100);
      case 5 -> rnd.nextInt(1000) / 8.0;
      case 6 -> (short) rnd.nextInt(100);
      case 7 -> rnd.nextInt(100) / 4f;
      default -> BigDecimal.valueOf(rnd.nextInt(1000), 2);
    };
  }

  private List<Result> query(final String query, final boolean parallel) {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, parallel);
    try (final ResultSet rs = database.query("sql", query)) {
      final List<Result> rows = new ArrayList<>();
      while (rs.hasNext())
        rows.add(rs.next());
      final String plan = rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
      if (!parallel)
        assertThat(plan).as(query).doesNotContain("(parallel");
      return rows;
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.QUERY_PARALLEL_SCAN, true);
    }
  }

  private static List<String> render(final List<Result> rows) {
    final List<String> rendered = new ArrayList<>();
    for (final Result row : rows) {
      final StringBuilder sb = new StringBuilder();
      for (final String p : row.getPropertyNames()) {
        final Object value = row.getProperty(p);
        sb.append(p).append('=').append(value instanceof Double d ? String.format("%.6f", d) : String.valueOf(value)).append(';');
      }
      rendered.add(sb.toString());
    }
    return rendered;
  }
}
