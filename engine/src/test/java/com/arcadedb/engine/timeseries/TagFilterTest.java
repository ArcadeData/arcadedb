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
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.codec.TimeSeriesCodec;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.TimeSeriesTypeBuilder;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link TagFilter}, including the {@code matchesMapped} method that
 * handles subset column projections.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TagFilterTest {

  @Test
  void matchesFullSchemaOrder() {
    // row[0]=ts, row[1]=col0, row[2]=col1, row[3]=col2
    final Object[] row = { 1000L, "shard-a", 42.0, "region-eu" };
    final TagFilter filter = TagFilter.eq(0, "shard-a");
    assertThat(filter.matches(row)).isTrue();

    final TagFilter noMatch = TagFilter.eq(0, "shard-b");
    assertThat(noMatch.matches(row)).isFalse();
  }

  @Test
  void matchesMappedNullColumnIndicesFallsBackToMatches() {
    // null columnIndices → full schema order, same semantics as matches()
    final Object[] row = { 1000L, "shard-a", 42.0, "region-eu" };
    final TagFilter filter = TagFilter.eq(2, "region-eu");
    assertThat(filter.matchesMapped(row, null)).isTrue();
    assertThat(filter.matchesMapped(row, null)).isEqualTo(filter.matches(row));
  }

  @Test
  void matchesMappedSubsetColumnsCorrectly() {
    // Regression: column index 2 in a subset projection [0, 2] must resolve to row[2], not row[3].
    // Schema layout (non-ts): col0="shard", col1="value", col2="region"
    // Projection selects col0 and col2 (columnIndices=[0,2]).
    // Row: row[0]=ts, row[1]=col0, row[2]=col2  (col1 omitted)
    final Object[] row = { 1000L, "shard-a", "region-eu" };
    final int[] columnIndices = { 0, 2 };

    // Filter on col2 ("region-eu"), schema index 2 → should map to row[2]
    final TagFilter filter = TagFilter.eq(2, "region-eu");
    assertThat(filter.matchesMapped(row, columnIndices)).isTrue();

    // Without the fix, matches() would check row[3] (out of bounds or wrong) — demonstrate the difference:
    // matches(row) uses row[cond.columnIndex + 1] = row[3], which is out of range → returns false
    assertThat(filter.matches(row)).isFalse(); // row[3] doesn't exist

    final TagFilter noMatch = TagFilter.eq(2, "region-us");
    assertThat(noMatch.matchesMapped(row, columnIndices)).isFalse();
  }

  @Test
  void matchesMappedColumnNotInSubset() {
    // If a tag column is not present in the selected subset, matchesMapped returns false
    final Object[] row = { 1000L, "shard-a" }; // only col0 selected
    final int[] columnIndices = { 0 };

    // Filter on col2, which was not included in columnIndices=[0]
    final TagFilter filter = TagFilter.eq(2, "region-eu");
    assertThat(filter.matchesMapped(row, columnIndices)).isFalse();
  }

  @Test
  void matchesMappedMultipleConditions() {
    // Two conditions on non-adjacent columns in a subset projection
    // Schema: col0=shard, col1=value, col2=region, col3=env
    // Projection: [0, 3] → row[1]=col0, row[2]=col3
    final Object[] row = { 2000L, "shard-b", "prod" };
    final int[] columnIndices = { 0, 3 };

    final TagFilter filter = TagFilter.eq(0, "shard-b").and(3, "prod");
    assertThat(filter.matchesMapped(row, columnIndices)).isTrue();

    final TagFilter partial = TagFilter.eq(0, "shard-b").and(3, "staging");
    assertThat(partial.matchesMapped(row, columnIndices)).isFalse();
  }

  // #7394
  // Issue #7394 item 3: a tag value the write path refuses must not be silently accepted as a read filter.
  // <p>
  // {@code requireStorableTagValue} refuses an array, a collection or a map on the way in, because a tag is
  // stored by its text form and those have none that means anything - a {@code byte[]} would be stored as
  // {@code [B@6bc7c054}, a different value on every run. The read side did not apply the same rule, so a filter
  // value that could never have been written was coerced (a byte array to its object identity), matched nothing
  // and returned an empty series. The client was told "no data" instead of "that value is not valid".
  // <p>
  // The rule now lives in {@link TimeSeriesGateway#andTag}, which every protocol's tag selection already
  // converges on - the gRPC {@code TimeSeriesQuery}/{@code TimeSeriesLatest} RPCs through
  // {@link TimeSeriesGateway#buildTagFilter}, and both HTTP {@code tags}-object endpoints through
  // {@code TimeSeriesHandlerUtils} - so read and write cannot disagree on any of them.
  //
  // @see <a href="https://github.com/ArcadeData/arcadedb/issues/7394">issue #7394</a>

  /** ts, host (TAG), cpu (FIELD). */
  private static final List<ColumnDefinition> READ_RULE_COLUMNS = List.of(
      new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
      new ColumnDefinition("host", Type.STRING, ColumnDefinition.ColumnRole.TAG),
      new ColumnDefinition("cpu", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

  // #7394
  @Test
  void aBytesFilterValueIsRefusedRatherThanMatchingNothing() {
    assertThatThrownBy(() -> TimeSeriesGateway.buildTagFilter(null, Map.of("host", new byte[] { 1, 2 }), READ_RULE_COLUMNS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("host");
  }

  // #7394
  @Test
  void aListFilterValueIsRefused() {
    assertThatThrownBy(() -> TimeSeriesGateway.buildTagFilter(null, Map.of("host", List.of("a", "b")), READ_RULE_COLUMNS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("host");
  }

  // #7394
  @Test
  void aMapFilterValueIsRefused() {
    assertThatThrownBy(() -> TimeSeriesGateway.buildTagFilter(null, Map.of("host", Map.of("a", "b")), READ_RULE_COLUMNS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("host");
  }

  // #7394
  /**
   * The HTTP {@code tags} object hands over the JSON types, and {@code JSONArray} is an {@link Iterable} that
   * is not a {@code Collection} - so the original {@code Collection} test would have let it through, and its
   * text form ({@code ["a","b"]}) is not a tag value anyone means. {@code JSONObject} is a {@code Map} and was
   * already refused; it is pinned here so the pair cannot drift.
   */
  @Test
  void aJsonArrayFilterValueIsRefused() {
    assertThatThrownBy(() -> TimeSeriesGateway.buildTagFilter(null,
        Map.of("host", new JSONArray(List.of("a", "b"))), READ_RULE_COLUMNS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("host");
  }

  // #7394
  @Test
  void aJsonObjectFilterValueIsRefused() {
    assertThatThrownBy(() -> TimeSeriesGateway.buildTagFilter(null,
        Map.of("host", new JSONObject().put("a", "b")), READ_RULE_COLUMNS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("host");
  }

  // #7394
  /**
   * The value is judged on its own, BEFORE the name is resolved. Both are refusals now (issue #7334 made the
   * unresolvable name one too), but they are different refusals and the caller is entitled to the one that
   * describes what is actually wrong: an unstorable value is unstorable whichever name carries it, so a
   * request that gets both wrong is told about the value rather than sent to check its spelling.
   */
  @Test
  void anUnstorableValueIsRefusedEvenUnderANameThatResolvesToNoColumn() {
    assertThatThrownBy(() -> TimeSeriesGateway.buildTagFilter(null, Map.of("nosuchtag", new byte[] { 1 }), READ_RULE_COLUMNS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("nosuchtag")
        .as("the VALUE rule wins: 'not a TAG column' would send the caller to fix the spelling of a name "
            + "whose value would be refused after they did")
        .hasMessageContaining("cannot hold a");
  }

  // #7394
  @Test
  void theOrdinaryScalarFilterValuesStillBuildAFilter() {
    assertThatCode(() -> TimeSeriesGateway.buildTagFilter(null, Map.of("host", "web1"), READ_RULE_COLUMNS))
        .doesNotThrowAnyException();

    final TagFilter filter = TimeSeriesGateway.buildTagFilter(null, Map.of("host", "web1"), READ_RULE_COLUMNS);
    assertThat(filter).isNotNull();
    // row layout is {timestamp, host, cpu}; TagFilter#matches offsets by the timestamp.
    assertThat(filter.matches(new Object[] { 1_000L, "web1", 1.0d })).isTrue();
    assertThat(filter.matches(new Object[] { 1_000L, "web2", 1.0d })).isFalse();
  }

  // #7394
  @Test
  void aNumberAndABooleanAreStillAcceptedAsTheyAreOnTheWritePath() {
    assertThatCode(() -> TimeSeriesGateway.andTag(null, null, "host", 42, READ_RULE_COLUMNS)).doesNotThrowAnyException();
    assertThatCode(() -> TimeSeriesGateway.andTag(null, null, "host", true, READ_RULE_COLUMNS)).doesNotThrowAnyException();
  }

  // #7394
  /** A null value is not a malformed one - it is how "no value" is said, and it was always allowed. */
  @Test
  void aNullFilterValueIsStillAllowed() {
    assertThatCode(() -> TimeSeriesGateway.andTag(null, null, "host", null, READ_RULE_COLUMNS)).doesNotThrowAnyException();
  }

  // #7334
  // Issue #7334: a tag name that resolved to no TAG column was dropped from the conjunction, so a typo WIDENED
  // the query instead of failing it.
  // <p>
  // The symptom is what makes this worth a refusal rather than a warning: {@code {"hsot":"web1"}} on
  // {@code /ts/query} returned every row of the range and {@code ?tag=hsot:web1} on {@code /ts/latest} the
  // newest sample of ANY series, and neither is distinguishable, to the caller, from a correct filter that
  // happened to match everything. The tests below hold both directions - that the typo is refused, and that the
  // refusal names what the type actually declares so the caller can see the spelling they meant.
  // <p>
  // Pinned on {@link TimeSeriesGateway#andTag} because that is where every protocol's tag selection converges
  // (issue #7321): the gRPC {@code TimeSeries*} RPCs, the {@code tags} object of both HTTP query endpoints and
  // the repeated {@code tag=name:value} parameter of {@code /ts/latest}. A refusal added to one handler would
  // have left the others widening.
  // <p>
  // The PromQL evaluator deliberately does NOT converge here and must not be made to: an unknown label is a
  // Prometheus-SPECIFIED selection - {@code =} on an absent label selects nothing, {@code !=} selects everything -
  // decided ahead of the scan by {@code PromQLEvaluator.excludesEverySeries} (issue #6938), not a malformed
  // request. Routing it through this resolver would turn a legal PromQL query into a 400.
  //
  // @see <a href="https://github.com/ArcadeData/arcadedb/issues/7334">issue #7334</a>
  //
  // @author Luca Garulli (l.garulli@arcadedata.com)

  /** ts, host (TAG), region (TAG), cpu (FIELD). */
  private static final List<ColumnDefinition> UNKNOWN_TAG_COLUMNS = List.of(
      new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
      new ColumnDefinition("host", Type.STRING, ColumnDefinition.ColumnRole.TAG),
      new ColumnDefinition("region", Type.STRING, ColumnDefinition.ColumnRole.TAG),
      new ColumnDefinition("cpu", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

  // #7334
  @Test
  void aMisspelledTagNameIsRefusedInsteadOfDropped() {
    assertThatThrownBy(() -> TimeSeriesGateway.andTag(null, null, "hsot", "web1", UNKNOWN_TAG_COLUMNS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("hsot")
        .hasMessageContaining("host")
        .hasMessageContaining("region");
  }

  // #7334
  /**
   * The whole-map entry point refuses too, and this is the shape that actually mattered: the surviving terms
   * used to still build a filter, so the request succeeded with one fewer condition than the caller wrote.
   */
  @Test
  void aMisspelledNameAmongCorrectOnesStillFailsTheWholeFilter() {
    assertThatThrownBy(() -> TimeSeriesGateway.buildTagFilter(null,
        Map.of("host", "web1", "hsot", "web1"), UNKNOWN_TAG_COLUMNS))
        .as("a conjunction the caller wrote three terms of must not be answered with two")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("hsot");
  }

  // #7334
  /** A FIELD column and the TIMESTAMP column are not tags, and naming either is the same mistake. */
  @Test
  void aNonTagColumnIsRefusedAsATagName() {
    assertThatThrownBy(() -> TimeSeriesGateway.andTag(null, null, "cpu", 1.0d, UNKNOWN_TAG_COLUMNS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cpu");
    assertThatThrownBy(() -> TimeSeriesGateway.andTag(null, null, "ts", 1_000L, UNKNOWN_TAG_COLUMNS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("ts");
  }

  // #7334
  /** A type with no TAG column at all says so, rather than listing an empty set. */
  @Test
  void aTypeWithNoTagColumnSaysThatRatherThanListingNothing() {
    final List<ColumnDefinition> tagless = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("cpu", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

    assertThatThrownBy(() -> TimeSeriesGateway.andTag(null, null, "host", "web1", tagless))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("declares no TAG column");
  }

  // #7334
  /** Nothing about a correct filter changes: the conjunction is built exactly as before. */
  @Test
  void aCorrectFilterIsUnaffected() {
    assertThatCode(() -> TimeSeriesGateway.buildTagFilter(null, Map.of("host", "web1", "region", "eu"), UNKNOWN_TAG_COLUMNS))
        .doesNotThrowAnyException();

    final TagFilter filter = TimeSeriesGateway.buildTagFilter(null, Map.of("host", "web1", "region", "eu"), UNKNOWN_TAG_COLUMNS);
    assertThat(filter.getConditionCount()).isEqualTo(2);
    // row layout is {timestamp, host, region, cpu}; TagFilter#matches offsets by the timestamp.
    assertThat(filter.matches(new Object[] { 1_000L, "web1", "eu", 1.0d })).isTrue();
    assertThat(filter.matches(new Object[] { 1_000L, "web1", "us", 1.0d })).isFalse();

    assertThat(TimeSeriesGateway.buildTagFilter(null, Map.of(), UNKNOWN_TAG_COLUMNS))
        .as("no tags at all is still no filter, not a refusal")
        .isNull();
    assertThat(TimeSeriesGateway.buildTagFilter(null, null, UNKNOWN_TAG_COLUMNS)).isNull();
  }

  @Nested
  class BoundedTagScan extends TestHelper {
    // #7354
    // Issue #7354: a reader whose ANSWER is O(label cardinality) had no way to ask for the rows without holding them.
    // <p>
    // {@code query()} merges every shard's full range into one {@code ArrayList} and sorts it by timestamp;
    // {@code iterateQuery()} skips the sort but the sealed layer still materialises every matching row before the
    // caller sees the first one, because the directory read lock has to be released first. So the PromQL label-values
    // and series endpoints - which read every row and keep a handful of strings - allocated the whole series per call.
    // <p>
    // {@code forEachRow} is the third shape: the same block walk, folding into the caller instead of into a list.
    // These tests pin the two things that makes it worth having - it sees exactly the rows the other two see, and its
    // residency is one block rather than the range.
    //
    // @author Luca Garulli (l.garulli@arcadedata.com)

    private static final int  TAGS    = 8;
    private static final int  PER_TAG = 20_000;
    private static final int  SHARDS  = 2;
    private static final long BASE_TS = 1_700_000_000_000L;
    private static final long STEP_MS = 1_000L;

    private TimeSeriesEngine engine;

    @BeforeEach
    void populate() throws IOException {
      database.command("sql",
          "CREATE TIMESERIES TYPE Point TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS " + SHARDS);

      engine = ((LocalTimeSeriesType) database.getSchema().getType("Point")).getEngine();

      final int total = TAGS * PER_TAG;
      final long[] timestamps = new long[total];
      final Object[] hosts = new Object[total];
      final Object[] values = new Object[total];

      int i = 0;
      for (int t = 0; t < PER_TAG; t++)
        for (int h = 0; h < TAGS; h++) {
          timestamps[i] = BASE_TS + t * STEP_MS;
          hosts[i] = "host_" + h;
          values[i] = (double) (t * TAGS + h);
          i++;
        }

      engine.appendBatch(timestamps, new Object[][] { hosts, values });
      engine.compactAll();
      // Left uncompacted on purpose, so the mutable layer is exercised alongside the sealed one: a scan that walked
      // only the sealed blocks would answer with a series that is missing its newest rows, and every assertion below
      // would still pass on a set of four host names.
      engine.appendBatch(new long[] { BASE_TS + PER_TAG * STEP_MS }, new Object[][] {
          new Object[] { "host_only_in_the_mutable_bucket" }, new Object[] { 1.0d } });
    }

    // #7354
    /** It sees the same rows the materialising readers see - the sealed layer and the mutable one alike. */
    @Test
    void everyRowTheMaterialisingReadersSeeReachesTheVisitor() throws Exception {
      final List<Object[]> reference = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);

      final List<Object[]> visited = new ArrayList<>();
      assertThat(engine.forEachRow(Long.MIN_VALUE, Long.MAX_VALUE, null, null, null, row -> visited.add(row)))
          .as("a visitor that never says stop runs to the end")
          .isTrue();

      assertThat(visited).hasSameSizeAs(reference);
      assertThat(distinctHosts(visited))
          .as("and the rows are the same rows, whatever order they arrive in")
          .isEqualTo(distinctHosts(reference));
    }

    // #7354
    /** The time range is honoured exactly as the materialising readers honour it. */
    @Test
    void theRangeBoundsAreTheSameOnesTheOtherReadersApply() throws Exception {
      final long from = BASE_TS + 50 * STEP_MS;
      final long to = BASE_TS + 150 * STEP_MS;

      final List<Object[]> reference = engine.query(from, to, null, null);
      final List<Object[]> visited = new ArrayList<>();
      engine.forEachRow(from, to, null, null, null, row -> visited.add(row));

      assertThat(visited).hasSameSizeAs(reference);
      for (final Object[] row : visited)
        assertThat((long) row[0]).isBetween(from, to);
    }

    // #7354
    /** A tag filter pushes down the same way, so the visitor is not a filter-less back door onto the rows. */
    @Test
    void theTagFilterIsPushedDownAsItIsForTheOtherReaders() throws Exception {
      final TagFilter filter = TagFilter.eq(0, "host_2");

      final List<Object[]> reference = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, filter);
      final List<Object[]> visited = new ArrayList<>();
      engine.forEachRow(Long.MIN_VALUE, Long.MAX_VALUE, null, filter, null, row -> visited.add(row));

      assertThat(visited).hasSameSizeAs(reference);
      assertThat(distinctHosts(visited)).containsExactly("host_2");
    }

    // #7354
    /**
     * The property the whole change is for: the scan is INCREMENTAL, so it can be stopped before it has read the
     * range. A materialising reader cannot be - by the time it returns, every row is already in memory - so a
     * visitor that stops on the first row and still causes one block to be decompressed is the observable
     * difference between the two, with no wall clock and no heap measurement involved.
     */
    @Test
    void aVisitorThatStopsEarlyStopsTheScan() throws Exception {
      int totalBlocks = 0;
      for (int i = 0; i < engine.getShardCount(); i++)
        totalBlocks += engine.getShard(i).getSealedStore().getBlockCount();
      assertThat(totalBlocks)
          .as("the fixture has to span several blocks, or 'only one was read' says nothing")
          .isGreaterThan(engine.getShardCount());

      final AggregationMetrics metrics = new AggregationMetrics();
      final AtomicInteger seen = new AtomicInteger();

      assertThat(engine.forEachRow(Long.MIN_VALUE, Long.MAX_VALUE, null, null, metrics, row -> {
        seen.incrementAndGet();
        return false;
      })).as("a visitor that says stop is reported as having stopped it").isFalse();

      assertThat(seen.get()).isEqualTo(1);
      assertThat(metrics.getFastPathBlocks() + metrics.getSlowPathBlocks())
          .as("one block decompressed, not the range: the rows are produced as they are read")
          .isEqualTo(1);
      assertThat(metrics.getMaterializedRows())
          .as("and one row turned into an Object[], out of the whole series")
          .isEqualTo(1);
    }

    // #7354
    /** An empty range visits nothing and reads no block, rather than answering an empty list expensively. */
    @Test
    void aRangeWithNoRowsVisitsNothing() throws Exception {
      final AtomicInteger seen = new AtomicInteger();
      assertThat(engine.forEachRow(0L, 1L, null, null, null, row -> {
        seen.incrementAndGet();
        return true;
      })).isTrue();
      assertThat(seen.get()).isZero();
    }

    private static Set<String> distinctHosts(final List<Object[]> rows) {
      final Set<String> hosts = new LinkedHashSet<>();
      for (final Object[] row : rows)
        if (row[1] != null)
          hosts.add(row[1].toString());
      return hosts;
    }
  }

  @Nested
  class DistinctTagValuesFromBlockMetadata extends TestHelper {
    // #7660
    // Issue #7660: the distinct values of a TAG column are already written down, so reading every sample to recover
    // them is work nobody has to do.
    // <p>
    // Each sealed block's directory entry carries {@code tagDistinctValues} - the complete distinct value set of each
    // of its TAG columns, recorded at seal time from the very rows the block holds - and block-level tag pruning has
    // always relied on it. {@code TimeSeriesEngine.collectDistinctTagValues} unions those entries instead of walking
    // the samples, which turns a Grafana label picker from O(samples) into O(blocks x cardinality).
    // <p>
    // <b>The assertion that matters is not the speed, it is the EQUALITY.</b> Issue #7371 left this out precisely
    // because the two stores that hold the answer over-approximate in ways nobody had pinned down, and Prometheus
    // documents {@code /label/{name}/values} as the values a label actually carries: a stale extra value is a wrong
    // answer a user sees in a picker, not a free win. Every test below therefore compares against
    // {@link TimeSeriesEngine#forEachRow} over the same one-column projection - the scan this replaces, run in the
    // same JVM on the same data - rather than against a hand-written expected set.
    //
    // @author Luca Garulli (l.garulli@arcadedata.com)

    private static final long BASE_TS   = 1_700_000_000_000L;
    private static final long STEP_MS   = 10L;
    /**
     * Bucket-aligned compaction, so a few thousand samples seal into a few dozen blocks instead of one: the union
     * ACROSS block directory entries is the thing under test, and a single-block sealed layer would not exercise it.
     * A fixed-size chunk is {@code TimeSeriesShard.SEALED_BLOCK_SIZE} = 65536 samples, which is a slow way to make
     * the same point.
     */
    private static final long BUCKET_MS = 1_000L;

    // #7660
    /**
     * Both layers at once, over enough samples to make several sealed blocks: the union of the directory entries
     * plus the mutable scan is the set the full scan produces, value for value.
     */
    @Test
    void theSetIsTheOneTheFullScanProduces() throws Exception {
      final TimeSeriesEngine engine = createMetric("both_layers", 2);
      appendHosts(engine, 0, 2_000, 6);
      engine.compactAll();
      // Left uncompacted on purpose: a host that exists ONLY in the mutable bucket is how a sealed-only answer
      // gives itself away, since every assertion here would still pass on the six sealed names alone.
      appendHosts(engine, 1_000_000, 50, 1, "host_only_in_the_mutable_bucket");

      assertThat(engine.getShard(0).getSealedStore().getBlockCount())
          .as("the sealed layer has to hold more than one block for the union across blocks to be exercised")
          .isGreaterThan(1);

      assertThat(distinctTagValues(engine, "host"))
          .isEqualTo(scannedTagValues(engine, "host"))
          .contains("host_only_in_the_mutable_bucket");
    }

    // #7660
    /**
     * The point of the change: for a {@code STRING} TAG column no sealed block is decompressed at all. Every block
     * is counted as SKIPPED, no block is counted as read, and the only rows materialised are the mutable bucket's.
     */
    @Test
    void noSealedBlockIsDecompressedForAStringTag() throws Exception {
      final TimeSeriesEngine engine = createMetric("no_decompression", 1);
      appendHosts(engine, 0, 2_000, 4);
      engine.compactAll();

      final int sealedBlocks = engine.getShard(0).getSealedStore().getBlockCount();
      assertThat(sealedBlocks).isGreaterThan(1);

      final AggregationMetrics metrics = new AggregationMetrics();
      final Set<String> values = new LinkedHashSet<>();
      engine.collectDistinctTagValues("host", values, metrics);

      assertThat(values).hasSize(4);
      assertThat(metrics.getSkippedBlocks())
          .as("every sealed block answered from its directory entry, none of them read")
          .isEqualTo(sealedBlocks);
      assertThat(metrics.getSlowPathBlocks())
          .as("a block that had to be decompressed would be counted here")
          .isZero();
      assertThat(metrics.getMaterializedRows())
          .as("the mutable bucket is empty after compaction, so no row is materialised anywhere")
          .isZero();
    }

    // #7660
    /**
     * A tag value whose every sample has been dropped by retention must not be named. This is the staleness the
     * issue raised against {@code tagDistinctValues}: {@code TimeSeriesSealedStore.truncateBefore} drops WHOLE
     * blocks and copies every retained one verbatim, so a declaration never outlives the rows it was built from -
     * but a test that never expires anything would not notice if it did.
     */
    @Test
    void aValueRetentionDroppedIsNotNamed() throws Exception {
      final TimeSeriesEngine engine = createMetric("retained", 1);

      appendHosts(engine, 0, 500, 1, "expired_host");
      engine.compactAll();
      appendHosts(engine, 100_000, 500, 1, "surviving_host");
      engine.compactAll();

      assertThat(distinctTagValues(engine, "host"))
          .as("before retention both hosts are sealed and both are named")
          .contains("expired_host", "surviving_host");

      engine.applyRetention(BASE_TS + 50_000);

      assertThat(distinctTagValues(engine, "host"))
          .as("a host no surviving sample carries must not be named back to a label picker")
          .isEqualTo(scannedTagValues(engine, "host"))
          .doesNotContain("expired_host")
          .contains("surviving_host");
    }

    // #7660
    /**
     * Downsampling is the rewrite path that changes a block's rows rather than copying them, and it recomputes the
     * declaration from the rows it emits. It groups by tag key, so no tag value disappears and none is invented.
     */
    @Test
    void downsamplingKeepsTheDeclarationTrue() throws Exception {
      final TimeSeriesEngine engine = createMetric("downsampled", 1);
      appendHosts(engine, 0, 2_000, 3);
      engine.compactAll();

      final Set<String> before = distinctTagValues(engine, "host");

      // nowMs far past the newest sample so every block is older than the tier's cutoff, and a granularity coarse
      // enough against the 1ms sample spacing that the density heuristic selects the blocks rather than skipping them.
      engine.applyDownsampling(List.of(new DownsamplingTier(1_000L, 60_000L)), BASE_TS + 10_000_000L);

      assertThat(distinctTagValues(engine, "host"))
          .as("downsampling averages the fields per tag group; the tag values themselves survive unchanged")
          .isEqualTo(before)
          .isEqualTo(scannedTagValues(engine, "host"));
    }

    // #7660
    /**
     * A TAG column that is not a {@code STRING} is READ rather than trusted. The declaration stores
     * {@code value.toString()} while {@code decompressColumns} hands the scan
     * {@code ColumnDefinition.boxString(...)}, which is the identity only for {@code STRING} - so for any other type
     * the two can disagree and the block is decompressed instead.
     */
    @Test
    void aNonStringTagColumnIsReadRatherThanTrusted() throws Exception {
      new TimeSeriesTypeBuilder((DatabaseInternal) database)
          .withName("int_tag")
          .withTimestamp("ts")
          .withTag("code", Type.INTEGER)
          .withField("value", Type.DOUBLE)
          .withShards(1)
          .withCompactionBucketInterval(BUCKET_MS)
          .create();

      final TimeSeriesEngine engine = engineOf("int_tag");
      final int total = 2_000;
      final long[] timestamps = new long[total];
      final Object[] codes = new Object[total];
      final Object[] values = new Object[total];
      for (int i = 0; i < total; i++) {
        timestamps[i] = BASE_TS + i * STEP_MS;
        codes[i] = i % 3;
        values[i] = (double) i;
      }
      engine.appendBatch(timestamps, new Object[][] { codes, values });
      engine.compactAll();

      final AggregationMetrics metrics = new AggregationMetrics();
      final Set<String> collected = new LinkedHashSet<>();
      engine.collectDistinctTagValues("code", collected, metrics);

      assertThat(collected)
          .as("the same values the scan produces, which is the only reason reading the block is acceptable here")
          .isEqualTo(scannedTagValues(engine, "code"))
          .containsExactlyInAnyOrder("0", "1", "2");
      assertThat(metrics.getSkippedBlocks())
          .as("no block may be answered from a declaration whose boxing the scan does not reproduce")
          .isZero();
      assertThat(metrics.getSlowPathBlocks())
          .isEqualTo(engine.getShard(0).getSealedStore().getBlockCount());
    }

    // #7660
    /**
     * A null tag value, spelled the way the scan spells it and not the way the metadata stores it.
     * <p>
     * The issue expected the two to disagree - block metadata storing a null tag as {@code ""} against a handler
     * that skips nulls - but no read path hands a null out for a {@code STRING} TAG at all. The mutable bucket
     * normalises it on the way out ({@code TimeSeriesBucket.readColumnValue} returns {@code ""} for a zero-length
     * STRING, and the tag dictionary maps {@code null} and {@code ""} onto the same {@code EMPTY_ID}), and
     * {@code compressColumn} writes it into a sealed block as {@code ""}. So {@code ""} is a label value here in
     * both layers, before and after this change alike, and the declaration agrees with the scan on it.
     */
    @Test
    void aNullTagValueIsSpelledTheWayTheScanSpellsIt() throws Exception {
      final TimeSeriesEngine engine = createMetric("null_tag", 1);
      engine.appendBatch(new long[] { BASE_TS, BASE_TS + 1, BASE_TS + 2 },
          new Object[][] { new Object[] { "h1", null, "h2" }, new Object[] { 1.0d, 2.0d, 3.0d } });

      assertThat(distinctTagValues(engine, "host"))
          .as("the mutable bucket hands a null tag back as the empty string, and so does the scan this replaces")
          .isEqualTo(scannedTagValues(engine, "host"))
          .containsExactlyInAnyOrder("h1", "h2", "");

      engine.compactAll();

      assertThat(distinctTagValues(engine, "host"))
          .as("once sealed the same null is the empty string in the declaration and in the scan alike")
          .isEqualTo(scannedTagValues(engine, "host"))
          .containsExactlyInAnyOrder("h1", "h2", "");
    }

    // #7660
    /**
     * Samples are routed round-robin, so a tag value can be absent from a shard entirely. The answer is the union
     * over every shard, not the first shard's.
     */
    @Test
    void everyShardContributes() throws Exception {
      final TimeSeriesEngine engine = createMetric("sharded", 4);
      appendHosts(engine, 0, 2_000, 7);
      engine.compactAll();

      assertThat(engine.getShardCount()).isEqualTo(4);
      assertThat(distinctTagValues(engine, "host"))
          .isEqualTo(scannedTagValues(engine, "host"))
          .hasSize(7);
    }

    // #7660
    /**
     * A type with nothing sealed yet still answers, from the mutable bucket alone - the branch where the block
     * directory is empty and the whole answer comes from the scan.
     */
    @Test
    void aTypeWithOnlyMutableRowsAnswersFromTheScan() throws Exception {
      final TimeSeriesEngine engine = createMetric("mutable_only", 1);
      appendHosts(engine, 0, 50, 3);

      assertThat(engine.getShard(0).getSealedStore().getBlockCount()).isZero();
      assertThat(distinctTagValues(engine, "host"))
          .isEqualTo(scannedTagValues(engine, "host"))
          .hasSize(3);
    }

    // #7660
    /**
     * The TIMESTAMP column is written at the head of every scanned row whatever its declared position, so the
     * projected slot of a TAG column is not its schema index. This layout - {@code [host, ts, value]} - makes the
     * two differ, and both the resolution of the projection and the indexing of the declaration have to survive it.
     */
    @Test
    void aTimestampDeclaredAfterATagDoesNotShiftTheAnswer() throws Exception {
      new TimeSeriesTypeBuilder((DatabaseInternal) database)
          .withName("mid_ts")
          .withTag("host", Type.STRING)
          .withTimestamp("ts")
          .withField("value", Type.DOUBLE)
          .withShards(1)
          .create();

      final TimeSeriesEngine engine = engineOf("mid_ts");
      engine.appendBatch(new long[] { BASE_TS, BASE_TS + 1 },
          new Object[][] { new Object[] { "h1", "h2" }, new Object[] { 1.0d, 2.0d } });
      engine.compactAll();

      assertThat(distinctTagValues(engine, "host"))
          .as("the host TAG's values, not the timestamps the row starts with")
          .isEqualTo(scannedTagValues(engine, "host"))
          .containsExactlyInAnyOrder("h1", "h2");
    }

    // #7660
    /** A FIELD is not a label, however well its name resolves against the schema, and neither is a name nobody declared. */
    @Test
    void aColumnThatIsNotADeclaredTagIsRejected() throws Exception {
      final TimeSeriesEngine engine = createMetric("not_a_tag", 1);
      appendHosts(engine, 0, 10, 2);

      assertThatThrownBy(() -> engine.collectDistinctTagValues("value", new LinkedHashSet<>(), null))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("value");
      assertThatThrownBy(() -> engine.collectDistinctTagValues("nosuchcolumn", new LinkedHashSet<>(), null))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("nosuchcolumn");
    }

    // --- helpers ---

    private TimeSeriesEngine createMetric(final String typeName, final int shards) {
      new TimeSeriesTypeBuilder((DatabaseInternal) database)
          .withName(typeName)
          .withTimestamp("ts")
          .withTag("host", Type.STRING)
          .withField("value", Type.DOUBLE)
          .withShards(shards)
          .withCompactionBucketInterval(BUCKET_MS)
          .create();
      return engineOf(typeName);
    }

    private TimeSeriesEngine engineOf(final String typeName) {
      return ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine();
    }

    /**
     * Appends {@code count} samples starting {@code startOffsetMs} after the base timestamp, {@link #STEP_MS} apart,
     * cycling over {@code hostCount} host names - or all carrying {@code fixedHost} when one is given.
     */
    private void appendHosts(final TimeSeriesEngine engine, final long startOffsetMs, final int count,
        final int hostCount, final String... fixedHost) throws IOException {
      final long[] timestamps = new long[count];
      final Object[] hosts = new Object[count];
      final Object[] values = new Object[count];
      for (int i = 0; i < count; i++) {
        timestamps[i] = BASE_TS + startOffsetMs + i * STEP_MS;
        hosts[i] = fixedHost.length > 0 ? fixedHost[0] : "host_" + (i % hostCount);
        values[i] = (double) i;
      }
      engine.appendBatch(timestamps, new Object[][] { hosts, values });
    }

    /** The new reader's answer. */
    private static Set<String> distinctTagValues(final TimeSeriesEngine engine, final String tag) throws IOException {
      final Set<String> values = new LinkedHashSet<>();
      engine.collectDistinctTagValues(tag, values, null);
      return values;
    }

    /**
     * The answer the scan produces, written the way {@code GetPromQLLabelValuesHandler} wrote it before this change:
     * a one-column projection folded through {@code forEachRow}, taking the value from slot 1 - the projected row is
     * {@code { timestamp, the selected column }} - and skipping a null.
     */
    private static Set<String> scannedTagValues(final TimeSeriesEngine engine, final String tag) throws IOException {
      final int[] columnIndices = TimeSeriesGateway.resolveColumnIndices(List.of(tag),
          engine.getColumns());
      final Set<String> values = new LinkedHashSet<>();
      engine.forEachRow(Long.MIN_VALUE, Long.MAX_VALUE, columnIndices, null, null, row -> {
        if (row.length > 1 && row[1] != null)
          values.add(row[1].toString());
        return true;
      });
      return values;
    }
  }

  @Nested
  class RangeScopedDistinctTagValues extends TestHelper {
    // #7709
    // Issue #7709: the distinct values of a TAG column, bounded to the rows a time range actually holds.
    // <p>
    // Issue #7660 made {@code collectDistinctTagValues} answer the sealed layer from each block's declared value set
    // instead of scanning every sample, and left the RANGE out: the fold ran over the whole retention, so a picker
    // scoped to the last hour was offered every value the type had ever held. The bound has to be nearly free or it
    // gives back what #7660 bought, which is why it is applied per BLOCK: outside the range a block is dropped on its
    // directory entry, inside it the declaration still answers, and only the at most two blocks that straddle a bound
    // are decompressed and filtered per row.
    // <p>
    // Every ranged assertion below is compared against {@link TimeSeriesEngine#forEachRow} over the SAME range - the
    // scan the push-down replaces, run in the same JVM on the same data - for the reason #7660 gave: an
    // over-approximation on this endpoint is a wrong answer a user sees in a Grafana picker, and a hand-written
    // expected set would not catch one that came from the declaration rather than from the rows.
    //
    // @author Luca Garulli (l.garulli@arcadedata.com)

    private static final long BASE_TS   = 1_700_000_000_000L;
    private static final long STEP_MS   = 10L;
    /** Bucket-aligned compaction, so a few thousand samples seal into dozens of blocks rather than one. */
    private static final long BUCKET_MS = 1_000L;

    // #7709
    /**
     * The defect itself: a host whose every sample is older than the window must not be named by a request scoped to
     * that window, and must still be named by the unscoped one.
     */
    @Test
    void anOldHostIsNotNamedByARecentWindow() throws Exception {
      final TimeSeriesEngine engine = createMetric("windowed", 1);
      appendHosts(engine, 0, 500, 1, "old_host");
      appendHosts(engine, 100_000, 500, 1, "recent_host");
      engine.compactAll();

      assertThat(distinctTagValues(engine, "host", Long.MIN_VALUE, Long.MAX_VALUE))
          .as("both hosts are there, so the ranged assertion is not asserting an absence that was never a presence")
          .containsExactlyInAnyOrder("old_host", "recent_host");

      assertThat(distinctTagValues(engine, "host", BASE_TS + 100_000, Long.MAX_VALUE))
          .isEqualTo(scannedTagValues(engine, "host", BASE_TS + 100_000, Long.MAX_VALUE))
          .containsExactly("recent_host");

      assertThat(distinctTagValues(engine, "host", Long.MIN_VALUE, BASE_TS + 4_990))
          .isEqualTo(scannedTagValues(engine, "host", Long.MIN_VALUE, BASE_TS + 4_990))
          .containsExactly("old_host");
    }

    // #7709
    /**
     * A bound that falls INSIDE a block is the one case the declaration cannot answer: the entry says which values
     * the block holds and nothing about which rows carry them. The value on the far side of the cut must be absent,
     * which is what says the straddling block was read rather than declared from.
     */
    @Test
    void aBoundInsideABlockIsAppliedPerRow() throws Exception {
      final TimeSeriesEngine engine = createMetric("straddle", 1);
      // One compaction bucket = 1s = 100 samples at STEP_MS. The first 50 carry "first_half", the rest "second_half",
      // so the two live in the SAME sealed block and only a per-row filter can separate them.
      appendHalves(engine);
      engine.compactAll();

      final long cut = BASE_TS + 50 * STEP_MS;

      assertThat(distinctTagValues(engine, "host", Long.MIN_VALUE, Long.MAX_VALUE))
          .containsExactlyInAnyOrder("first_half", "second_half");

      assertThat(distinctTagValues(engine, "host", cut, Long.MAX_VALUE))
          .as("the cut is inside the block, so the block's own declaration would have named both")
          .isEqualTo(scannedTagValues(engine, "host", cut, Long.MAX_VALUE))
          .containsExactly("second_half");

      assertThat(distinctTagValues(engine, "host", Long.MIN_VALUE, cut - 1))
          .isEqualTo(scannedTagValues(engine, "host", Long.MIN_VALUE, cut - 1))
          .containsExactly("first_half");
    }

    // #7709
    /**
     * The cost of the bound: a block wholly inside the range is still answered from its declaration, and a block
     * outside it is not read either. Only the blocks holding a bound are decompressed, and there are at most two.
     */
    @Test
    void onlyTheBlocksHoldingABoundAreDecompressed() throws Exception {
      final TimeSeriesEngine engine = createMetric("cost", 1);
      appendHosts(engine, 0, 4_000, 4);
      engine.compactAll();

      final int sealedBlocks = engine.getShard(0).getSealedStore().getBlockCount();
      assertThat(sealedBlocks).as("several blocks, or there is no inside/outside to tell apart").isGreaterThan(4);

      final AggregationMetrics metrics = new AggregationMetrics();
      final Set<String> values = new LinkedHashSet<>();
      // Bounds deliberately off a bucket boundary, so each end lands inside a block rather than between two.
      engine.collectDistinctTagValues("host", BASE_TS + 5_005, BASE_TS + 25_005, values, metrics);

      assertThat(metrics.getSlowPathBlocks())
          .as("at most the two blocks that straddle a bound are read")
          .isLessThanOrEqualTo(2);
      assertThat(metrics.getSkippedBlocks() + metrics.getSlowPathBlocks())
          .as("every block is accounted for: dropped on its entry, answered from its declaration, or read")
          .isEqualTo(sealedBlocks);
    }

    // #7709
    /**
     * The mutable bucket takes the same bound. It carries no declaration and is scanned either way, but it must not
     * hand back a value from outside the window just because the scan is cheap.
     */
    @Test
    void theMutableBucketTakesTheBoundToo() throws Exception {
      final TimeSeriesEngine engine = createMetric("mutable", 1);
      appendHosts(engine, 0, 100, 1, "sealed_host");
      engine.compactAll();
      appendHosts(engine, 500_000, 100, 1, "mutable_old");
      appendHosts(engine, 900_000, 100, 1, "mutable_recent");

      assertThat(distinctTagValues(engine, "host", BASE_TS + 900_000, Long.MAX_VALUE))
          .isEqualTo(scannedTagValues(engine, "host", BASE_TS + 900_000, Long.MAX_VALUE))
          .containsExactly("mutable_recent");
    }

    // #7709
    /**
     * The compatibility half of the contract: an unscoped request answers exactly what it answered before the bounds
     * existed, which is what the 3-argument overload is for.
     */
    @Test
    void theUnscopedOverloadStillAnswersOverTheWholeSeries() throws Exception {
      final TimeSeriesEngine engine = createMetric("unscoped", 2);
      appendHosts(engine, 0, 2_000, 6);
      engine.compactAll();
      appendHosts(engine, 1_000_000, 50, 1, "host_only_in_the_mutable_bucket");

      final Set<String> unscoped = new LinkedHashSet<>();
      engine.collectDistinctTagValues("host", unscoped, null);

      assertThat(unscoped)
          .isEqualTo(distinctTagValues(engine, "host", Long.MIN_VALUE, Long.MAX_VALUE))
          .isEqualTo(scannedTagValues(engine, "host", Long.MIN_VALUE, Long.MAX_VALUE))
          .contains("host_only_in_the_mutable_bucket");
    }

    // #7709
    /** An empty range names nothing, and reads nothing to find that out. */
    @Test
    void aRangeHoldingNoSampleNamesNothing() throws Exception {
      final TimeSeriesEngine engine = createMetric("empty_window", 1);
      appendHosts(engine, 0, 500, 3);
      engine.compactAll();

      final AggregationMetrics metrics = new AggregationMetrics();
      final Set<String> values = new LinkedHashSet<>();
      engine.collectDistinctTagValues("host", BASE_TS + 10_000_000, BASE_TS + 20_000_000, values, metrics);

      assertThat(values).isEmpty();
      assertThat(metrics.getSlowPathBlocks()).as("no block is read to answer 'nothing here'").isZero();
    }

    // #7709
    /**
     * {@code hasRowsInRange} is the same bound for a metric NAME, which has no tag column and so no declaration to
     * read. It is what scopes {@code /label/__name__/values} to the window.
     */
    @Test
    void hasRowsInRangeAnswersWhetherTheWindowHoldsASample() throws Exception {
      final TimeSeriesEngine engine = createMetric("probe", 1);
      appendHosts(engine, 0, 500, 2);
      engine.compactAll();
      appendHosts(engine, 900_000, 10, 1, "mutable_only");

      assertThat(engine.hasRowsInRange(Long.MIN_VALUE, Long.MAX_VALUE, null)).isTrue();
      assertThat(engine.hasRowsInRange(BASE_TS, BASE_TS + 100, null)).as("sealed layer").isTrue();
      assertThat(engine.hasRowsInRange(BASE_TS + 900_000, Long.MAX_VALUE, null)).as("mutable bucket").isTrue();
      assertThat(engine.hasRowsInRange(BASE_TS + 10_000_000, BASE_TS + 20_000_000, null)).isFalse();

      final TimeSeriesEngine empty = createMetric("probe_empty", 1);
      assertThat(empty.hasRowsInRange(Long.MIN_VALUE, Long.MAX_VALUE, null))
          .as("a type holding no sample at all")
          .isFalse();
    }

    // --- helpers ---

    private TimeSeriesEngine createMetric(final String typeName, final int shards) {
      new TimeSeriesTypeBuilder((DatabaseInternal) database)
          .withName(typeName)
          .withTimestamp("ts")
          .withTag("host", Type.STRING)
          .withField("value", Type.DOUBLE)
          .withShards(shards)
          .withCompactionBucketInterval(BUCKET_MS)
          .create();
      return ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine();
    }

    private void appendHosts(final TimeSeriesEngine engine, final long startOffsetMs, final int count,
        final int hostCount, final String... fixedHost) throws IOException {
      final long[] timestamps = new long[count];
      final Object[] hosts = new Object[count];
      final Object[] values = new Object[count];
      for (int i = 0; i < count; i++) {
        timestamps[i] = BASE_TS + startOffsetMs + i * STEP_MS;
        hosts[i] = fixedHost.length > 0 ? fixedHost[0] : "host_" + (i % hostCount);
        values[i] = (double) i;
      }
      engine.appendBatch(timestamps, new Object[][] { hosts, values });
    }

    /** 100 samples inside one compaction bucket, the first half under one host name and the second under another. */
    private void appendHalves(final TimeSeriesEngine engine) throws IOException {
      final long[] timestamps = new long[100];
      final Object[] hosts = new Object[100];
      final Object[] values = new Object[100];
      for (int i = 0; i < 100; i++) {
        timestamps[i] = BASE_TS + i * STEP_MS;
        hosts[i] = i < 50 ? "first_half" : "second_half";
        values[i] = (double) i;
      }
      engine.appendBatch(timestamps, new Object[][] { hosts, values });
    }

    private static Set<String> distinctTagValues(final TimeSeriesEngine engine, final String tag, final long fromTs,
        final long toTs) throws IOException {
      final Set<String> values = new LinkedHashSet<>();
      engine.collectDistinctTagValues(tag, fromTs, toTs, values, null);
      return values;
    }

    /** The answer the scan produces over the same range: the reference every ranged assertion is compared against. */
    private static Set<String> scannedTagValues(final TimeSeriesEngine engine, final String tag, final long fromTs,
        final long toTs) throws IOException {
      final int[] columnIndices = TimeSeriesGateway.resolveColumnIndices(List.of(tag), engine.getColumns());
      final Set<String> values = new LinkedHashSet<>();
      engine.forEachRow(fromTs, toTs, columnIndices, null, null, row -> {
        if (row.length > 1 && row[1] != null)
          values.add(row[1].toString());
        return true;
      });
      return values;
    }
  }

  @Nested
  class TagCombinationsFromBlockMetadata extends TestHelper {
    // #7710
    // Issue #7710: {@code GET /prom/api/v1/series} answers the distinct label COMBINATIONS a metric carries, and used
    // to walk every sample of every matching type to find them - on the same Grafana dashboard-load path #7660
    // fixed for label-values.
    // <p>
    // Nothing on disk records a tuple: {@code BlockEntry.tagDistinctValues} is per COLUMN, so the cross product of
    // two columns' declared sets OVER-COUNTS - a block declaring {@code host in {h1, h2}} and
    // {@code region in {eu, us}} is consistent with two combinations and with four, and the entry cannot say which.
    // What a block CAN answer alone is the case where it declares every tag column as a single value: that block
    // holds exactly one combination, which is the common shape for Prometheus data, where one block holds one series.
    // <p>
    // What is pinned here is that the fold over {@code forEachTagCombination} reaches the same answer as the fold
    // over {@code forEachRow} - the set of combinations, and the earliest timestamp each was observed at - and that
    // it reaches it without decompressing the blocks it could answer from their directory entries.
    //
    // @author Luca Garulli (l.garulli@arcadedata.com)

    private static final int[] TAG_PROJECTION = { 0, 1 }; // host, region: the non-timestamp indices of the tags

    private static List<ColumnDefinition> twoTagColumns() {
      return List.of(
          new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
          new ColumnDefinition("host", Type.STRING, ColumnDefinition.ColumnRole.TAG),
          new ColumnDefinition("region", Type.STRING, ColumnDefinition.ColumnRole.TAG),
          new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));
    }

    // #7710
    /**
     * The shape the optimisation is for: each series sealed into its own block, so every block declares one value
     * per tag column and the whole answer comes off the directory.
     */
    @Test
    void answersABlockHoldingOneSeriesFromItsDirectoryEntry() throws Exception {
      final TimeSeriesEngine engine = engineFor("ts_series_per_block");
      try {
        appendAndSeal(engine, "h1", "eu", 1000L, 5);
        appendAndSeal(engine, "h2", "us", 2000L, 5);

        final AggregationMetrics metrics = new AggregationMetrics();
        final Map<String, Long> combinations = foldCombinations(engine, Long.MIN_VALUE, Long.MAX_VALUE, metrics);

        assertThat(combinations).containsOnlyKeys("h1|eu", "h2|us");
        assertThat(combinations.get("h1|eu")).isEqualTo(1000L);
        assertThat(combinations.get("h2|us")).isEqualTo(2000L);

        assertThat(metrics.getMaterializedRows())
            .as("not one sample is read: both blocks are answered from their directory entries").isZero();
        assertThat(metrics.getSkippedBlocks()).isEqualTo(2);

        assertThat(combinations).isEqualTo(foldRows(engine, Long.MIN_VALUE, Long.MAX_VALUE));
      } finally {
        engine.close();
      }
    }

    // #7710
    /**
     * The case the issue says a test has to hold: ONE block carrying two combinations. Its per-column declaration
     * is {@code host in {h1, h2}} and {@code region in {eu, us}}, whose cross product is four - so a reader that
     * trusted it would report two series that do not exist. The block must be read.
     */
    @Test
    void readsABlockHoldingMoreThanOneCombinationRatherThanCrossingItsColumns() throws Exception {
      final TimeSeriesEngine engine = engineFor("ts_two_series_one_block");
      try {
        // Interleaved, so both series live in the same sealed block.
        final int count = 10;
        final long[] timestamps = new long[count];
        final Object[] hosts = new Object[count];
        final Object[] regions = new Object[count];
        final Object[] values = new Object[count];
        for (int i = 0; i < count; i++) {
          timestamps[i] = 1000L + i * 1000L;
          hosts[i] = i % 2 == 0 ? "h1" : "h2";
          regions[i] = i % 2 == 0 ? "eu" : "us";
          values[i] = (double) i;
        }
        append(engine, timestamps, hosts, regions, values);
        seal(engine);
        assertThat(engine.getShard(0).getSealedStore().getBlockCount()).isEqualTo(1);

        final AggregationMetrics metrics = new AggregationMetrics();
        final Map<String, Long> combinations = foldCombinations(engine, Long.MIN_VALUE, Long.MAX_VALUE, metrics);

        assertThat(combinations)
            .as("two combinations, not the four the cross product of the declared sets would give")
            .containsOnlyKeys("h1|eu", "h2|us");
        assertThat(combinations.get("h1|eu")).isEqualTo(1000L);
        assertThat(combinations.get("h2|us")).isEqualTo(2000L);

        assertThat(metrics.getMaterializedRows())
            .as("a block that cannot answer from its entry is READ, exactly as forEachRow would read it")
            .isEqualTo(count);

        assertThat(combinations).isEqualTo(foldRows(engine, Long.MIN_VALUE, Long.MAX_VALUE));
      } finally {
        engine.close();
      }
    }

    // #7710
    /**
     * The bounds the endpoint honours have to keep being honoured, and the timestamp a combination is reported at
     * is the earliest one INSIDE the range. A block only partly covered by the range carries no such timestamp in
     * its directory entry - {@code minTimestamp} is the block's, not the range's - so it is read.
     */
    @Test
    void honoursTheRangeAndReportsTheEarliestTimestampInsideIt() throws Exception {
      final TimeSeriesEngine engine = engineFor("ts_partial_range");
      try {
        appendAndSeal(engine, "h1", "eu", 1000L, 5);   // 1000..5000
        appendAndSeal(engine, "h2", "us", 10000L, 5);  // 10000..14000

        // Cuts the first block in half and leaves the second one out entirely.
        final Map<String, Long> combinations = foldCombinations(engine, 3000L, 6000L, new AggregationMetrics());

        assertThat(combinations).containsOnlyKeys("h1|eu");
        assertThat(combinations.get("h1|eu"))
            .as("the earliest sample inside the range, not the block's own minTimestamp").isEqualTo(3000L);

        assertThat(combinations).isEqualTo(foldRows(engine, 3000L, 6000L));
      } finally {
        engine.close();
      }
    }

    // #7710
    /**
     * The mutable layer declares nothing, so it is always scanned - and its rows have to fold into the same answer
     * as the sealed ones, including when a series lives in BOTH layers.
     */
    @Test
    void foldsTheMutableBucketTogetherWithTheSealedBlocks() throws Exception {
      final TimeSeriesEngine engine = engineFor("ts_both_layers");
      try {
        appendAndSeal(engine, "h1", "eu", 1000L, 5);
        // Not sealed: stays in the mutable bucket. One row extends a sealed series, one opens a new one.
        append(engine, new long[] { 6000L, 7000L }, new Object[] { "h1", "h3" }, new Object[] { "eu", "ap" },
            new Object[] { 1.0, 2.0 });

        final Map<String, Long> combinations = foldCombinations(engine, Long.MIN_VALUE, Long.MAX_VALUE,
            new AggregationMetrics());

        assertThat(combinations).containsOnlyKeys("h1|eu", "h3|ap");
        assertThat(combinations.get("h1|eu"))
            .as("the sealed block is older, so it owns the series' earliest timestamp").isEqualTo(1000L);
        assertThat(combinations.get("h3|ap")).isEqualTo(7000L);

        assertThat(combinations).isEqualTo(foldRows(engine, Long.MIN_VALUE, Long.MAX_VALUE));
      } finally {
        engine.close();
      }
    }

    // #7710
    /**
     * A metric with NO tag columns carries exactly one combination - the empty one - which every block answers
     * from its {@code sampleCount} and {@code minTimestamp} alone. {@code GetPromQLSeriesHandler} spells that
     * projection out by name, so it is not a corner case nobody reaches; and a block with no TAG column declares
     * no distinct values at all, which is why the declaration has to be consulted per projected column rather than
     * required up front (code review on PR #7730).
     */
    @Test
    void answersAMetricWithNoTagColumnsWithoutReadingABlock() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final List<ColumnDefinition> columns = List.of(
          new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
          new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

      database.begin();
      final TimeSeriesEngine engine = new TimeSeriesEngine(db, "ts_no_tags", columns, 1);
      database.commit();
      try {
        database.begin();
        engine.appendSamples(new long[] { 1000L, 2000L, 3000L }, new Object[] { 1.0, 2.0, 3.0 });
        database.commit();
        seal(engine);

        final AggregationMetrics metrics = new AggregationMetrics();
        final List<Object[]> visited = new ArrayList<>();
        // The projection GetPromQLSeriesHandler builds for a type with no TAG columns: nothing but the timestamp.
        engine.forEachTagCombination(Long.MIN_VALUE, Long.MAX_VALUE, new int[0], metrics, row -> {
          visited.add(row);
          return true;
        });

        assertThat(visited).hasSize(1);
        assertThat(visited.getFirst()).as("just the timestamp: one series, identified by its metric name alone")
            .hasSize(1);
        assertThat((long) visited.getFirst()[0]).isEqualTo(1000L);
        assertThat(metrics.getMaterializedRows())
            .as("a block that cannot disagree with itself needs no read").isZero();
      } finally {
        engine.close();
      }
    }

    // #7710
    /**
     * The MIXED block: one projected tag single-valued, the other not. The fast path needs EVERY projected column
     * to declare exactly one value, and the two extremes - all single, all multiple - do not exercise the
     * short-circuit between them. A block like this is consistent with two combinations and with four, so it has
     * to be read (code review on PR #7730).
     */
    @Test
    void readsABlockWhereOnlyOneOfTheProjectedTagsIsSingleValued() throws Exception {
      final TimeSeriesEngine engine = engineFor("ts_mixed_declaration");
      try {
        // One host throughout, two regions: host declares {h1}, region declares {eu, us}.
        final int count = 6;
        final long[] timestamps = new long[count];
        final Object[] hosts = new Object[count];
        final Object[] regions = new Object[count];
        final Object[] values = new Object[count];
        for (int i = 0; i < count; i++) {
          timestamps[i] = 1000L + i * 1000L;
          hosts[i] = "h1";
          regions[i] = i % 2 == 0 ? "eu" : "us";
          values[i] = (double) i;
        }
        append(engine, timestamps, hosts, regions, values);
        seal(engine);
        assertThat(engine.getShard(0).getSealedStore().getBlockCount()).isEqualTo(1);

        final AggregationMetrics metrics = new AggregationMetrics();
        final Map<String, Long> combinations = foldCombinations(engine, Long.MIN_VALUE, Long.MAX_VALUE, metrics);

        assertThat(combinations).containsOnlyKeys("h1|eu", "h1|us");
        assertThat(metrics.getMaterializedRows())
            .as("one column declaring a single value is not enough: the block is read").isEqualTo(count);
        assertThat(combinations).isEqualTo(foldRows(engine, Long.MIN_VALUE, Long.MAX_VALUE));
      } finally {
        engine.close();
      }
    }

    // #7710
    /**
     * A tag column carrying an explicit non-dictionary codec declares its distinct values as TEXT while a scan of
     * it hands back the boxed value, so its declaration cannot stand in for the scan: the block is read instead of
     * being answered with a {@code String} where the caller would have seen an {@code Integer}.
     */
    @Test
    void readsABlockWhoseTagDeclarationIsNotSpelledTheWayAScanSpellsIt() throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      final List<ColumnDefinition> columns = List.of(
          new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
          new ColumnDefinition("zone", Type.INTEGER, ColumnDefinition.ColumnRole.TAG, TimeSeriesCodec.SIMPLE8B),
          new ColumnDefinition("value", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

      database.begin();
      final TimeSeriesEngine engine = new TimeSeriesEngine(db, "ts_numeric_tag", columns, 1);
      database.commit();
      try {
        append(engine, new long[] { 1000L, 2000L }, new Object[] { 7, 7 }, null, new Object[] { 1.0, 2.0 });
        seal(engine);

        final AggregationMetrics metrics = new AggregationMetrics();
        final List<Object[]> visited = new ArrayList<>();
        engine.forEachTagCombination(Long.MIN_VALUE, Long.MAX_VALUE, new int[] { 0 }, metrics, row -> {
          visited.add(row);
          return true;
        });

        assertThat(metrics.getMaterializedRows())
            .as("the declaration is text, the scan gives an Integer: the block has to be read").isEqualTo(2);
        assertThat(visited).allSatisfy(row -> assertThat(row[1]).isEqualTo(7));
      } finally {
        engine.close();
      }
    }

    private TimeSeriesEngine engineFor(final String typeName) throws Exception {
      final DatabaseInternal db = (DatabaseInternal) database;
      database.begin();
      final TimeSeriesEngine engine = new TimeSeriesEngine(db, typeName, twoTagColumns(), 1);
      database.commit();
      return engine;
    }

    /** {@code count} samples one second apart for one (host, region) pair, sealed into their own block. */
    private void appendAndSeal(final TimeSeriesEngine engine, final String host, final String region,
        final long startTs, final int count) throws Exception {
      final long[] timestamps = new long[count];
      final Object[] hosts = new Object[count];
      final Object[] regions = new Object[count];
      final Object[] values = new Object[count];
      for (int i = 0; i < count; i++) {
        timestamps[i] = startTs + i * 1000L;
        hosts[i] = host;
        regions[i] = region;
        values[i] = (double) i;
      }
      append(engine, timestamps, hosts, regions, values);
      seal(engine);
    }

    private void append(final TimeSeriesEngine engine, final long[] timestamps, final Object[] hosts,
        final Object[] regions, final Object[] values) throws Exception {
      database.begin();
      if (regions == null)
        engine.appendSamples(timestamps, hosts, values);
      else
        engine.appendSamples(timestamps, hosts, regions, values);
      database.commit();
    }

    private void seal(final TimeSeriesEngine engine) throws Exception {
      database.begin();
      engine.compactAll();
      database.commit();
    }

    /** The answer {@code GetPromQLSeriesHandler} builds, folded off {@code forEachTagCombination}. */
    private Map<String, Long> foldCombinations(final TimeSeriesEngine engine, final long fromTs, final long toTs,
        final AggregationMetrics metrics) throws IOException {
      final Map<String, Long> earliest = new LinkedHashMap<>();
      engine.forEachTagCombination(fromTs, toTs, TAG_PROJECTION, metrics, row -> {
        fold(earliest, row);
        return true;
      });
      return earliest;
    }

    /** The same answer folded off the full scan, which is what it has to equal. */
    private Map<String, Long> foldRows(final TimeSeriesEngine engine, final long fromTs, final long toTs)
        throws IOException {
      final Map<String, Long> earliest = new LinkedHashMap<>();
      engine.forEachRow(fromTs, toTs, TAG_PROJECTION, null, null, row -> {
        fold(earliest, row);
        return true;
      });
      return earliest;
    }

    private static void fold(final Map<String, Long> earliest, final Object[] row) {
      final String key = row[1] + "|" + row[2];
      final long timestamp = (long) row[0];
      earliest.merge(key, timestamp, Math::min);
    }
  }
}
