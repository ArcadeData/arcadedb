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
package com.arcadedb.function.sql.facet;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.columnar.ColumnDefinition;
import com.arcadedb.index.columnar.ColumnKind;
import com.arcadedb.index.columnar.ColumnarIndex;
import com.arcadedb.index.columnar.ColumnarIndexManager;
import com.arcadedb.index.columnar.ColumnarProjection;
import com.arcadedb.index.columnar.ColumnarSnapshot;
import com.arcadedb.index.columnar.DictionaryPrefixSearch;
import com.arcadedb.index.columnar.GlobalDictionary;
import com.arcadedb.query.facet.FacetBucket;
import com.arcadedb.query.facet.FacetMemoryBudget;
import com.arcadedb.query.facet.FacetResult;
import com.arcadedb.query.facet.FacetResults;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityHelper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * {@code facet.searchValues(type, property, prefix, limit)}: the values of a COLUMNAR property (a string, or a list of strings indexed
 * BY ITEM) that start with {@code prefix}, in value order ({@link String#compareTo}), as rows {@code {value, count}} where {@code count}
 * is the number of records of the type (polymorphic) carrying the value now. Made for type-ahead over facet values.
 * <p>
 * Candidates come from the per-bucket dictionaries ({@link DictionaryPrefixSearch}) of the buckets of the type the current user may read
 * records of (the dictionaries have no access check of their own), k-way merged by value with duplicates dropped, in batches of
 * {@code max(2 * limit, 16)}. A dictionary keeps the values of deleted and updated records until compaction, so every batch is counted
 * by an internal {@code SELECT FROM <type> FACETS v: <property> TERMS INCLUDE [...]} query, which counts only live records the user may
 * read (patching what the projection has not applied yet): a candidate counted 0 is dropped. The search stops at {@code limit} rows, at
 * the end of the prefix range or after {@value #MAX_BATCHES} batches, returning what it found.
 * <p>
 * The limit is capped by {@code arcadedb.columnar.topMax}. Buckets of the type without the column (an index declared only on a subtype)
 * hold no dictionary and are not searched: a value only they hold is not suggested. A readable bucket whose projection is missing,
 * closed, rebuilding or does not publish the column yet fails the call with a retry message: a partial candidate set would silently miss
 * values. The ordinal buffers, the batch and the rows are charged to {@link FacetMemoryBudget} before they are allocated, and so is the
 * unranked tail every dictionary search sorts (the search is bounded by the dictionary size the charge was computed from); a refusal
 * fails the call with a retry message.
 * <p>
 * The counts are exact unless the internal query could not patch every match the projection has not applied yet (its patch budget,
 * spec 7.3): then every row of that batch carries {@code exact: false}, which an exact row omits. Values are only suggested from
 * applied dictionaries: one written by the caller's open transaction or not applied yet is not, though its count would be patched.
 */
public class SQLFunctionFacetSearchValues extends SQLFunctionAbstract {
  public static final String NAME        = "facet.searchValues";
  static final        int    MAX_BATCHES = 8;
  static final        int    MIN_BATCH   = 16;
  /** Budget per candidate of a batch: the reference, its query parameter entry and name, its INCLUDE literal and facet bucket. */
  private static final long  CANDIDATE_BYTES = 256;
  /** Budget per returned row: its map, its entries (value, count and, when inexact, exact) and the boxed count. */
  private static final long  ROW_BYTES       = 160;

  public SQLFunctionFacetSearchValues() {
    super(NAME);
  }

  @Override
  public int getMinArgs() {
    return 4;
  }

  @Override
  public int getMaxArgs() {
    return 4;
  }

  @Override
  public String getSyntax() {
    return "facet.searchValues(<type>, <property>, <prefix>, <limit>)";
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params.length != 4)
      throw new IllegalArgumentException("Syntax error: " + getSyntax());
    if (!(params[3] instanceof Number number))
      throw new IllegalArgumentException(NAME + ": limit must be a number");
    if (number.longValue() < 0)
      throw new IllegalArgumentException(NAME + ": limit must be >= 0");
    final DatabaseInternal db = context.getDatabase();
    final String typeName = params[0] == null ? null : params[0].toString();
    if (typeName == null || !db.getSchema().existsType(typeName))
      throw new CommandExecutionException(NAME + ": type '" + typeName + "' not found");
    final DocumentType type = db.getSchema().getType(typeName);
    final String property = params[1] == null ? null : params[1].toString();
    final String prefix = params[2] == null ? "" : params[2].toString();

    // the buckets whose type has the column, readable by the current user; the column of the first one names the property to count
    final List<Bucket> polymorphic = type.getBuckets(true);
    final int[] bucketIds = new int[polymorphic.size()];
    final String[] bucketNames = new String[polymorphic.size()];
    ColumnDefinition column = null;
    boolean indexed = false;
    int n = 0;
    for (final Bucket bucket : polymorphic) {
      final ColumnDefinition bucketColumn = column(db.getSchema().getTypeByBucketId(bucket.getFileId()), property);
      if (bucketColumn == null)
        continue;
      indexed = true;
      if (bucketColumn.kind() != ColumnKind.ORDINAL && bucketColumn.kind() != ColumnKind.ORDINAL_LIST)
        throw new CommandExecutionException(NAME + ": " + type.getName() + "." + property + " is not a dictionary column (" + bucketColumn.kind()
            + "): only string values can be searched by prefix");
      if (column == null)
        column = bucketColumn;
      if (!SecurityHelper.canAccessFile(db, bucket.getFileId(), SecurityDatabaseUser.ACCESS.READ_RECORD))
        continue;
      bucketIds[n] = bucket.getFileId();
      bucketNames[n++] = bucket.getName();
    }
    if (!indexed)
      throw new CommandExecutionException(NAME + ": no COLUMNAR index on " + type.getName() + "." + property);
    if (type.getName().indexOf('`') >= 0 || column.property().indexOf('`') >= 0)
      throw new CommandExecutionException(NAME + ": the name of " + type.getName() + "." + column.property() + " cannot be quoted");

    final int limit = (int) Math.min(number.longValue(), GlobalConfiguration.COLUMNAR_TOP_MAX.getValueAsInteger());
    final List<Map<String, Object>> rows = new ArrayList<>();
    if (limit == 0 || n == 0)
      return rows;

    final Search search = new Search(n, prefix, Math.max(limit * 2, MIN_BATCH));
    try {
      search.acquire(((LocalDatabase) db.getEmbedded()).getColumnarIndexManager(), bucketIds, bucketNames, column.property(), limit);
      final String sql = "SELECT FROM `" + type.getName() + "` FACETS v: `" + column.property() + "` TERMS INCLUDE [";
      final List<String> candidates = new ArrayList<>(search.batch);
      for (int round = 0; round < MAX_BATCHES && rows.size() < limit; round++) {
        candidates.clear();
        for (String next = search.next(); next != null; next = candidates.size() < search.batch ? search.next() : null)
          candidates.add(next);
        if (candidates.isEmpty())
          break;
        final Counts counted = count(db, sql, candidates);
        for (final String candidate : candidates) {
          final Long count = counted.counts().get(candidate);
          if (count == null || count < 1)
            continue;
          final Map<String, Object> row = new LinkedHashMap<>(4);
          row.put("value", candidate);
          row.put("count", count);
          if (!counted.exact())
            row.put("exact", false);
          rows.add(row);
          if (rows.size() == limit)
            break;
        }
        if (candidates.size() < search.batch)
          break;
      }
      return rows;
    } finally {
      search.close();
    }
  }

  /**
   * The live counts of the candidates: INCLUDE returns every listed value, count 0 included (TOP and MIN_COUNT do not apply to it), and
   * resolves one named parameter per value, so no value is ever written into the statement.
   */
  private static Counts count(final DatabaseInternal db, final String select, final List<String> candidates) {
    final StringBuilder sql = new StringBuilder(select.length() + candidates.size() * 6 + 16).append(select);
    final Map<String, Object> parameters = new HashMap<>(candidates.size() * 2);
    for (int i = 0; i < candidates.size(); i++) {
      if (i > 0)
        sql.append(", ");
      sql.append(":v").append(i);
      parameters.put("v" + i, candidates.get(i));
    }
    sql.append("] LIMIT 1");
    final FacetResult result;
    final boolean exact;
    try (final ResultSet rs = db.query("sql", sql.toString(), parameters)) {
      final FacetResults facets = rs.getFacets();
      result = facets.get("v");
      exact = facets.exact();
    }
    if (FacetResult.UNAVAILABLE.equals(result.source()))
      throw new CommandExecutionException(NAME + ": the counts are unavailable (" + result.reason() + "), retry");
    final Map<Object, Long> counts = new HashMap<>(result.buckets().size() * 2);
    for (final FacetBucket bucket : result.buckets())
      if (!bucket.isMissing())
        counts.put(bucket.value(), bucket.count());
    return new Counts(counts, exact);
  }

  /** The counts of one batch; {@code exact} false when the query could not patch every pending match (spec 7.3). */
  private record Counts(Map<Object, Long> counts, boolean exact) {
  }

  /** The column of the type's COLUMNAR index on the property (its own or a super type's), null when it has none. */
  private static ColumnDefinition column(final DocumentType type, final String property) {
    if (type == null || property == null)
      return null;
    final TypeIndex index = type.getPolymorphicColumnarIndex(property);
    if (index != null)
      for (final IndexInternal sub : index.getSubIndexes())
        if (sub instanceof ColumnarIndex columnar)
          return columnar.columnDefinition();
    return null;
  }

  /**
   * The k-way merge of the readable buckets' prefix ranges: one buffer of ordinals per bucket, refilled after the last value it returned
   * (never after a rank: unranked ordinals share one), heads compared by value and equal heads consumed together. Holds the snapshots,
   * and with them their dictionaries, until {@link #close()}.
   */
  private static final class Search {
    private final String                        prefix;
    private final int                           batch;
    private final long                          deadlineNanos;
    private final ColumnarSnapshot[]            snapshots;
    private final GlobalDictionary[]            dictionaries;
    private final int[][]                       buffers;
    private final int[]                         sizes;
    private final int[]                         positions;
    private final boolean[]                     exhausted;
    private       int                           buckets;
    private       FacetMemoryBudget.Reservation reservation;

    private Search(final int buckets, final String prefix, final int batch) {
      this.prefix = prefix;
      this.batch = batch;
      this.deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(GlobalConfiguration.COLUMNAR_QUERY_BUDGET_WAIT.getValueAsLong());
      this.snapshots = new ColumnarSnapshot[buckets];
      this.dictionaries = new GlobalDictionary[buckets];
      this.buffers = new int[buckets][];
      this.sizes = new int[buckets];
      this.positions = new int[buckets];
      this.exhausted = new boolean[buckets];
    }

    /** Acquires a usable snapshot publishing the column for every bucket, then charges and allocates the buffers. */
    private void acquire(final ColumnarIndexManager manager, final int[] bucketIds, final String[] bucketNames, final String property,
        final int limit) {
      for (int i = 0; i < snapshots.length; i++) {
        final ColumnarSnapshot snapshot = snapshotOf(manager, bucketIds[i]);
        if (snapshot == null)
          throw notReady(bucketNames[i]);
        snapshots[i] = snapshot;
        buckets = i + 1;
        dictionaries[i] = snapshot.dictionary(property);
        if (!snapshot.publishes(property) || dictionaries[i] == null)
          throw notReady(bucketNames[i]);
      }
      reservation = reserve((long) snapshots.length * batch * Integer.BYTES + batch * CANDIDATE_BYTES + limit * ROW_BYTES);
      for (int i = 0; i < buffers.length; i++)
        buffers[i] = new int[batch];
    }

    /** The usable snapshot of the bucket's projection; null when it has none, it was closed or dropped meanwhile, or it is unusable. */
    private static ColumnarSnapshot snapshotOf(final ColumnarIndexManager manager, final int bucketId) {
      final ColumnarProjection projection = manager.projection(bucketId);
      if (projection == null)
        return null;
      final ColumnarSnapshot snapshot;
      try {
        snapshot = projection.acquire();
      } catch (final IllegalStateException e) {
        return null;
      }
      if (snapshot.isUsable())
        return snapshot;
      snapshot.close();
      return null;
    }

    /** The next value in value order across the buckets, each value once; null at the end of every range. */
    private String next() {
      String min = null;
      for (int b = 0; b < buckets; b++) {
        final String head = head(b);
        if (head != null && (min == null || head.compareTo(min) < 0))
          min = head;
      }
      if (min != null)
        for (int b = 0; b < buckets; b++)
          if (positions[b] < sizes[b] && min.equals(dictionaries[b].valueOf(buffers[b][positions[b]])))
            positions[b]++;
      return min;
    }

    private String head(final int b) {
      if (positions[b] == sizes[b]) {
        if (exhausted[b])
          return null;
        refill(b);
        if (sizes[b] == 0)
          return null;
      }
      return (String) dictionaries[b].valueOf(buffers[b][positions[b]]);
    }

    /** The next ordinals of the bucket's range, after the last value of the previous fill; the unranked tail it sorts is charged first. */
    private void refill(final int b) {
      final GlobalDictionary dictionary = dictionaries[b];
      final String after = sizes[b] > 0 ? (String) dictionary.valueOf(buffers[b][sizes[b] - 1]) : null;
      // the size the charge is computed from bounds the search: an ordinal appended meanwhile is left to the next call
      final int size = dictionary.size();
      final long tail = Math.max(0, size - dictionary.sortedOrdinals().length);
      // the matches array while it doubles (3 ints a value at the peak), then the matches, their trimmed copy and the sort scratch (4)
      try (final FacetMemoryBudget.Reservation tailReservation = reserve(4L * Integer.BYTES * tail)) {
        sizes[b] = DictionaryPrefixSearch.search(dictionary, prefix, after, batch, buffers[b], size);
      }
      positions[b] = 0;
      if (sizes[b] < batch)
        exhausted[b] = true;
    }

    private FacetMemoryBudget.Reservation reserve(final long bytes) {
      final long waitMs = Math.max(0, TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime()));
      final FacetMemoryBudget.Reservation granted = FacetMemoryBudget.instance().reserve(bytes, waitMs);
      if (granted == null)
        throw new CommandExecutionException(NAME + ": the memory budget refused " + bytes + " bytes (setting "
            + GlobalConfiguration.COLUMNAR_QUERY_MEMORY_BUDGET.getKey() + "), retry");
      return granted;
    }

    private void close() {
      if (reservation != null)
        reservation.close();
      for (int b = 0; b < buckets; b++)
        snapshots[b].close();
    }

    private static CommandExecutionException notReady(final String bucketName) {
      return new CommandExecutionException(NAME + ": the projection of bucket " + bucketName + " is not ready, retry");
    }
  }
}
