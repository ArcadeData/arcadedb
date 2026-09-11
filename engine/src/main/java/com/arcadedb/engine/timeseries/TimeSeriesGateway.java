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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.LineProtocolParser.Sample;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.security.SecurityDatabaseUser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeSet;

/**
 * Protocol-neutral entry point to the time-series store, shared by every wire protocol that reaches samples
 * on behalf of a user: the HTTP {@code /ts/*} handlers and the gRPC {@code TimeSeries*} RPCs (issue #7305).
 * <p>
 * The point of the class is structural, not cosmetic. A TimeSeries type owns no record bucket, so the
 * per-file permission check {@code LocalBucket} applies to a normal record never runs for it, and the
 * type-name ACL applied here through {@link LocalTimeSeriesType#getEngine(SecurityDatabaseUser.ACCESS)} /
 * {@link LocalTimeSeriesType#requireEngine(SecurityDatabaseUser.ACCESS)} is the only thing standing between
 * a denied user and the samples. Two protocols each reimplementing the resolve/authorize/group/append
 * sequence agree only until one of them is fixed; one implementation cannot drift.
 * <p>
 * Nothing here is HTTP- or gRPC-shaped: no status codes, no response-size caps, no JSON. Those belong to the
 * protocol that is answering, which is why the failure of a read is reported as a {@link ReadFailure} the
 * caller renders rather than as a message this class invents.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class TimeSeriesGateway {

  private TimeSeriesGateway() {
  }

  /**
   * Why {@link #resolveForRead(DatabaseInternal, String)} could not hand back an engine. Deliberately distinct
   * cases: {@link #NOT_TIME_SERIES} and {@link #ENGINE_UNAVAILABLE} used to share one message, which sent an
   * operator chasing the wrong cause (issue #6356 follow-up).
   */
  public enum ReadFailure {
    /** No type with that name exists in the schema. */
    NOT_FOUND,
    /** The type exists but is not a TIMESERIES type. */
    NOT_TIME_SERIES,
    /** The type IS a TIMESERIES type; its storage engine failed to load. */
    ENGINE_UNAVAILABLE
  }

  /**
   * The outcome of resolving a type name for reading. Exactly one of {@link #engine()} and {@link #failure()}
   * is non-null.
   *
   * @param type              the resolved type, or {@code null} when the name did not resolve to one
   * @param engine            the storage engine, or {@code null} on failure
   * @param failure           why the resolution failed, or {@code null} on success
   * @param unavailableReason why the engine failed to load, set only for {@link ReadFailure#ENGINE_UNAVAILABLE}
   */
  public record TypeResolution(LocalTimeSeriesType type, TimeSeriesEngine engine, ReadFailure failure,
                               String unavailableReason) {

    public boolean isSuccess() {
      return failure == null;
    }

    /** The columns of the resolved type. Only valid on a successful resolution. */
    public List<ColumnDefinition> columns() {
      return type.getTsColumns();
    }
  }

  /**
   * What a {@link #write(DatabaseInternal, List)} call did. {@code dropped} counts individual samples, and every
   * parsed sample is either inserted or skipped into exactly one of the three sets, so
   * {@code written + dropped == samples.size()}.
   * <p>
   * The three sets are kept apart on purpose: an unknown type means "create the type first", a non-TimeSeries
   * type means "only TIMESERIES types take samples", and an unavailable one means the type is right but its
   * storage failed to load (issue #6356 follow-up). They preserve first-occurrence order.
   */
  public record WriteReport(int written, int dropped, Set<String> unknownTypes, Set<String> nonTimeSeriesTypes,
                            Set<String> unavailableTypes) {

    /** Whether every sample handed in was appended. */
    public boolean isComplete() {
      return dropped == 0;
    }
  }

  /**
   * The samples of one measurement in a single request, paired with the type they resolved to so the schema
   * lookup and the {@code instanceof} narrowing happen once per measurement, not per sample.
   */
  private record MeasurementBatch(LocalTimeSeriesType type, List<Sample> samples) {
  }

  /**
   * Appends {@code samples} to the time-series types their measurement names select.
   * <p>
   * Samples are grouped by measurement and each group is appended as ONE batch. Appending sample-by-sample
   * would open a shard transaction per sample; on a Raft HA leader every one of those is a replicated quorum
   * round trip, serialized behind the per-shard append lock, so ingest rate collapses to one sample per round
   * trip. Grouping first keeps the cost proportional to the number of measurements, not samples.
   * <p>
   * The per-type {@code CREATE_RECORD} ACL is applied during grouping - that is, entirely before the first
   * append - because {@code TimeSeriesShard.appendSamples} commits its own shard transaction: a denial
   * discovered mid-write would leave the measurements before it already durable. It is also applied before the
   * engine-availability check so a denied caller cannot learn from the report that the type exists.
   * <p>
   * <b>This call is not atomic.</b> The transaction opened here does not make it so: every {@code appendBatch}
   * below has already committed its own shard writes by the time it returns, so a failure on a later
   * measurement cannot undo the earlier ones. That is the partial-write shape the caller must report.
   *
   * @param database the database owning the types
   * @param samples  the samples to append, in any measurement order
   *
   * @return what was written and what was dropped
   *
   * @throws SecurityException if the current user is not entitled to write one of the measurements
   */
  public static WriteReport write(final DatabaseInternal database, final List<Sample> samples) throws Exception {
    // LinkedHashMap/LinkedHashSet preserve first-occurrence order, so the drop sets and their reported order
    // stay identical to a straight pass over the samples.
    final Set<String> unknownTypes = new LinkedHashSet<>();
    final Set<String> nonTimeSeriesTypes = new LinkedHashSet<>();
    final Set<String> unavailableTypes = new LinkedHashSet<>();
    final Map<String, MeasurementBatch> byMeasurement = new LinkedHashMap<>();

    for (final Sample sample : samples) {
      final String measurement = sample.getMeasurement();

      if (unknownTypes.contains(measurement) || nonTimeSeriesTypes.contains(measurement)
          || unavailableTypes.contains(measurement))
        continue;

      final MeasurementBatch batch = byMeasurement.get(measurement);
      if (batch != null) {
        batch.samples().add(sample);
        continue;
      }

      if (!database.getSchema().existsType(measurement)) {
        unknownTypes.add(measurement);
        continue;
      }

      final DocumentType docType = database.getSchema().getType(measurement);
      if (!(docType instanceof LocalTimeSeriesType tsType)) {
        nonTimeSeriesTypes.add(measurement);
        continue;
      }
      tsType.checkAccess(SecurityDatabaseUser.ACCESS.CREATE_RECORD);
      if (!tsType.isEngineAvailable()) {
        unavailableTypes.add(measurement);
        continue;
      }

      final MeasurementBatch created = new MeasurementBatch(tsType, new ArrayList<>());
      created.samples().add(sample);
      byMeasurement.put(measurement, created);
    }

    int inserted = 0;
    // Only begin a transaction when there is not already one on this thread. HTTP always arrives with none, but
    // gRPC reuses pool threads across RPCs, and an unconditional begin() there would nest inside whatever the
    // caller had open and let the commit below close a transaction this call does not own. Skipping it costs
    // nothing: TimeSeriesShard.appendSamples commits its own shard transaction either way, which is also why
    // the enclosing transaction never made this call atomic (see the note above).
    final boolean beganHere = !database.isTransactionActive();
    if (beganHere)
      database.begin();
    try {
      for (final MeasurementBatch batch : byMeasurement.values()) {
        // requireEngine(), not getEngine(): every batch here was already filtered by isEngineAvailable() above,
        // so this can never actually throw, but getEngine() alone would silently reintroduce the "no engine"
        // possibility at the type level if that filtering were ever changed.
        final TimeSeriesEngine engine = batch.type().requireEngine(SecurityDatabaseUser.ACCESS.CREATE_RECORD);
        final List<ColumnDefinition> columns = batch.type().getTsColumns();
        final List<Sample> group = batch.samples();
        final int count = group.size();

        final long[] timestamps = new long[count];
        final Object[][] columnValues = new Object[columns.size() - 1][count]; // exclude timestamp

        for (int s = 0; s < count; s++) {
          final Sample sample = group.get(s);
          timestamps[s] = sample.getTimestampMs();

          int colIdx = 0;
          for (int i = 0; i < columns.size(); i++) {
            final ColumnDefinition col = columns.get(i);
            if (col.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
              continue;

            final Object value;
            if (col.getRole() == ColumnDefinition.ColumnRole.TAG)
              value = sample.getTags().get(col.getName());
            else
              value = sample.getFields().get(col.getName());

            columnValues[colIdx][s] = value;
            colIdx++;
          }
        }

        engine.appendBatch(timestamps, columnValues);
        inserted += count;
      }
      if (beganHere)
        database.commit();
    } catch (final Exception e) {
      if (beganHere)
        database.rollback();
      throw e;
    }

    return new WriteReport(inserted, samples.size() - inserted, unknownTypes, nonTimeSeriesTypes, unavailableTypes);
  }

  /**
   * Resolves {@code typeName} to a readable time-series engine, applying the per-type {@code READ_RECORD} ACL.
   * <p>
   * The ACL runs BEFORE the engine-availability branch, so a denied caller gets the {@code SecurityException}
   * and not the unavailable-engine diagnostic, which names a file path on disk. The "does not exist" and "is
   * not a TimeSeries type" answers stay ahead of it: the ACL is keyed by type NAME and has no entry for a name
   * that is not in the schema, so it cannot be consulted before the type resolves.
   *
   * @throws SecurityException if the current user is not entitled to read the type
   */
  public static TypeResolution resolveForRead(final DatabaseInternal database, final String typeName) {
    if (!database.getSchema().existsType(typeName))
      return new TypeResolution(null, null, ReadFailure.NOT_FOUND, null);

    final DocumentType docType = database.getSchema().getType(typeName);
    if (!(docType instanceof LocalTimeSeriesType tsType))
      return new TypeResolution(null, null, ReadFailure.NOT_TIME_SERIES, null);

    final TimeSeriesEngine engine = tsType.getEngine(SecurityDatabaseUser.ACCESS.READ_RECORD);
    if (engine == null)
      return new TypeResolution(tsType, null, ReadFailure.ENGINE_UNAVAILABLE, tsType.getEngineUnavailableReason());

    return new TypeResolution(tsType, engine, null, null);
  }

  /**
   * The newest sample matching {@code tagFilter}, or {@code null} when the selection holds none.
   * <p>
   * Asks {@link TimeSeriesEngine#queryDescending} for a single row, so the cost is O(shards x blocks touched)
   * rather than O(series): each shard stops walking blocks as soon as its own limit is satisfied, and the
   * running lower bound prunes the shards visited after the first hit. Until #7322 this merged every shard's
   * whole range into one list and sorted it, which meant a type holding millions of samples allocated all of
   * them to answer a question about one row - on the endpoint a Grafana single-stat panel polls.
   * <p>
   * <b>Tie-break.</b> The guarantee is the timestamp: the returned row's timestamp is the newest in the
   * selection. When several samples share it, the row the newest-first scan yields first wins. Both this scan
   * and the whole-series scan it replaced sort stably over the same shard-ordered list, in opposite
   * directions, so the two disagree on exactly that case and only on it - the pre-#7322 answer was the last of
   * the tied rows, this one is the first. Nothing specified either. A caller that must distinguish samples
   * sharing a timestamp narrows the selection with tags instead.
   * <p>
   * Both the HTTP {@code GET /ts/{database}/latest} endpoint and the gRPC {@code TimeSeriesLatest} RPC call
   * this method and nothing else, so the two protocols cannot answer different rows (issue #7305).
   */
  public static Object[] latest(final TimeSeriesEngine engine, final TagFilter tagFilter) throws IOException {
    final List<Object[]> newest = engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, null, tagFilter, 1, null);
    return newest.isEmpty() ? null : newest.getFirst();
  }

  /**
   * Builds the conjunction of tag equality predicates described by {@code tags}, or {@code null} when the map
   * selects nothing. Values are coerced to the column's declared type so they match what both storage layers
   * hand back (issue #5475).
   *
   * @throws IllegalArgumentException if a name is not a TAG column of this type, or a value cannot be stored as
   *                                  a tag. See {@link #andTag}
   */
  public static TagFilter buildTagFilter(final Map<String, Object> tags, final List<ColumnDefinition> columns) {
    if (tags == null || tags.isEmpty())
      return null;

    TagFilter filter = null;

    for (final Map.Entry<String, Object> tag : tags.entrySet())
      filter = andTag(filter, tag.getKey(), tag.getValue(), columns);

    return filter;
  }

  /**
   * ANDs one tag condition onto {@code filter}, resolving {@code tagName} to its position among the
   * non-timestamp columns. Three tag selections converge here - the gRPC {@code TimeSeries*} RPCs through
   * {@link #buildTagFilter(Map, List)}, the {@code tags} object of {@code POST /ts/{database}/query} (and of
   * the Grafana query endpoint), and the repeated {@code tag=name:value} parameter of
   * {@code GET /ts/{database}/latest} - so they cannot drift apart on how a tag is resolved, which is how two
   * of them came to disagree in the first place (issue #7321). The PromQL evaluator is the one reader that
   * does NOT: it builds its own {@link TagFilter} from label matchers, whose values are always parser-produced
   * strings.
   * <p>
   * The value is coerced to the column's declared type so it matches what both storage layers hand back
   * (issue #5475).
   * <p>
   * It is first held to {@link #requireStorableTagValue}, the same rule the write path applies. A value that
   * could never have been written is not a selection that matches nothing - it is a malformed request, and
   * answering it with an empty series told the caller "no data" when the truth was "that value is not valid"
   * (issue #7394). The check runs before the name is resolved, because an unstorable value is unstorable
   * whichever name carries it.
   * <p>
   * <b>A name that resolves to no TAG column is refused too</b> (issue #7334), for the same reason and with a
   * worse symptom: dropping it silently left a conjunction with one fewer term, so a typo WIDENED the query
   * instead of failing it. {@code {"hsot":"web1"}} returned every row of the range and {@code ?tag=hsot:web1}
   * the newest sample of any series, neither distinguishable by the caller from a correct filter that happened
   * to match everything - and on a multi-tag type a single mistyped name removed the whole predicate. The
   * message lists the type's actual TAG columns, because a name that does not resolve is almost always a
   * misspelling of one of them.
   * <p>
   * The PromQL evaluator deliberately does NOT come through here: an unknown label is a Prometheus-specified
   * selection ({@code PromQLEvaluator.excludesEverySeries}) rather than a malformed request, so it decides the
   * whole type in one go instead of being refused.
   *
   * @return a new filter - {@link TagFilter} is immutable, so the return value must be used
   *
   * @throws IllegalArgumentException if no TAG column carries {@code tagName}, or if {@code tagValue} cannot be
   *                                  stored as a tag. Callers on the gRPC path let this surface as
   *                                  {@code INVALID_ARGUMENT} through {@code GrpcErrorMapper}, and the HTTP
   *                                  handlers as a 400
   */
  public static TagFilter andTag(final TagFilter filter, final String tagName, final Object tagValue,
      final List<ColumnDefinition> columns) {
    requireStorableTagValue(tagName, tagValue);

    int nonTsIdx = 0;
    for (final ColumnDefinition col : columns) {
      if (col.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
        continue;
      if (col.getRole() == ColumnDefinition.ColumnRole.TAG && col.getName().equals(tagName)) {
        final Object coerced = col.coerceValue(tagValue);
        return filter == null ? TagFilter.eq(nonTsIdx, coerced) : filter.and(nonTsIdx, coerced);
      }
      nonTsIdx++;
    }
    throw new IllegalArgumentException("Tag '" + tagName + "' is not a TAG column of this type" + tagColumnNames(columns));
  }

  /**
   * The type's TAG column names, rendered for the refusal above: {@code " (declared tags: a, b)"}, or a sentence
   * saying there are none. Built from the same {@code columns} list the resolution walked, so it cannot name a
   * set the caller was not actually matched against.
   */
  private static String tagColumnNames(final List<ColumnDefinition> columns) {
    final StringJoiner declared = new StringJoiner(", ");
    for (final ColumnDefinition col : columns)
      if (col.getRole() == ColumnDefinition.ColumnRole.TAG)
        declared.add(col.getName());
    return declared.length() == 0 ? ": the type declares no TAG column" : " (declared tags: " + declared + ")";
  }

  /**
   * Resolves a field projection to the column indices the engine query takes: indices among the
   * <b>non-timestamp</b> columns, ascending and without repeats. Returns {@code null} for an empty projection,
   * which the engine reads as "every column". A name that matches no non-timestamp column contributes nothing.
   * <p>
   * The convention matters and used to be wrong here (found while building the gRPC surface for issue #7305).
   * {@code TimeSeriesBucket.readRow} always prepends the timestamp and then tests each non-timestamp column's
   * own ordinal against this array, and {@link TagFilter#matchesMapped} reads it the same way; a resolver that
   * returned full-schema indices - counting the timestamp column as 0 and shifting every field by one - made
   * {@code POST /ts/{database}/query?fields=[...]} answer the neighbouring column's values under the requested
   * column's name, and pad the row with a trailing null. Ascending order is not cosmetic either: the engine
   * emits the values in schema order whatever order they were asked for, so the names
   * {@link #columnNames(List, int[])} produces only line up with the values when both are ascending.
   */
  public static int[] resolveColumnIndices(final List<String> fields, final List<ColumnDefinition> columns) {
    if (fields == null || fields.isEmpty())
      return null;

    // TreeSet: ascending, and a field named twice selects its column once.
    final SortedSet<Integer> indices = new TreeSet<>();

    for (final String fieldName : fields) {
      int nonTsIdx = 0;
      for (final ColumnDefinition column : columns) {
        if (column.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
          continue;
        if (column.getName().equals(fieldName)) {
          indices.add(nonTsIdx);
          break;
        }
        nonTsIdx++;
      }
    }

    return indices.stream().mapToInt(Integer::intValue).toArray();
  }

  /**
   * The index of the column named {@code fieldName}, or {@code -1} when the type has no such column.
   */
  public static int findColumnIndex(final String fieldName, final List<ColumnDefinition> columns) {
    for (int i = 0; i < columns.size(); i++) {
      if (columns.get(i).getName().equals(fieldName))
        return i;
    }
    return -1;
  }

  /**
   * The columns a projection selects, in the order the engine returns their values: the timestamp column
   * first, then the selected non-timestamp columns in schema order. {@code columnIndices} is the result of
   * {@link #resolveColumnIndices(List, List)}; {@code null} means every column.
   */
  public static List<ColumnDefinition> selectedColumns(final List<ColumnDefinition> columns,
      final int[] columnIndices) {
    if (columnIndices == null)
      return columns;

    final List<ColumnDefinition> selected = new ArrayList<>(columnIndices.length + 1);
    for (final ColumnDefinition column : columns)
      if (column.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP) {
        selected.add(column);
        break;
      }

    // The indices count non-timestamp columns, so walk the schema skipping the timestamp and match ordinals.
    int nonTsIdx = 0;
    for (final ColumnDefinition column : columns) {
      if (column.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
        continue;
      for (final int wanted : columnIndices)
        if (wanted == nonTsIdx) {
          selected.add(column);
          break;
        }
      nonTsIdx++;
    }

    return selected;
  }

  /**
   * The names of the columns a projection selects, in the order the engine returns their values. See
   * {@link #selectedColumns(List, int[])}.
   */
  public static List<String> columnNames(final List<ColumnDefinition> columns, final int[] columnIndices) {
    final List<ColumnDefinition> selected = selectedColumns(columns, columnIndices);
    final List<String> names = new ArrayList<>(selected.size());
    for (final ColumnDefinition column : selected)
      names.add(column.getName());
    return names;
  }

  /**
   * Checks that a tag value has a meaningful text form, and returns it unchanged.
   * <p>
   * Every protocol stores a tag by its text: {@code LineProtocolParser} models tags as
   * {@code Map<String, String>}, the column coerces that text to its declared type, and the gRPC path converts
   * its typed value the same way so the two cannot store a tag differently. That works for anything whose
   * {@code toString()} means something - a string, a number, a boolean, an enum, a temporal - and silently
   * corrupts for anything whose does not. A {@code byte[]} is the sharp case: it has no {@code toString()}
   * override, so it would be stored as {@code [B@6bc7c054}, a different meaningless value on every run
   * (claude-review on PR #7323). Iterables and maps are refused with it: their text form is stable but is not
   * a tag value anyone means.
   * <p>
   * {@link Iterable} rather than {@link Collection} because the HTTP {@code tags} object hands over JSON types
   * and {@code JSONArray} implements only the former, so a {@code Collection} test would have let
   * {@code ["a","b"]} through as a tag value (issue #7394). {@code JSONObject} is a {@link Map} and was already
   * refused.
   * <p>
   * Fields are deliberately NOT subject to this - they carry typed values into typed columns, and the column
   * decides what it can hold.
   *
   * @throws IllegalArgumentException if the value cannot be stored as a tag. Callers on the gRPC path let this
   *                                  surface as {@code INVALID_ARGUMENT} through {@code GrpcErrorMapper}
   */
  public static Object requireStorableTagValue(final String tagName, final Object value) {
    if (value != null && (value.getClass().isArray() || value instanceof Iterable<?> || value instanceof Map<?, ?>))
      throw new IllegalArgumentException("Tag '" + tagName + "' cannot hold a " + value.getClass().getSimpleName()
          + ": a tag is stored by its text form, and this one has none that means anything. "
          + "Use a string, a number or a boolean.");
    return value;
  }

  /**
   * Whether a sample value stands for "no measurement" rather than a number. A non-finite {@code double} or
   * {@code float} - what an absent MIN/MAX aggregate answers, and what a raw sample can carry too - is not a
   * value a client should read as zero, so every protocol renders it as its own null (JSON {@code null} over
   * HTTP, an unset {@code GrpcValue} over gRPC).
   */
  public static boolean isAbsentSampleValue(final Object value) {
    return (value instanceof Double d && !Double.isFinite(d)) || (value instanceof Float f && !Float.isFinite(f));
  }
}
