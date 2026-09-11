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
package com.arcadedb.remote.timeseries;

import com.arcadedb.engine.timeseries.AggregationType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A time-series read, in the protocol-independent shape both remote clients take (issue #7305). Mutable and
 * chainable so a query reads as one expression:
 *
 * <pre>
 * new TimeSeriesQuery("weather").from(1000).to(3000).tag("location", "us-east").limit(100)
 * </pre>
 * <p>
 * Timestamps are epoch milliseconds. A bound left unset is unbounded in that direction, which is not the same
 * as zero: zero is a real epoch timestamp.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TimeSeriesQuery {
  private final String              type;
  private       Long                fromTimestamp;
  private       Long                toTimestamp;
  private final List<String>        fields      = new ArrayList<>();
  private final Map<String, Object> tags        = new LinkedHashMap<>();
  private       int                 limit       = 0;
  private       long                bucketIntervalMs;
  private final List<Aggregation>   aggregations = new ArrayList<>();

  /**
   * One requested aggregate.
   *
   * @param field the column to aggregate
   * @param type  how to aggregate it
   * @param alias the output name; {@code null} or blank defaults to {@code <field>_<type>} lowercased
   */
  public record Aggregation(String field, AggregationType type, String alias) {
    public Aggregation(final String field, final AggregationType type) {
      this(field, type, null);
    }

    /** The output name this aggregate will carry, resolving the default when no alias was stated. */
    public String resolvedAlias() {
      return alias == null || alias.isBlank() ? field + "_" + type.name().toLowerCase() : alias;
    }
  }

  public TimeSeriesQuery(final String type) {
    if (type == null || type.isBlank())
      throw new IllegalArgumentException("A time-series query needs a type");
    this.type = type;
  }

  /** Inclusive lower bound, epoch milliseconds. */
  public TimeSeriesQuery from(final long timestampMs) {
    this.fromTimestamp = timestampMs;
    return this;
  }

  /** Inclusive upper bound, epoch milliseconds. */
  public TimeSeriesQuery to(final long timestampMs) {
    this.toTimestamp = timestampMs;
    return this;
  }

  /**
   * Restricts the answer to these columns. The timestamp column is always returned and always first, whether
   * or not it is named here.
   */
  public TimeSeriesQuery fields(final String... names) {
    for (final String name : names)
      fields.add(name);
    return this;
  }

  /** Adds a tag equality predicate; predicates are conjoined. */
  public TimeSeriesQuery tag(final String name, final Object value) {
    tags.put(name, value);
    return this;
  }

  /** Adds every entry of {@code values} as a tag equality predicate. */
  public TimeSeriesQuery tags(final Map<String, Object> values) {
    if (values != null)
      tags.putAll(values);
    return this;
  }

  /** Maximum rows returned. Non-positive means the server's own default. Ignored by an aggregated query. */
  public TimeSeriesQuery limit(final int limit) {
    this.limit = limit;
    return this;
  }

  /**
   * Turns this into an aggregated query: the answer carries fixed-interval buckets instead of raw rows.
   *
   * @param bucketIntervalMs width of each bucket, milliseconds; must be positive
   */
  public TimeSeriesQuery aggregate(final long bucketIntervalMs, final Aggregation... requests) {
    if (bucketIntervalMs <= 0)
      throw new IllegalArgumentException("bucketIntervalMs must be positive");
    if (requests.length == 0)
      throw new IllegalArgumentException("An aggregated query needs at least one aggregation");
    this.bucketIntervalMs = bucketIntervalMs;
    for (final Aggregation request : requests)
      aggregations.add(request);
    return this;
  }

  public String getType() {
    return type;
  }

  /** The lower bound, or {@code null} when unbounded. */
  public Long getFromTimestamp() {
    return fromTimestamp;
  }

  /** The upper bound, or {@code null} when unbounded. */
  public Long getToTimestamp() {
    return toTimestamp;
  }

  public List<String> getFields() {
    return fields;
  }

  public Map<String, Object> getTags() {
    return tags;
  }

  public int getLimit() {
    return limit;
  }

  public boolean isAggregated() {
    return !aggregations.isEmpty();
  }

  public long getBucketIntervalMs() {
    return bucketIntervalMs;
  }

  public List<Aggregation> getAggregations() {
    return aggregations;
  }
}
