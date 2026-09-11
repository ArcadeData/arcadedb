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
package com.arcadedb.server.http.handler;

import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.TagFilter;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7321: {@code GET /ts/{database}/latest} honoured only the first {@code tag} query parameter, so a
 * type with more than one tag column could not name a single series. These exercise the filter builder the
 * handler delegates to, in the shape Undertow hands a repeated query parameter over - a {@link Deque} with
 * one entry per occurrence.
 * <p>
 * {@code TimeSeriesQueryHandlerIT} drives the same behaviour over a real socket; this covers the ordering,
 * arity and column-resolution cases exhaustively without paying for a server per case.
 */
class TimeSeriesTagFilterBuilderTest {

  /** ts, host (TAG), region (TAG), cpu (FIELD) - so 'region' sits at non-timestamp index 1, not 2. */
  private static final List<ColumnDefinition> COLUMNS = List.of(
      new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
      new ColumnDefinition("host", Type.STRING, ColumnDefinition.ColumnRole.TAG),
      new ColumnDefinition("region", Type.STRING, ColumnDefinition.ColumnRole.TAG),
      new ColumnDefinition("cpu", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

  private static Deque<String> occurrences(final String... tags) {
    return new ArrayDeque<>(List.of(tags));
  }

  /** row layout is {timestamp, host, region, cpu} - {@link TagFilter#matches} offsets by the timestamp. */
  private static Object[] row(final String host, final String region) {
    return new Object[] { 1_000L, host, region, 1.0d };
  }

  @Test
  void noOccurrenceYieldsNoFilter() {
    assertThat(TimeSeriesHandlerUtils.buildTagFilterFromQueryParams(null, COLUMNS)).isNull();
    assertThat(TimeSeriesHandlerUtils.buildTagFilterFromQueryParams(occurrences(), COLUMNS)).isNull();
  }

  @Test
  void oneOccurrenceYieldsOneCondition() {
    final TagFilter filter = TimeSeriesHandlerUtils.buildTagFilterFromQueryParams(
        occurrences("host:web1"), COLUMNS);

    assertThat(filter.getConditionCount()).isEqualTo(1);
    assertThat(filter.matches(row("web1", "eu"))).isTrue();
    assertThat(filter.matches(row("web1", "us"))).as("region is unconstrained").isTrue();
    assertThat(filter.matches(row("web2", "eu"))).isFalse();
  }

  /**
   * The regression this issue is about: before the fix the handler read the Deque's first entry only, so
   * this produced one condition on 'host' and dropped 'region' entirely.
   */
  @Test
  void everyOccurrenceBecomesAnAndedCondition() {
    final TagFilter filter = TimeSeriesHandlerUtils.buildTagFilterFromQueryParams(
        occurrences("host:web1", "region:eu"), COLUMNS);

    assertThat(filter.getConditionCount()).isEqualTo(2);
    assertThat(filter.matches(row("web1", "eu"))).isTrue();
    assertThat(filter.matches(row("web1", "us"))).as("the second occurrence must still apply").isFalse();
    assertThat(filter.matches(row("web2", "eu"))).as("the first occurrence must still apply").isFalse();
  }

  @Test
  void occurrenceOrderDoesNotMatter() {
    assertThat(TimeSeriesHandlerUtils.buildTagFilterFromQueryParams(
        occurrences("region:eu", "host:web1"), COLUMNS).describe(new String[] { "host", "region", "cpu" }))
        .isEqualTo("region = 'eu' AND host = 'web1'");
    assertThat(TimeSeriesHandlerUtils.buildTagFilterFromQueryParams(
        occurrences("region:eu", "host:web1"), COLUMNS).matches(row("web1", "eu"))).isTrue();
  }

  /**
   * A tag column's position is its index among the NON-timestamp columns, so 'region' is 1 even though it
   * is the third column in the schema. Pinned because an off-by-one here would compare the region against
   * the host and still look like a working filter on a single-tag type.
   */
  @Test
  void tagPositionSkipsTheTimestampColumnOnly() {
    final TagFilter filter = TimeSeriesHandlerUtils.buildTagFilterFromQueryParams(
        occurrences("region:eu"), COLUMNS);
    assertThat(filter.getColumnIndex()).isEqualTo(1);
    assertThat(filter.matches(row("eu", "us"))).as("must read region, not host").isFalse();
    assertThat(filter.matches(row("us", "eu"))).isTrue();
  }

  /**
   * Only the FIRST ':' separates name from value, so a value carrying one of its own survives intact.
   */
  @Test
  void onlyTheFirstColonSeparatesNameFromValue() {
    final TagFilter filter = TimeSeriesHandlerUtils.buildTagFilterFromQueryParams(
        occurrences("host:web1:8080"), COLUMNS);
    assertThat(filter.matches(row("web1:8080", "eu"))).isTrue();
    assertThat(filter.matches(row("web1", "eu"))).isFalse();
  }

  /**
   * Occurrences are conjoined even when they name the same tag, which asks for a sample that carries two
   * different values at once and so matches nothing. Deliberate, and documented as such on the endpoint -
   * a set-membership reading would be a different, separately documented syntax.
   */
  @Test
  void twoOccurrencesOfTheSameTagAreAndedNotUnioned() {
    final TagFilter filter = TimeSeriesHandlerUtils.buildTagFilterFromQueryParams(
        occurrences("host:web1", "host:web2"), COLUMNS);

    assertThat(filter.getConditionCount()).isEqualTo(2);
    assertThat(filter.matches(row("web1", "eu"))).isFalse();
    assertThat(filter.matches(row("web2", "eu"))).isFalse();
  }

  /**
   * Issue #7334: an occurrence that resolves to no TAG column is REFUSED, and so is one that is not in
   * {@code name:value} form.
   * <p>
   * It used to be skipped, which is the worst of the three possible answers on this endpoint. {@code latest}
   * returns ONE row, so dropping a term does not merely widen a result set the caller can inspect - it hands
   * back the newest sample of some other series as if it were the one asked for. And on a multi-tag type a
   * single typo could drop the only term that was narrowing anything, so the answer was the newest sample of
   * the whole type.
   */
  @Test
  void anUnresolvableOccurrenceIsRefusedRatherThanDropped() {
    for (final String unresolvable : new String[] {
        "nosuchtag:x",     // no column of that name
        "cpu:1.0",         // a FIELD column, not a TAG
        "ts:1000" }) {     // the timestamp column
      assertThatThrownBy(() -> TimeSeriesHandlerUtils.buildTagFilterFromQueryParams(
          occurrences("host:web1", unresolvable), COLUMNS))
          .as("'%s' names no TAG column and must not be dropped", unresolvable)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining(unresolvable.substring(0, unresolvable.indexOf(':')))
          .as("the message names what the type DOES declare, because the name is almost always a misspelling "
              + "of one of them")
          .hasMessageContaining("host")
          .hasMessageContaining("region");
    }
  }

  /** A malformed occurrence is refused for the same reason, and says which form was expected. */
  @Test
  void anOccurrenceWithNoSeparatorIsRefused() {
    for (final String malformed : new String[] {
        "host",            // no ':' at all
        ":web1" }) {       // empty name
      assertThatThrownBy(() -> TimeSeriesHandlerUtils.buildTagFilterFromQueryParams(
          occurrences("host:web1", malformed), COLUMNS))
          .as("'%s' is not a tag selection and must not be read as one", malformed)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("name:value");
    }
  }

  /**
   * A blank occurrence is still ignored, and deliberately: {@code ?tag=} or {@code &tag=&} is what an empty
   * form field produces, and it states nothing at all rather than stating something malformed.
   */
  @Test
  void aBlankOccurrenceIsStillIgnored() {
    final TagFilter filter = TimeSeriesHandlerUtils.buildTagFilterFromQueryParams(
        occurrences("host:web1", "", "   "), COLUMNS);

    assertThat(filter.getConditionCount()).isEqualTo(1);
    assertThat(filter.matches(row("web1", "eu"))).isTrue();
    assertThat(filter.matches(row("web2", "eu"))).isFalse();
  }

  /** The 'tags' object of POST /ts/{database}/query is refused the same way, through the same resolver. */
  @Test
  void anUnresolvableNameInTheTagsObjectIsRefusedToo() {
    assertThatThrownBy(() -> TimeSeriesHandlerUtils.buildTagFilter(
        new JSONObject().put("hsot", "web1"), COLUMNS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("hsot")
        .hasMessageContaining("host");
  }

  /**
   * The refactor that gave the two endpoints one shared resolver must not have changed what the 'tags'
   * request object of POST /ts/{database}/query builds: every pair, ANDed, resolved the same way.
   */
  @Test
  void theTagsObjectPathStillAndsEveryPair() {
    final JSONObject tags = new JSONObject().put("host", "web1").put("region", "eu");
    final TagFilter filter = TimeSeriesHandlerUtils.buildTagFilter(tags, COLUMNS);

    assertThat(filter.getConditionCount()).isEqualTo(2);
    assertThat(filter.matches(row("web1", "eu"))).isTrue();
    assertThat(filter.matches(row("web1", "us"))).isFalse();
    assertThat(filter.matches(row("web2", "eu"))).isFalse();

    assertThat(TimeSeriesHandlerUtils.buildTagFilter(null, COLUMNS)).isNull();
    assertThat(TimeSeriesHandlerUtils.buildTagFilter(new JSONObject(), COLUMNS)).isNull();
  }
}
