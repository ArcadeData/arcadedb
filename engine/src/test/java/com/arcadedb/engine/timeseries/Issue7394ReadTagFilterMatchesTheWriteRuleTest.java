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

import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7394 item 3: a tag value the write path refuses must not be silently accepted as a read filter.
 * <p>
 * {@code requireStorableTagValue} refuses an array, a collection or a map on the way in, because a tag is
 * stored by its text form and those have none that means anything - a {@code byte[]} would be stored as
 * {@code [B@6bc7c054}, a different value on every run. The read side did not apply the same rule, so a filter
 * value that could never have been written was coerced (a byte array to its object identity), matched nothing
 * and returned an empty series. The client was told "no data" instead of "that value is not valid".
 * <p>
 * The rule now lives in {@link TimeSeriesGateway#andTag}, which every protocol's tag selection already
 * converges on - the gRPC {@code TimeSeriesQuery}/{@code TimeSeriesLatest} RPCs through
 * {@link TimeSeriesGateway#buildTagFilter}, and both HTTP {@code tags}-object endpoints through
 * {@code TimeSeriesHandlerUtils} - so read and write cannot disagree on any of them.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7394">issue #7394</a>
 */
class Issue7394ReadTagFilterMatchesTheWriteRuleTest {

  /** ts, host (TAG), cpu (FIELD). */
  private static final List<ColumnDefinition> COLUMNS = List.of(
      new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
      new ColumnDefinition("host", Type.STRING, ColumnDefinition.ColumnRole.TAG),
      new ColumnDefinition("cpu", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

  @Test
  void aBytesFilterValueIsRefusedRatherThanMatchingNothing() {
    assertThatThrownBy(() -> TimeSeriesGateway.buildTagFilter(Map.of("host", new byte[] { 1, 2 }), COLUMNS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("host");
  }

  @Test
  void aListFilterValueIsRefused() {
    assertThatThrownBy(() -> TimeSeriesGateway.buildTagFilter(Map.of("host", List.of("a", "b")), COLUMNS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("host");
  }

  @Test
  void aMapFilterValueIsRefused() {
    assertThatThrownBy(() -> TimeSeriesGateway.buildTagFilter(Map.of("host", Map.of("a", "b")), COLUMNS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("host");
  }

  /**
   * The HTTP {@code tags} object hands over the JSON types, and {@code JSONArray} is an {@link Iterable} that
   * is not a {@code Collection} - so the original {@code Collection} test would have let it through, and its
   * text form ({@code ["a","b"]}) is not a tag value anyone means. {@code JSONObject} is a {@code Map} and was
   * already refused; it is pinned here so the pair cannot drift.
   */
  @Test
  void aJsonArrayFilterValueIsRefused() {
    assertThatThrownBy(() -> TimeSeriesGateway.buildTagFilter(
        Map.of("host", new JSONArray(List.of("a", "b"))), COLUMNS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("host");
  }

  @Test
  void aJsonObjectFilterValueIsRefused() {
    assertThatThrownBy(() -> TimeSeriesGateway.buildTagFilter(
        Map.of("host", new JSONObject().put("a", "b")), COLUMNS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("host");
  }

  /**
   * The value is judged on its own, before the name is resolved: a name that matches no TAG column still
   * contributes nothing to the filter (issue #7321's documented behaviour, tracked for reporting by #7334),
   * but an unstorable value is unstorable whichever name carries it, and answering "no data" for it is the
   * very confusion this refusal removes.
   */
  @Test
  void anUnstorableValueIsRefusedEvenUnderANameThatResolvesToNoColumn() {
    assertThatThrownBy(() -> TimeSeriesGateway.buildTagFilter(Map.of("nosuchtag", new byte[] { 1 }), COLUMNS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("nosuchtag");
  }

  @Test
  void theOrdinaryScalarFilterValuesStillBuildAFilter() {
    assertThatCode(() -> TimeSeriesGateway.buildTagFilter(Map.of("host", "web1"), COLUMNS))
        .doesNotThrowAnyException();

    final TagFilter filter = TimeSeriesGateway.buildTagFilter(Map.of("host", "web1"), COLUMNS);
    assertThat(filter).isNotNull();
    // row layout is {timestamp, host, cpu}; TagFilter#matches offsets by the timestamp.
    assertThat(filter.matches(new Object[] { 1_000L, "web1", 1.0d })).isTrue();
    assertThat(filter.matches(new Object[] { 1_000L, "web2", 1.0d })).isFalse();
  }

  @Test
  void aNumberAndABooleanAreStillAcceptedAsTheyAreOnTheWritePath() {
    assertThatCode(() -> TimeSeriesGateway.andTag(null, "host", 42, COLUMNS)).doesNotThrowAnyException();
    assertThatCode(() -> TimeSeriesGateway.andTag(null, "host", true, COLUMNS)).doesNotThrowAnyException();
  }

  /** A null value is not a malformed one - it is how "no value" is said, and it was always allowed. */
  @Test
  void aNullFilterValueIsStillAllowed() {
    assertThatCode(() -> TimeSeriesGateway.andTag(null, "host", null, COLUMNS)).doesNotThrowAnyException();
  }
}
