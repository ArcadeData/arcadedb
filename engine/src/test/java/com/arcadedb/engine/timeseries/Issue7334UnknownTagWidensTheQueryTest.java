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
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7334: a tag name that resolved to no TAG column was dropped from the conjunction, so a typo WIDENED
 * the query instead of failing it.
 * <p>
 * The symptom is what makes this worth a refusal rather than a warning: {@code {"hsot":"web1"}} on
 * {@code /ts/query} returned every row of the range and {@code ?tag=hsot:web1} on {@code /ts/latest} the
 * newest sample of ANY series, and neither is distinguishable, to the caller, from a correct filter that
 * happened to match everything. The tests below hold both directions - that the typo is refused, and that the
 * refusal names what the type actually declares so the caller can see the spelling they meant.
 * <p>
 * Pinned on {@link TimeSeriesGateway#andTag} because that is where every protocol's tag selection converges
 * (issue #7321): the gRPC {@code TimeSeries*} RPCs, the {@code tags} object of both HTTP query endpoints and
 * the repeated {@code tag=name:value} parameter of {@code /ts/latest}. A refusal added to one handler would
 * have left the others widening.
 * <p>
 * The PromQL evaluator deliberately does NOT converge here and must not be made to: an unknown label is a
 * Prometheus-SPECIFIED selection - {@code =} on an absent label selects nothing, {@code !=} selects everything -
 * decided ahead of the scan by {@code PromQLEvaluator.excludesEverySeries} (issue #6938), not a malformed
 * request. Routing it through this resolver would turn a legal PromQL query into a 400.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7334">issue #7334</a>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7334UnknownTagWidensTheQueryTest {

  /** ts, host (TAG), region (TAG), cpu (FIELD). */
  private static final List<ColumnDefinition> COLUMNS = List.of(
      new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
      new ColumnDefinition("host", Type.STRING, ColumnDefinition.ColumnRole.TAG),
      new ColumnDefinition("region", Type.STRING, ColumnDefinition.ColumnRole.TAG),
      new ColumnDefinition("cpu", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

  @Test
  void aMisspelledTagNameIsRefusedInsteadOfDropped() {
    assertThatThrownBy(() -> TimeSeriesGateway.andTag(null, "hsot", "web1", COLUMNS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("hsot")
        .hasMessageContaining("host")
        .hasMessageContaining("region");
  }

  /**
   * The whole-map entry point refuses too, and this is the shape that actually mattered: the surviving terms
   * used to still build a filter, so the request succeeded with one fewer condition than the caller wrote.
   */
  @Test
  void aMisspelledNameAmongCorrectOnesStillFailsTheWholeFilter() {
    assertThatThrownBy(() -> TimeSeriesGateway.buildTagFilter(
        Map.of("host", "web1", "hsot", "web1"), COLUMNS))
        .as("a conjunction the caller wrote three terms of must not be answered with two")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("hsot");
  }

  /** A FIELD column and the TIMESTAMP column are not tags, and naming either is the same mistake. */
  @Test
  void aNonTagColumnIsRefusedAsATagName() {
    assertThatThrownBy(() -> TimeSeriesGateway.andTag(null, "cpu", 1.0d, COLUMNS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cpu");
    assertThatThrownBy(() -> TimeSeriesGateway.andTag(null, "ts", 1_000L, COLUMNS))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("ts");
  }

  /** A type with no TAG column at all says so, rather than listing an empty set. */
  @Test
  void aTypeWithNoTagColumnSaysThatRatherThanListingNothing() {
    final List<ColumnDefinition> tagless = List.of(
        new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP),
        new ColumnDefinition("cpu", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

    assertThatThrownBy(() -> TimeSeriesGateway.andTag(null, "host", "web1", tagless))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("declares no TAG column");
  }

  /** Nothing about a correct filter changes: the conjunction is built exactly as before. */
  @Test
  void aCorrectFilterIsUnaffected() {
    assertThatCode(() -> TimeSeriesGateway.buildTagFilter(Map.of("host", "web1", "region", "eu"), COLUMNS))
        .doesNotThrowAnyException();

    final TagFilter filter = TimeSeriesGateway.buildTagFilter(Map.of("host", "web1", "region", "eu"), COLUMNS);
    assertThat(filter.getConditionCount()).isEqualTo(2);
    // row layout is {timestamp, host, region, cpu}; TagFilter#matches offsets by the timestamp.
    assertThat(filter.matches(new Object[] { 1_000L, "web1", "eu", 1.0d })).isTrue();
    assertThat(filter.matches(new Object[] { 1_000L, "web1", "us", 1.0d })).isFalse();

    assertThat(TimeSeriesGateway.buildTagFilter(Map.of(), COLUMNS))
        .as("no tags at all is still no filter, not a refusal")
        .isNull();
    assertThat(TimeSeriesGateway.buildTagFilter(null, COLUMNS)).isNull();
  }

}
