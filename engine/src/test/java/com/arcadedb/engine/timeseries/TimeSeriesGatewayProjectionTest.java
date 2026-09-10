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

import com.arcadedb.engine.timeseries.ColumnDefinition.ColumnRole;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The projection convention {@link TimeSeriesGateway#resolveColumnIndices} must produce, pinned at the unit
 * level so it cannot drift back (issue #7305).
 * <p>
 * {@code TimeSeriesBucket.readRow} always prepends the timestamp and then tests each NON-timestamp column's own
 * ordinal against the array, and {@link TagFilter#matchesMapped} reads it the same way. A resolver that
 * returned full-schema indices instead - counting the timestamp column as 0 and shifting every field by one -
 * made a projected query answer the neighbouring column's values under the requested column's name. The column
 * NAMES still looked right, which is why the defect survived: a test that checks only the names, or only the
 * row's width, passes against it.
 */
class TimeSeriesGatewayProjectionTest {

  /** ts (TIMESTAMP), location (TAG), sensor (TAG), temperature (FIELD), humidity (FIELD). */
  private static final List<ColumnDefinition> COLUMNS = List.of(
      new ColumnDefinition("ts", Type.LONG, ColumnRole.TIMESTAMP),
      new ColumnDefinition("location", Type.STRING, ColumnRole.TAG),
      new ColumnDefinition("sensor", Type.STRING, ColumnRole.TAG),
      new ColumnDefinition("temperature", Type.DOUBLE, ColumnRole.FIELD),
      new ColumnDefinition("humidity", Type.DOUBLE, ColumnRole.FIELD));

  @Test
  void indicesCountNonTimestampColumns() {
    // temperature is the 4th column of the schema but the 3rd non-timestamp one, so index 2 - not 3.
    assertThat(TimeSeriesGateway.resolveColumnIndices(List.of("temperature"), COLUMNS)).containsExactly(2);
    assertThat(TimeSeriesGateway.resolveColumnIndices(List.of("location"), COLUMNS)).containsExactly(0);
    assertThat(TimeSeriesGateway.resolveColumnIndices(List.of("humidity"), COLUMNS)).containsExactly(3);
  }

  @Test
  void indicesComeBackAscendingWhateverOrderWasAsked() {
    // The engine emits the values in schema order regardless of the request order, so an unsorted array would
    // pair the names with the wrong values.
    assertThat(TimeSeriesGateway.resolveColumnIndices(List.of("humidity", "location", "temperature"), COLUMNS))
        .containsExactly(0, 2, 3);
  }

  @Test
  void aFieldNamedTwiceSelectsItsColumnOnce() {
    assertThat(TimeSeriesGateway.resolveColumnIndices(List.of("temperature", "temperature"), COLUMNS))
        .containsExactly(2);
  }

  @Test
  void namingTheTimestampColumnAddsNoIndexBecauseItIsAlwaysReturned() {
    // The timestamp is not one of the indexed columns: readRow prepends it unconditionally. Counting it would
    // shift every following column by one, which is exactly the defect this class exists to prevent.
    assertThat(TimeSeriesGateway.resolveColumnIndices(List.of("ts"), COLUMNS)).isEmpty();
    assertThat(TimeSeriesGateway.resolveColumnIndices(List.of("ts", "temperature"), COLUMNS)).containsExactly(2);
  }

  @Test
  void anUnknownFieldContributesNothing() {
    assertThat(TimeSeriesGateway.resolveColumnIndices(List.of("nosuchcolumn"), COLUMNS)).isEmpty();
    assertThat(TimeSeriesGateway.resolveColumnIndices(List.of("nosuchcolumn", "humidity"), COLUMNS))
        .containsExactly(3);
  }

  @Test
  void anEmptyProjectionMeansEveryColumn() {
    assertThat(TimeSeriesGateway.resolveColumnIndices(List.of(), COLUMNS)).isNull();
    assertThat(TimeSeriesGateway.resolveColumnIndices(null, COLUMNS)).isNull();
  }

  @Test
  void theNamesLineUpWithTheValuesTheEngineEmits() {
    final int[] indices = TimeSeriesGateway.resolveColumnIndices(List.of("humidity", "temperature"), COLUMNS);

    // The row is [timestamp, <selected non-ts columns in schema order>], so the names must be too.
    assertThat(TimeSeriesGateway.columnNames(COLUMNS, indices)).containsExactly("ts", "temperature", "humidity");
    assertThat(TimeSeriesGateway.columnNames(COLUMNS, indices)).hasSize(indices.length + 1);

    assertThat(TimeSeriesGateway.selectedColumns(COLUMNS, indices))
        .extracting(ColumnDefinition::getName)
        .containsExactly("ts", "temperature", "humidity");
  }

  @Test
  void noProjectionNamesEveryColumnInSchemaOrder() {
    assertThat(TimeSeriesGateway.columnNames(COLUMNS, null))
        .containsExactly("ts", "location", "sensor", "temperature", "humidity");
  }

  @Test
  void findColumnIndexUsesTheFullSchemaBecauseAggregationDoes() {
    // Deliberately the other convention: MultiColumnAggregationRequest#columnIndex() is a full-schema index,
    // as its own javadoc says. The two live side by side, so the distinction is pinned here rather than left
    // to be rediscovered.
    assertThat(TimeSeriesGateway.findColumnIndex("temperature", COLUMNS)).isEqualTo(3);
    assertThat(TimeSeriesGateway.findColumnIndex("ts", COLUMNS)).isZero();
    assertThat(TimeSeriesGateway.findColumnIndex("nosuchcolumn", COLUMNS)).isEqualTo(-1);
  }
}
