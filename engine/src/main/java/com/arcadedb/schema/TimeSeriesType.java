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
package com.arcadedb.schema;

import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.DownsamplingTier;
import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

import java.util.ArrayList;
import java.util.List;

/**
 * Schema TimeSeries Type: the declaration of a time-series type, independent of where it lives.
 * <p>
 * This is the abstraction {@link TimeSeriesTypeBuilder#create()} returns, so one body of builder code produces the
 * same type description whether the {@link Schema} it ran against was embedded or remote (issue #7399). Before it
 * existed, the only TimeSeries type was {@code LocalTimeSeriesType} and the builder's terminal operation named it
 * directly, which is what made a remote implementation impossible to write rather than merely unwritten.
 * <p>
 * What it deliberately does NOT carry is the storage engine: {@code getEngine()} stays on
 * {@code LocalTimeSeriesType}, because a remote type has no engine to hand back and a method that exists only to
 * throw is worse than one that is absent. Code that needs the engine is engine-internal and already holds the
 * local type.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@ExcludeFromJacocoGeneratedReport
public interface TimeSeriesType extends DocumentType {
  /**
   * Name of the column holding the sample timestamp, the one declared as {@code TIMESTAMP} in
   * {@code CREATE TIMESERIES TYPE}.
   */
  String getTimestampColumn();

  /**
   * Timestamp precision as declared ({@code NANOSECOND}, {@code MICROSECOND}, {@code MILLISECOND} or
   * {@code SECOND}), or {@code null} when the type did not declare one.
   */
  String getPrecision();

  int getShardCount();

  long getRetentionMs();

  long getCompactionBucketIntervalMs();

  /**
   * The declared columns in declaration order: the TIMESTAMP, the TAGs and the FIELDs.
   */
  List<ColumnDefinition> getTsColumns();

  List<DownsamplingTier> getDownsamplingTiers();

  /**
   * The declared column named {@code columnName}, or {@code null} when the type declares no such column.
   */
  default ColumnDefinition getTsColumn(final String columnName) {
    final List<ColumnDefinition> columns = getTsColumns();
    // Indexed over the list rather than an enhanced for: this runs on the DDL path for every property validation
    // and a column list is a handful of entries, so the iterator allocation buys nothing.
    for (int i = 0; i < columns.size(); i++) {
      final ColumnDefinition col = columns.get(i);
      if (col.getName().equals(columnName))
        return col;
    }
    return null;
  }

  /**
   * Whether {@code columnName} is one of this type's declared time-series columns. A schema property outside that
   * set can never hold a value: the write path reads the document under each declared column's name only.
   */
  default boolean isDeclaredColumn(final String columnName) {
    return getTsColumn(columnName) != null;
  }

  /**
   * The declared column names, in declaration order, for error messages that have to tell the user what the type
   * actually accepts.
   */
  default List<String> getTsColumnNames() {
    final List<ColumnDefinition> columns = getTsColumns();
    final List<String> names = new ArrayList<>(columns.size());
    for (int i = 0; i < columns.size(); i++)
      names.add(columns.get(i).getName());
    return names;
  }
}
