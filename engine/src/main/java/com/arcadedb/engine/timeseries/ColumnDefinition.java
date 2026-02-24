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

import com.arcadedb.engine.timeseries.codec.TimeSeriesCodec;
import com.arcadedb.schema.Type;

/**
 * Defines a column in a TimeSeries type.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class ColumnDefinition {

  public enum ColumnRole {
    TIMESTAMP, TAG, FIELD
  }

  private final String          name;
  private final Type            dataType;
  private final ColumnRole      role;
  private final TimeSeriesCodec compressionHint;

  public ColumnDefinition(final String name, final Type dataType, final ColumnRole role) {
    this(name, dataType, role, defaultCodecFor(dataType, role));
  }

  public ColumnDefinition(final String name, final Type dataType, final ColumnRole role, final TimeSeriesCodec compressionHint) {
    this.name = name;
    this.dataType = dataType;
    this.role = role;
    this.compressionHint = compressionHint;
  }

  public String getName() {
    return name;
  }

  public Type getDataType() {
    return dataType;
  }

  public ColumnRole getRole() {
    return role;
  }

  public TimeSeriesCodec getCompressionHint() {
    return compressionHint;
  }

  /**
   * Returns the fixed byte size for this column's data type in the mutable row format.
   * Variable-length types (STRING) return -1; the caller must handle dictionary encoding.
   */
  public int getFixedSize() {
    return switch (dataType) {
      case LONG, DATETIME -> 8;
      case DOUBLE -> 8;
      case INTEGER -> 4;
      case FLOAT -> 4;
      case SHORT -> 2;
      case BYTE -> 1;
      case BOOLEAN -> 1;
      default -> -1; // Variable length (STRING etc.)
    };
  }

  @Override
  public String toString() {
    return name + " " + dataType + " (" + role + ")";
  }

  private static TimeSeriesCodec defaultCodecFor(final Type dataType, final ColumnRole role) {
    if (role == ColumnRole.TIMESTAMP)
      return TimeSeriesCodec.DELTA_OF_DELTA;
    if (role == ColumnRole.TAG)
      return TimeSeriesCodec.DICTIONARY;
    return switch (dataType) {
      case DOUBLE, FLOAT -> TimeSeriesCodec.GORILLA_XOR;
      case LONG, INTEGER, SHORT, BYTE -> TimeSeriesCodec.SIMPLE8B;
      default -> TimeSeriesCodec.DICTIONARY;
    };
  }
}
