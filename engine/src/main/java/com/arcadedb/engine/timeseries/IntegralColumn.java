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

import com.arcadedb.engine.timeseries.codec.Simple8bCodec;

import java.io.IOException;

/**
 * A decoded {@code SIMPLE8B} block column: its values as {@code long}s and the samples that carry none (issue #9721).
 * <p>
 * An absent sample holds a zero in {@link #values}, so a reader that wants the values has to ask {@link #isNull} first; the
 * bitmap is {@code null} for a column every sample of which has a value, which is every block written before the format had one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class IntegralColumn {
  final long[] values;
  /** Bit {@code i} of word {@code i >>> 6} set when sample {@code i} is absent; {@code null} when none is. */
  final long[] nulls;

  IntegralColumn(final long[] values, final long[] nulls) {
    this.values = values;
    this.nulls = nulls;
  }

  static IntegralColumn decode(final byte[] compressed) throws IOException {
    return new IntegralColumn(Simple8bCodec.decode(compressed), Simple8bCodec.nulls(compressed));
  }

  boolean isNull(final int index) {
    return nulls != null && (nulls[index >>> 6] & (1L << index)) != 0;
  }

  int size() {
    return values.length;
  }
}
