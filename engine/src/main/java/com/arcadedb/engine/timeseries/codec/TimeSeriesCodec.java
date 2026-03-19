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
package com.arcadedb.engine.timeseries.codec;

/**
 * Defines the compression codec types used by TimeSeries columnar storage.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public enum TimeSeriesCodec {
  DELTA_OF_DELTA(0),
  GORILLA_XOR(1),
  DICTIONARY(2),
  SIMPLE8B(3),
  NONE(255);

  private final int code;

  TimeSeriesCodec(final int code) {
    this.code = code;
  }

  public int getCode() {
    return code;
  }

  public static TimeSeriesCodec fromCode(final int code) {
    for (final TimeSeriesCodec codec : values())
      if (codec.code == code)
        return codec;
    throw new IllegalArgumentException("Unknown codec code: " + code);
  }
}
