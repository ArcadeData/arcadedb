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

import java.util.List;

/**
 * The newest sample of a time-series type, optionally restricted by tag (issue #7305).
 *
 * @param type    the queried type
 * @param columns names of the values in {@code latest}, in order; always every column of the type
 * @param latest  the newest sample, or {@code null} when the selection holds none
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public record TimeSeriesLatestResult(String type, List<String> columns, Object[] latest) {

  public TimeSeriesLatestResult {
    columns = columns == null ? List.of() : List.copyOf(columns);
    latest = latest == null ? null : latest.clone();
  }

  @Override
  public Object[] latest() {
    // Defensive copy: see TimeSeriesBucket#values().
    return latest == null ? null : latest.clone();
  }

  /** Whether the selection held any sample at all. */
  public boolean isPresent() {
    return latest != null;
  }

  /** The value of {@code columnName} in the newest sample, or {@code null} when absent or not present. */
  public Object value(final String columnName) {
    if (latest == null)
      return null;
    final int index = columns.indexOf(columnName);
    return index < 0 || index >= latest.length ? null : latest[index];
  }
}
