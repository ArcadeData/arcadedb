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

import java.util.Iterator;
import java.util.List;

/**
 * Iterator over timeseries samples. Each element is an Object[] where
 * index 0 is the timestamp (long) and subsequent indices are column values.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class TimeSeriesCursor implements Iterator<Object[]>, AutoCloseable {

  private final List<Object[]> data;
  private       int            position = 0;

  public TimeSeriesCursor(final List<Object[]> data) {
    this.data = data;
  }

  @Override
  public boolean hasNext() {
    return position < data.size();
  }

  @Override
  public Object[] next() {
    return data.get(position++);
  }

  public int size() {
    return data.size();
  }

  public void reset() {
    position = 0;
  }

  @Override
  public void close() {
    // Nothing to close for in-memory cursor
  }
}
