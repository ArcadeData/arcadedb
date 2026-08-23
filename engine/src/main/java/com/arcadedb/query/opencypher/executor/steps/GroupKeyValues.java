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
package com.arcadedb.query.opencypher.executor.steps;

import java.util.Objects;

/**
 * Represents the values of one or more grouping keys, used as a {@code HashMap} key by the
 * aggregation steps (issue #6629: a row-multiplying clause upstream of a GROUP BY, such as
 * UNWIND, must still collapse into one output row per distinct key combination). Caches its
 * hash code since the same instance is hashed on every row of its group.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class GroupKeyValues {
  final Object[] values;
  private final int hash;

  GroupKeyValues(final Object[] values) {
    this.values = values;
    int h = 1;
    for (final Object v : values)
      h = 31 * h + (v == null ? 0 : v.hashCode());
    this.hash = h;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (!(o instanceof GroupKeyValues that))
      return false;
    if (hash != that.hash || values.length != that.values.length)
      return false;
    for (int i = 0; i < values.length; i++)
      if (!Objects.equals(values[i], that.values[i]))
        return false;
    return true;
  }

  @Override
  public int hashCode() {
    return hash;
  }
}
