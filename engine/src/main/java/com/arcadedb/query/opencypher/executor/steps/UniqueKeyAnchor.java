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

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.index.ConsistentKeyLookup;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;

import java.util.Arrays;

/**
 * The one vertex a position of a count push-down is pinned to by an equality on every column of a UNIQUE index (issue
 * #11594). The key is resolved when the operator is built, from literals and parameters only, and the vertex is looked up
 * when the operator runs, so a write made by an earlier clause of the same statement is seen.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class UniqueKeyAnchor {
  private final TypeIndex index;
  private final Object[]  key;

  public UniqueKeyAnchor(final TypeIndex index, final Object[] key) {
    if (!index.isUnique())
      throw new IllegalArgumentException("Index '" + index.getName() + "' is not unique");
    if (key.length != index.getPropertyNames().size())
      throw new IllegalArgumentException("Expected a value for each of " + index.getPropertyNames() + ", found " + key.length);
    this.index = index;
    this.key = key;
  }

  /**
   * The vertex holding the key, or null when no record holds it. Read against one committed state, and refused with the
   * {@link com.arcadedb.index.IndexException} of {@link TypeIndex#getForQuery} when a concurrent DDL left the index unable to
   * answer whole: the caller then walks the label, which applies the same equality as a predicate.
   */
  public RID resolve(final Database database) {
    try (final IndexCursor cursor = ConsistentKeyLookup.getForQuery(database, index, key)) {
      return cursor.hasNext() ? cursor.next().getIdentity() : null;
    }
  }

  public String describe() {
    return index.getTypeName() + index.getPropertyNames() + " = " + Arrays.toString(key);
  }
}
