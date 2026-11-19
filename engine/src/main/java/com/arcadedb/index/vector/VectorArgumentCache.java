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
package com.arcadedb.index.vector;

import com.arcadedb.query.sql.executor.CommandContext;

/**
 * The float[] form of a vector argument, computed once per command instead of once per row.
 * <p>
 * A scan that scores every record against one query vector calls the vector function once per row, and the query vector is
 * the same object every time: it is the parameter of the statement, and the parameter is one object for the whole command.
 * The function nevertheless re-derived it every call: it converted the parameter - a {@code List<Double>} of 1024 boxed
 * doubles - into a {@code float[]}. Measured over 100k calls at 1024 dimensions, that is 1.33 us per row, so a scan of
 * 100k rows spends 133 ms repeating work whose answer does not change.
 * <p>
 * The cached form is held for the lifetime of the command, in the command's own cache, and is keyed by the identity of the
 * object it was derived from: the same object is the same vector, and a different object is a different vector.
 * <p>
 * A scan asks for two vectors on every row: the query vector, which is the parameter and the same object every row, and
 * the record's own vector, which is a fresh object every row. Only the first is worth keeping, and the two are told apart by
 * how often they are asked for: an entry is counted every time it answers, and the entry that answers least is the one
 * given up when there is no free slot. The query vector therefore stays for the whole scan, while the record's own vector
 * - asked for once and never again - is the one that goes.
 * <p>
 * The two conversions are kept apart because they differ: {@link #toFloatArray} refuses a {@code byte[]} (it is a
 * quantized vector, not one of plain coordinates) while {@link #toFloatArrayOfCoordinates} widens its bytes as
 * coordinates. An entry of one kind is never answered for the other.
 */
public final class VectorArgumentCache {
  /**
   * The key the cached forms are held under. The name is opaque to the command context, which never interprets it.
   */
  private static final String CACHE_KEY = "vector_argument_float_array";

  /**
   * How many converted forms are kept. A scan asks for the query vector and the record's own vector on every row, so two
   * of them are in flight at any time; the room for one more lets a second constant vector stay too.
   */
  private static final int SLOTS = 4;

  private VectorArgumentCache() {
  }

  /**
   * The vector, prepared for the cosine, as {@link VectorUtils#toFloatArray(Object)} reads it: converted now and reused for
   * the rest of the command.
   *
   * @param vector  the vector to prepare
   * @param context the command the cache lives in, or null when there is none
   *
   * @return the prepared vector
   */
  public static VectorUtils.PreparedVector prepare(final Object vector, final CommandContext context) {
    return convert(vector, context, Kind.SQL);
  }

  /**
   * The vector, prepared for the cosine, as {@link VectorUtils#toFloatArrayOfCoordinates(Object)} reads it: converted now
   * and reused for the rest of the command.
   *
   * @param vector  the vector to prepare
   * @param context the command the cache lives in, or null when there is none
   *
   * @return the prepared vector
   */
  public static VectorUtils.PreparedVector prepareCoordinates(final Object vector, final CommandContext context) {
    return convert(vector, context, Kind.COORDINATES);
  }

  /**
   * How a vector is converted: the two conversions differ on a {@code byte[]}.
   */
  private enum Kind {
    SQL,
    COORDINATES
  }

  private static VectorUtils.PreparedVector convert(final Object vector, final CommandContext context, final Kind kind) {
    if (context == null)
      return convertNow(vector, kind);

    final Entries entries = (Entries) context.getCachedValue(CACHE_KEY);
    if (entries != null) {
      final VectorUtils.PreparedVector cached = entries.get(vector, kind);
      if (cached != null)
        return cached;
    }

    final VectorUtils.PreparedVector converted = convertNow(vector, kind);
    if (entries != null)
      entries.put(vector, kind, converted);
    else
      context.setCachedValue(CACHE_KEY, new Entries(vector, kind, converted));
    return converted;
  }

  private static VectorUtils.PreparedVector convertNow(final Object vector, final Kind kind) {
    final float[] values = kind == Kind.SQL ? VectorUtils.toFloatArray(vector) : VectorUtils.toFloatArrayOfCoordinates(vector);
    return VectorUtils.prepare(values);
  }

  /**
   * The converted forms the command keeps.
   */
  private static final class Entries {
    private final Object[]                 sources = new Object[SLOTS];
    private final Kind[]                   kinds   = new Kind[SLOTS];
    private final VectorUtils.PreparedVector[] prepared = new VectorUtils.PreparedVector[SLOTS];
    private final int[]                    hits    = new int[SLOTS];

    private Entries(final Object source, final Kind kind, final VectorUtils.PreparedVector vector) {
      sources[0] = source;
      kinds[0] = kind;
      prepared[0] = vector;
      hits[0] = 1;
    }

    /**
     * The converted form of this object, or null when it is not one the command keeps.
     * <p>
     * The object is compared by identity, not by {@code equals}: the query vector of a scan is the one object the statement
     * carries as its parameter, and comparing a 1024-element list with {@code equals} on every row would cost what the
     * cache saves. A vector that is not that same object is simply converted again.
     */
    @SuppressWarnings("PMD.CompareObjectsWithEquals") // identity on purpose: "is this the object I already converted?"
    private VectorUtils.PreparedVector get(final Object candidate, final Kind kind) {
      for (int i = 0; i < SLOTS; i++)
        if (sources[i] == candidate && kinds[i] == kind) {
          hits[i]++;
          return prepared[i];
        }
      return null;
    }

    /**
     * The converted form of this object, kept for the rest of the command unless the room runs out.
     */
    private void put(final Object source, final Kind kind, final VectorUtils.PreparedVector vector) {
      for (int i = 0; i < SLOTS; i++)
        if (sources[i] == null) {
          sources[i] = source;
          kinds[i] = kind;
          prepared[i] = vector;
          hits[i] = 1;
          return;
        }
      // The room is full: the entry that answered least goes, and this one takes its place. The query vector of a scan
      // answers on every row, so it is never the one that goes.
      int victim = 0;
      for (int i = 1; i < SLOTS; i++)
        if (hits[i] < hits[victim])
          victim = i;
      sources[victim] = source;
      kinds[victim] = kind;
      prepared[victim] = vector;
      hits[victim] = 1;
    }
  }
}
