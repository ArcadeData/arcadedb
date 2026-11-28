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

import java.util.Arrays;

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
   * of them are in flight at any time; the room for two more lets a second constant vector stay too.
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
    // Entries are never changed once published: a parallel scan's worker reads the ones its parent context holds while it
    // keeps what it converts in its own, so growing means a new Entries that replaces the old one in this context only
    final Slot slot = new Slot(vector, kind, converted);
    final Entries grown = entries == null ? new Entries(new Slot[] { slot }) : entries.with(slot);
    if (grown != null)
      context.setCachedValue(CACHE_KEY, grown);
    return converted;
  }

  private static VectorUtils.PreparedVector convertNow(final Object vector, final Kind kind) {
    final float[] values = kind == Kind.SQL ? VectorUtils.toFloatArray(vector) : VectorUtils.toFloatArrayOfCoordinates(vector);
    return VectorUtils.prepare(values);
  }

  /**
   * One vector the command has converted: the object it was handed, how it was converted and the result.
   */
  private record Slot(Object source, Kind kind, VectorUtils.PreparedVector prepared) {
  }

  /**
   * The converted forms the command keeps: an immutable list of at most {@link #SLOTS}, so that no thread can see one half
   * written and no entry is replaced under a reader. The room is not reused: a vector met after it ran out is converted every
   * time, as it was before the cache. The first call of a scan prepares the query vector together with the first record's own
   * vector, so the query vector is always among those kept.
   */
  private static final class Entries {
    private final Slot[] slots;

    private Entries(final Slot[] slots) {
      this.slots = slots;
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
      for (final Slot slot : slots)
        if (slot.source() == candidate && slot.kind() == kind)
          return slot.prepared();
      return null;
    }

    /**
     * These entries and one more, or null when the room has run out.
     */
    private Entries with(final Slot slot) {
      if (slots.length >= SLOTS)
        return null;
      final Slot[] grown = Arrays.copyOf(slots, slots.length + 1);
      grown[slots.length] = slot;
      return new Entries(grown);
    }
  }
}
