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
package com.arcadedb.query.opencypher.executor;

import com.arcadedb.database.Identifiable;
import com.arcadedb.index.ConsistentKeyLookup;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.opencypher.temporal.TemporalUtil;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.OperationHeapLimit;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.RidHashSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Streams the records of a list of single-column index keys, opening the next key's cursor only when the current one is
 * drained and returning each RID once. The RIDs remembered are charged to the heap budget all queries share, as in
 * {@code NodeIndexSeek}. Shared by the IN-list seek of {@code MatchNodeStep} and the seeded start of a pattern comprehension
 * (issue #13622), so both read an index the same way.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
public final class DistinctKeySeek implements Iterator<Identifiable> {
  private final TypeIndex          index;
  private final List<Object[]>     keys;
  private final WorkGuard          guard;
  private final OperationHeapLimit seenHeapLimit;
  private       RidHashSet         seen;
  private       IndexCursor        cursor;
  private       int                nextKey;
  private       Identifiable       next;

  /**
   * @param operation what holds the RIDs remembered, as the heap budget's error message names it
   * @param dedupe    false for one range whose entries are distinct already: nothing is remembered then
   */
  public DistinctKeySeek(final CommandContext context, final String operation, final TypeIndex index, final List<Object[]> keys,
      final IndexCursor first, final int nextKey, final boolean dedupe) {
    this.index = index;
    this.keys = keys;
    this.cursor = first;
    this.nextKey = nextKey;
    this.guard = WorkGuard.forCommandDeadline(context);
    this.seenHeapLimit = OperationHeapLimit.of(context, "record addresses", operation);
    this.seen = dedupe ? new RidHashSet() : null;
  }

  /**
   * Every record the keys select, each once. Up to {@link ConsistentKeyLookup#MAX_ENTRIES} entries are read whole against
   * one committed state; past that the keys are sought one at a time as the caller pulls, so a LIMIT stops the seek early,
   * as in {@code NodeIndexSeek}. Null when a concurrent DDL dropped or rebuilt the index before the first cursor opened,
   * for the scan to answer; once records have been returned, such a failure surfaces as it does on the optimizer path.
   *
   * @param keys at least one key
   */
  public static DistinctKeySeek open(final CommandContext context, final String operation, final TypeIndex index,
      final List<Object[]> keys) {
    final IndexInternal[] subIndexes = index.getIndexesOnBuckets();
    final IndexCursor first;
    final int nextKey;
    try {
      final IndexCursor consistent = ConsistentKeyLookup.lookupCursor(context.getDatabase(), index, keys);
      first = consistent != null ? consistent : index.get(keys.getFirst());
      nextKey = consistent != null ? keys.size() : 1;
      if (!index.isUnchangedSince(subIndexes)) {
        first.close();
        return null;
      }
    } catch (final IndexException e) {
      return null;
    } catch (final IllegalArgumentException e) {
      if (TypeIndex.isStaleRead(index, e) || !index.isReadyForQueries())
        return null;
      throw e;
    }
    return new DistinctKeySeek(context, operation, index, keys, first, nextKey, true);
  }

  /**
   * The distinct single-column index keys of {@code elements}; null elements are dropped because they equal nothing. A RID
   * spelled as text is the id it stands for in a numeric column (issue #11933). Null when an element is not a plain scalar (a
   * list, a map, an array), which no key lookup answers the way {@code =} does: the caller scans instead.
   */
  public static List<Object[]> keysOf(final Iterable<?> elements, final Type[] keyTypes) {
    final Set<Object> distinct = new LinkedHashSet<>();
    for (final Object element : elements) {
      if (element == null)
        continue;
      if (element instanceof Collection<?> || element instanceof Map<?, ?> || element.getClass().isArray())
        return null;
      distinct.add(RidTextKeys.keyFor(TemporalUtil.toIndexKey(element), keyTypes, 0));
    }

    final List<Object[]> keys = new ArrayList<>(distinct.size());
    for (final Object key : distinct)
      keys.add(new Object[] { key });
    return keys;
  }

  @Override
  public boolean hasNext() {
    while (next == null && cursor != null) {
      guard.check();
      if (cursor.hasNext()) {
        final Identifiable identifiable = cursor.next();
        if (seen == null) {
          if (identifiable != null)
            next = identifiable;
        } else if (seen.add(identifiable.getIdentity())) {
          seenHeapLimit.charge(RidHashSet.BYTES_PER_ENTRY);
          next = identifiable;
        }
      } else {
        cursor.close();
        cursor = nextKey < keys.size() ? index.get(keys.get(nextKey++)) : null;
        if (cursor == null)
          releaseSeen();
      }
    }
    return next != null;
  }

  /** Gives back the cursor and the heap the RIDs seen hold, whether the seek is drained or abandoned. Safe to call twice. */
  public void close() {
    if (cursor != null) {
      cursor.close();
      cursor = null;
    }
    next = null;
    releaseSeen();
  }

  private void releaseSeen() {
    if (seen != null) {
      seen = null;
      seenHeapLimit.release();
    }
  }

  @Override
  public Identifiable next() {
    if (!hasNext())
      throw new NoSuchElementException();
    final Identifiable result = next;
    next = null;
    return result;
  }
}
