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
package com.arcadedb.index;

import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionIndexContext;
import com.arcadedb.database.TransactionIndexContext.IndexKey;
import com.arcadedb.database.TransactionIndexContext.IndexKey.IndexKeyOperation;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * The disk RIDs that the pending changes on ONE key of a transaction's index overlay hide from a read, until commit
 * replays them onto the pages. This is the single place the rules live (#6970); every index read path that merges
 * the overlay ({@code LSMTreeIndex.get()}, {@code HashIndex.get()}, {@code LSMTreeIndexCursor}) feeds the per-key
 * entries of {@link TransactionIndexContext#getIndexKeys(String)} through {@link #accumulate} and then applies the
 * answer to its own result shape.
 * <p>
 * The rules, per entry:
 * <ol>
 *   <li>{@code REMOVE} with a RID hides that RID.</li>
 *   <li>{@code REMOVE} with no RID ({@code remove(keys)}), or ANY {@code REMOVE} on a unique index (the key holds at
 *   most one RID), hides the whole key.</li>
 *   <li>{@code REPLACE} with a non-null {@code oldRid} hides {@code oldRid}: it is a same-key REMOVE + ADD merged into
 *   one entry by {@code TransactionIndexContext.addIndexKeyLock}, and {@code TransactionIndexContext.commit()} replays
 *   exactly that removal. A {@code REPLACE} whose {@code oldRid} is null (today only the cross-bucket duplicate check)
 *   replays no removal and hides nothing.</li>
 * </ol>
 * These must stay in step with the replay in {@code TransactionIndexContext.commit()}: if the merge rules ever
 * produce a new operation shape, this is the class to re-audit.
 * <p>
 * The point lookups ({@code LSMTreeIndex.get()}, {@code HashIndex.get()}) answer a whole-key removal on a unique
 * index with an empty cursor before calling {@link #accumulate}, because nothing on disk or in the overlay survives
 * it. They ask {@link #removesWholeKey} for that, the same predicate {@link #accumulate} uses, so rule 2 still has
 * one definition.
 * <p>
 * Hot path: {@link #accumulate} returns {@code null} - and allocates nothing - for a key with no pending removal. The
 * instance, and its RID set, are created lazily on the first removal only, and a key-wide removal never allocates
 * the set at all.
 */
public final class PendingIndexRemovals {
  private boolean  keyWide;
  private Set<RID> rids;

  private PendingIndexRemovals() {
  }

  /**
   * Folds one overlay entry into the removals collected so far for its key.
   * <p>
   * The caller must always reassign the returned reference ({@code removals = accumulate(removals, entry, unique)}):
   * the first removal of a key creates the instance, so ignoring the result while {@code current} is still null
   * silently loses that removal.
   *
   * @param current the removals collected so far for this key, or null when none yet
   * @param value   the next overlay entry of this key
   * @param unique  whether the index is unique
   *
   * @return the updated removals: {@code current} itself (possibly still null) when the entry hides nothing, else a
   * non-null instance
   */
  public static PendingIndexRemovals accumulate(final PendingIndexRemovals current, final IndexKey value, final boolean unique) {
    final RID hidden;
    if (value.operation == IndexKeyOperation.REMOVE) {
      if (removesWholeKey(value, unique)) {
        final PendingIndexRemovals result = current != null ? current : new PendingIndexRemovals();
        result.keyWide = true;
        result.rids = null;
        return result;
      }
      hidden = value.rid;
    } else if (value.operation == IndexKeyOperation.REPLACE && value.oldRid != null)
      hidden = value.oldRid;
    else
      return current;

    final PendingIndexRemovals result = current != null ? current : new PendingIndexRemovals();
    if (!result.keyWide) {
      if (result.rids == null)
        result.rids = new HashSet<>();
      result.rids.add(hidden);
    }
    return result;
  }

  /**
   * Rule 2: true when the overlay entry hides every disk RID of its key - a {@code REMOVE} carrying no RID, or any
   * {@code REMOVE} on a unique index, whose key holds at most one RID.
   */
  public static boolean removesWholeKey(final IndexKey value, final boolean unique) {
    return value.operation == IndexKeyOperation.REMOVE && (value.rid == null || unique);
  }

  /** True when every disk RID of the key is hidden. */
  public boolean isKeyWide() {
    return keyWide;
  }

  /** True when the disk RID must not be returned by the read. */
  public boolean hides(final Identifiable rid) {
    return keyWide || (rids != null && rids.contains(rid.getIdentity()));
  }

  /** Removes from {@code diskRids} every RID hidden by these removals. */
  public void removeFrom(final Collection<RID> diskRids) {
    if (keyWide)
      diskRids.clear();
    else if (rids != null)
      diskRids.removeAll(rids);
  }
}
