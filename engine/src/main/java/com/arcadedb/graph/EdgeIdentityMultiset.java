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
package com.arcadedb.graph;

import com.arcadedb.database.RID;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The edges a backtracking search currently holds: {@link #add} when a branch takes an edge, {@link #remove} when it
 * gives it back, {@link #contains} to keep a branch from walking an edge it already holds. It is the removable
 * counterpart of {@link EdgeIdentitySet}, whose record-backed half is a primitive set with no removal.
 * <p>
 * Edges are told apart as {@link LightEdgeRID#isSameEdge} does, so two parallel lightweight edges of one type over the
 * same ordered pair, which share their RID, are two edges here (issues #9573, #9983). Each RID maps to the one entry
 * holding it, or to a list once a second entry with an equal RID arrives: only parallel lightweight twins (or the same
 * edge added twice) ever share a key, so the list stays as short as the number of twins.
 * <p>
 * A multiset: an edge added twice must be removed twice. A search that only adds an edge it does not hold never needs
 * that, but a seed taken from the bindings of a row can name the same edge twice, and it must not be released by the
 * first removal.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class EdgeIdentityMultiset {
  // A KEY MAPS TO THE RID OF ITS ONLY ENTRY, OR TO A List<RID> OF THE ENTRIES WITH AN EQUAL RID
  private final Map<RID, Object> entries = new HashMap<>();

  @SuppressWarnings("unchecked")
  public void add(final RID edgeRID) {
    final Object held = entries.get(edgeRID);
    if (held == null)
      entries.put(edgeRID, edgeRID);
    else if (held instanceof List<?> list)
      ((List<RID>) list).add(edgeRID);
    else {
      final List<RID> list = new ArrayList<>(2);
      list.add((RID) held);
      list.add(edgeRID);
      entries.put(edgeRID, list);
    }
  }

  @SuppressWarnings("unchecked")
  public boolean contains(final RID edgeRID) {
    final Object held = entries.get(edgeRID);
    if (held == null)
      return false;
    if (held instanceof List<?> list) {
      for (final RID entry : (List<RID>) list)
        if (LightEdgeRID.isSameEdge(entry, edgeRID))
          return true;
      return false;
    }
    return LightEdgeRID.isSameEdge((RID) held, edgeRID);
  }

  /**
   * Removes one entry for the edge, the very RID object that was added first (the common case of a search giving back
   * what it took), then any entry that is the same edge. The reference match comes first because it is free, while
   * {@link LightEdgeRID#isSameEdge} on two twins may have to walk an edge list to number them.
   *
   * @return true if an entry was removed
   */
  @SuppressWarnings("unchecked")
  public boolean remove(final RID edgeRID) {
    final Object held = entries.get(edgeRID);
    if (held == null)
      return false;
    if (!(held instanceof List<?>)) {
      if (held != edgeRID && !LightEdgeRID.isSameEdge((RID) held, edgeRID))
        return false;
      entries.remove(edgeRID);
      return true;
    }

    final List<RID> list = (List<RID>) held;
    int found = -1;
    for (int i = list.size() - 1; i >= 0; i--)
      if (list.get(i) == edgeRID) {
        found = i;
        break;
      }
    if (found < 0)
      for (int i = list.size() - 1; i >= 0; i--)
        if (LightEdgeRID.isSameEdge(list.get(i), edgeRID)) {
          found = i;
          break;
        }
    if (found < 0)
      return false;

    list.remove(found);
    if (list.size() == 1)
      entries.put(edgeRID, list.get(0));
    return true;
  }

  public boolean isEmpty() {
    return entries.isEmpty();
  }
}
