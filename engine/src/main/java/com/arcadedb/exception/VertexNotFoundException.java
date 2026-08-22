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
package com.arcadedb.exception;

import com.arcadedb.database.RID;

/**
 * A caller about to WRITE a vertex found the vertex RECORD ITSELF absent (#6572). The reference it arrived through -
 * an adjacency entry, a RID held across transactions, a traversal step - names a record that is not there.
 * <p>
 * ONE type for that fact, whichever operation met it (#6586): REMOVING an entry from an edge list
 * ({@code GraphEngine.getEdgeHeadChunkForWrite}), APPENDING one to it ({@code GraphEngine.getOrCreateEdgeList} -
 * typically the far endpoint of an edge being created, which is not the vertex the caller named), or DELETING the
 * vertex outright ({@code GraphEngine.deleteVertex}, which probes the slot before it walks anything). A sweep over a
 * graph that has to skip such a reference catches this and only this, instead of matching on messages or on the
 * accident of which read noticed first.
 * <p>
 * A {@link RecordNotFoundException}, and deliberately NOT a {@link NeedRetryException}, which is the whole reason
 * this type exists. The read side of an edge list cannot always tell "gone" from "not visible yet": a concurrent
 * commit publishes its pages one at a time and the reader takes no commit lock, so a HEAD CHUNK can legitimately
 * be unreadable a moment before it appears, and {@code GraphEngine.getEdgeHeadChunkForWrite} answers that window
 * with a retryable {@link ConcurrentModificationException} (#5670). One case inside that window is not a window at
 * all: when the missing record is the VERTEX the caller asked to modify, no amount of retrying makes it exist
 * again. Reported as a conflict it cost the caller a full retry budget of identical failures and then rolled back
 * a transaction - a single stale reference could kill a whole batch job, every run.
 * <p>
 * The repair is also different, which is the second reason the two must not share a type. An unreadable CHUNK is
 * repaired by {@code CHECK DATABASE RECORD <vertex> FIX}, which rebuilds that vertex's edge list from the
 * surviving edge records. Here the vertex is precisely what is gone, so there is nothing to rebuild the list of;
 * what has to be dropped is the reference on the OTHER endpoint, which only a database-wide (or type-wide) sweep
 * can find - no index maps a vertex back to the lists that name it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class VertexNotFoundException extends RecordNotFoundException {
  public VertexNotFoundException(final String s, final RID rid, final Exception e) {
    super(s, rid, e);
  }
}
