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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The edges of unidirectional types a transaction created or deleted, kept by its {@code TransactionContext} so that a
 * query's {@link IncomingEdgeLookup} can stay on the scan it took and add what its own transaction changed since,
 * rather than scan the type again after every write (issue #8625).
 * <p>
 * Every change takes the next value of a sequence that is never reset, so a scan knows which changes came after it. The
 * changes themselves only live until the transaction ends: {@link #transactionEnded()} drops them and records where the
 * next transaction starts, and a scan taken before that point is no longer covered by what is kept - it has to be taken
 * again, since the changes it missed are now committed or rolled back.
 * <p>
 * Changes of other transactions are never seen here: a query keeps reading the scan it took, as it keeps reading the
 * records it already loaded.
 * <p>
 * Not thread-safe: a transaction belongs to one thread.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class UnidirectionalEdgeChanges {
  /** An edge created in the transaction, the endpoints as the edge list stores them. */
  record Created(long sequence, Edge edge, RID source) {
  }

  private long                                    sequence;
  private long                                    transactionStart;
  // BY TYPE, THEN BY TARGET: A LOOKUP READS THE CHANGES OF ONE TYPE INTO ONE VERTEX
  private Map<String, Map<RID, List<Created>>> created;
  private Map<RID, Long>                        deleted;

  /** The sequence of the last change, in any transaction of the context holding this object. */
  public long getSequence() {
    return sequence;
  }

  /** The sequence the current transaction started at: the changes kept are the ones after it. */
  public long getTransactionStart() {
    return transactionStart;
  }

  public void edgeCreated(final String typeName, final Edge edge, final RID source, final RID target) {
    if (created == null)
      created = new HashMap<>();
    created.computeIfAbsent(typeName, k -> new HashMap<>()).computeIfAbsent(target, k -> new ArrayList<>(2))
        .add(new Created(++sequence, edge, source));
  }

  public void edgeDeleted(final RID edgeIdentity) {
    if (deleted == null)
      deleted = new HashMap<>();
    deleted.put(edgeIdentity, ++sequence);
  }

  /** Drops the changes of the transaction that ended: committed or rolled back, they are no longer its own. */
  public void transactionEnded() {
    created = null;
    deleted = null;
    transactionStart = sequence;
  }

  /** The edges of {@code typeName} into {@code target} the transaction created, in creation order. */
  List<Created> createdInto(final String typeName, final RID target) {
    if (created == null)
      return Collections.emptyList();
    final Map<RID, List<Created>> byTarget = created.get(typeName);
    if (byTarget == null)
      return Collections.emptyList();
    final List<Created> list = byTarget.get(target);
    return list != null ? list : Collections.emptyList();
  }

  /** Whether the edge was deleted after {@code afterSequence}. */
  boolean isDeletedAfter(final RID edgeIdentity, final long afterSequence) {
    if (deleted == null)
      return false;
    final Long at = deleted.get(edgeIdentity);
    return at != null && at > afterSequence;
  }

  boolean hasDeletions() {
    return deleted != null && !deleted.isEmpty();
  }
}
