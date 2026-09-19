/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

/**
 * A committed entry this node ORIGINATED whose pages could neither be published from the prepared transaction nor
 * reconciled from the replicated payload (issue #7602).
 * <p>
 * It exists to make the two apply paths answer alike. A follower reaching the same wall - {@code applyChanges}
 * throwing on the entry's own WAL bytes - throws, and the throw is what routes it to the per-database quarantine
 * and the targeted snapshot resync of issue #4797. The locally-originated path did not: the reconcile failure was
 * logged and swallowed, {@code applyTxEntry} returned normally, and the applied index advanced over an entry whose
 * pages are not on this node. A snapshot then checkpointed that position, so the entry was never replayed either -
 * the node that originated the write is the one that lost it, permanently and silently, while every follower had
 * it. The #5407 replay floor that used to tolerate this state (the {@code lowestPendingLocalPhase2Floor} clamp on
 * {@code takeSnapshot}) went with the #6965 rework of the local-commit path, which is why nothing caught it.
 * <p>
 * <b>Why a type of its own rather than rethrowing the reconcile failure.</b> The disposition has to be
 * deterministic, and the two failures it is built from are not: the publication failure can be an MVCC
 * {@code ConcurrentModificationException}, which {@code ArcadeStateMachine.applyWithRetry} classifies as
 * retryable, and the reconcile failure can be anything {@code applyChanges} raises. Retrying is pointless here -
 * the claim has already been consumed, so a retry runs the follower path, which is precisely the reconcile that
 * just failed - and a {@code ReplicationException} would be forwarded unchanged, past the quarantine. A plain
 * unchecked type that is neither reaches {@code handleUnexpectedApplyError} on the first pass, which quarantines
 * the database and arms the resync. Both original failures ride along: the publication failure as the cause, the
 * reconcile failure suppressed on it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LocalCommitNotAppliedException extends RuntimeException {
  private final String databaseName;

  public LocalCommitNotAppliedException(final String message, final String databaseName, final Throwable cause) {
    super(message, cause);
    this.databaseName = databaseName;
  }

  public String getDatabaseName() {
    return databaseName;
  }
}
