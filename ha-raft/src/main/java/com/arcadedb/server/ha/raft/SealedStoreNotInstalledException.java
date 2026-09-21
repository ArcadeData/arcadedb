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
 * A committed entry carried the sealed store of a TimeSeries type this node could not put in place (issue #8070).
 * <p>
 * The follow-up to issue #7602, whose rule is that an entry whose application did not actually take effect must not
 * be checkpointed as applied. {@code ArcadeStateMachine.applySealedBlobs} had exactly that shape for the
 * engine-less repair of issue #6839: {@code repairEngineWithSealedBlob} converts every failure into a SEVERE and a
 * {@code false}, the loop logged it and carried on, and the entry completed normally. A Raft entry is applied once
 * and never re-shipped - the method's own comment says so - so the blob that WAS the repair was consumed, the type
 * stayed engine-less for the life of the node, and nothing would ever send another. One SEVERE line was the whole
 * of the evidence.
 * <p>
 * <b>Thrown after the loop, never from inside it.</b> The contract the repair path was written to - one
 * unrepairable type must not abort the apply of an entry that may carry blobs for others - is still honoured:
 * every blob in the entry is attempted, the ones that CAN be installed are, and only then does this reach the
 * caller. What it changes is what happens next, which is the point: the entry is not recorded as applied.
 * <p>
 * <b>Why a plain unchecked type and not a {@link com.arcadedb.exception.ReplicationException}.</b> Same reasoning
 * as {@link LocalCommitNotAppliedException}, and the same choice. {@code ArcadeStateMachine.applyWithRetry}
 * forwards a {@code ReplicationException} unchanged, PAST the per-database quarantine, so it signals a resync
 * without marking the database diverged or arming the targeted snapshot download that would actually repair this
 * node. A type that is neither retryable nor a {@code ReplicationException} reaches
 * {@code handleUnexpectedApplyError} on the first pass, which quarantines the database, triggers the resync, and
 * keeps the node up for every other database it serves - which is precisely the disposition this failure wants:
 * the sealed store is not rebuildable from anything this node holds, and a snapshot from the leader is.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SealedStoreNotInstalledException extends RuntimeException {
  public SealedStoreNotInstalledException(final String message) {
    super(message);
  }
}
