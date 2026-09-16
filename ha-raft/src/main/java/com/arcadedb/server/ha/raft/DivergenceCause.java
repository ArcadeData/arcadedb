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
 * Why a database was quarantined from the committed Raft log, carried into the cluster status document and the
 * operator alert (issue #7741).
 * <p>
 * The quarantine had one cause when it was built - a WAL version gap, which is a replication problem - and the
 * status document said so in as many words. Issue #7495 added a second, an entry this node cannot decode, which
 * is a corrupt local log segment and points at this node's own disk rather than at the leader. Describing every
 * quarantine as the first one sends an operator to read the leader's logs for a fault only they can see.
 * <p>
 * The recovery is the same whichever it is - the database is reinstalled from the leader - so this changes what
 * is SAID, not what is done.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public enum DivergenceCause {

  /**
   * An entry's WAL page version was more than one ahead of the local page: an intermediate transaction never
   * reached this node, so its pages are behind the committed log (issues #4740, #4797).
   */
  WAL_VERSION_GAP("a WAL version gap: an intermediate transaction never reached this node"),

  /**
   * A committed entry this node cannot read back - truncated, corrupt, or written in a shape this version does
   * not understand (issues #7138, #7495). Nothing is wrong with replication: the bytes on THIS node's log are.
   */
  UNDECODABLE_LOG_ENTRY("a Raft log entry this node cannot decode, which is a corrupt local log segment or an "
      + "entry written by a newer node - not a replication fault"),

  /**
   * An unexpected error while applying a committed entry for the database, which leaves its in-memory state
   * unknown (issue #4797).
   */
  APPLY_ERROR("an unexpected error while applying a committed entry"),

  /**
   * A snapshot install completed without bringing the database to the snapshot's index, so this node holds a copy
   * it knows is behind and clamps its reads at an honest floor until a resync refreshes it (issue #6760). Nothing
   * failed while applying anything: the install is what did not finish the job (CodeRabbit on PR #7747).
   */
  SNAPSHOT_INSTALL_INCOMPLETE("a snapshot install that did not bring this database up to the snapshot's index");

  private final String description;

  DivergenceCause(final String description) {
    this.description = description;
  }

  /** The phrase the alert reads, as the tail of "quarantined after ...". */
  public String getDescription() {
    return description;
  }
}
