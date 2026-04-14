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
package com.arcadedb.server.ha.raft;

import com.arcadedb.exception.ConfigurationException;

/**
 * Quorum modes supported for Raft HA replication.
 *
 * <p><strong>{@code MAJORITY}:</strong> A transaction is considered committed once a majority of Raft peers
 * have acknowledged it. This is the default and matches standard Raft semantics.
 *
 * <p><strong>{@code ALL}:</strong> After MAJORITY acknowledgement, the leader additionally issues a
 * Ratis Watch(ALL_COMMITTED) call to wait until every peer has applied the entry. Success means
 * <em>all nodes have confirmed</em>. However, if the leader steps down or a follower stalls
 * between the MAJORITY ack and the ALL watch completion, the watch may time out even though the
 * entry is already majority-committed (and therefore durable). In this case,
 * {@link MajorityCommittedAllFailedException} is thrown to the caller. The entry is committed and
 * will eventually be applied on all nodes, but the caller cannot assume synchronous all-node
 * visibility. Callers that require all-node confirmation should retry or verify independently.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 * @see MajorityCommittedAllFailedException
 */
public enum Quorum {
  MAJORITY, ALL;

  public static Quorum parse(final String value) {
    return switch (value.toLowerCase()) {
      case "majority" -> MAJORITY;
      case "all" -> ALL;
      default -> throw new ConfigurationException(
          "Unsupported HA quorum mode '" + value + "'. Only 'majority' and 'all' are supported with Ratis");
    };
  }
}
