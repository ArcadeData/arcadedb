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

import com.arcadedb.exception.ConfigurationException;

import org.apache.ratis.protocol.RaftPeerId;

/**
 * Thrown when a leadership operation is asked of a node that is not the leader (issue #7134).
 * <p>
 * A distinct type rather than a plain {@link ConfigurationException} because the two mean different things to a
 * caller and must not share an HTTP status: this one says "right request, wrong node - reissue it against the
 * leader named in the message" (409, and the leader is named so the caller can act on it), while a
 * {@code ConfigurationException} raised by a transfer that the LEADER attempted and failed says "this node tried
 * and could not", which keeps the mapping it always had. Collapsing them would have turned every genuine
 * transfer failure into a 409 that invites a retry against the same node.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class NotTheLeaderRefusalException extends ConfigurationException {

  /**
   * @param prefix   what is being refused, e.g. {@code "Refusing to step down"}; the rest of the sentence is
   *                 built here so every guard phrases the refusal identically
   * @param leaderId the current leader, or {@code null} when none is known (an election is in flight)
   */
  public NotTheLeaderRefusalException(final String prefix, final RaftPeerId leaderId) {
    super(prefix + ": this node is not the leader"
        + (leaderId != null ? ", the current leader is '" + leaderId + "' - reissue the request against that node"
        : " and no leader is currently known - retry once one is elected"));
  }
}
