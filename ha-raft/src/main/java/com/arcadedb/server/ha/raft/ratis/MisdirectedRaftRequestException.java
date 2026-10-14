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
package com.arcadedb.server.ha.raft.ratis;

import org.apache.ratis.protocol.RaftPeerId;

import java.io.IOException;

/**
 * Refusal of a Raft server RPC that reached a node other than the peer it was addressed to (issue #10016).
 * <p>
 * An {@link IOException} because every server-protocol method of Ratis declares one, so the refusal travels back to
 * the sender through Ratis's own gRPC error path, message included, instead of being wrapped into an
 * {@code UndeclaredThrowableException} that would reach the wire without it. Public, with the single-{@code String}
 * constructor Ratis's {@code GrpcUtil} looks for, so a sender on the same build gets this type back rather than a
 * plain {@link IOException}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MisdirectedRaftRequestException extends IOException {

  public MisdirectedRaftRequestException(final String message) {
    super(message);
  }

  MisdirectedRaftRequestException(final String rpc, final RaftPeerId addressedTo, final RaftPeerId requestor,
      final RaftPeerId receiver) {
    this("Raft " + rpc + " addressed to " + addressedTo + " (sent by " + (requestor != null ? requestor : "<unknown>")
        + ") reached " + receiver
        + " instead, and was refused: a node never answers for another peer. The address the sender holds for "
        + addressedTo + " leads to " + receiver + ", typically a host name that now resolves elsewhere (issue #10016)");
  }
}
