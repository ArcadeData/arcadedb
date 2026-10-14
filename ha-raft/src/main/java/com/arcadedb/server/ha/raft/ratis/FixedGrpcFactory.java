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
package com.arcadedb.server.ha.raft.ratis;

import org.apache.ratis.conf.Parameters;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.grpc.server.GrpcServices;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.leader.FollowerInfo;
import org.apache.ratis.server.leader.LeaderState;
import org.apache.ratis.server.leader.LogAppender;

/**
 * {@link GrpcFactory} subclass that returns {@link FixedGrpcLogAppender} instead of the stock
 * {@code GrpcLogAppender}, working around RATIS-2523. See {@link FixedGrpcLogAppender} for the
 * detailed bug description and the conditions under which the fix kicks in.
 * <p>
 * It also hands the gRPC services a {@link GuardedRaftServer#wrap guarded} server, which refuses an inbound server RPC
 * addressed to another peer (issue #10016) and makes the target of a leadership transfer catch up with the commit it
 * was sent to campaign on before it campaigns (issue #8533).
 * <p>
 * Wired in via {@link FixedGrpcRpcType}. Both classes are intended to be deleted once
 * RATIS-2523 ships upstream (and the #8533 wrapper is no longer needed, see {@link TransfereeCatchUp}). The #10016
 * guard has no upstream counterpart yet, so dropping them means installing that wrapper some other way.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class FixedGrpcFactory extends GrpcFactory {

  private final GuardedRaftServer.MisdirectedRequestListener misdirectedRequestListener;

  public FixedGrpcFactory(final Parameters parameters) {
    super(parameters);
    this.misdirectedRequestListener = parameters == null ? null :
        parameters.get(GuardedRaftServer.MISDIRECTED_REQUEST_LISTENER_KEY,
            GuardedRaftServer.MisdirectedRequestListener.class);
  }

  @Override
  public LogAppender newLogAppender(final RaftServer.Division server, final LeaderState state, final FollowerInfo f) {
    return new FixedGrpcLogAppender(server, state, f);
  }

  @Override
  public GrpcServices newRaftServerRpc(final RaftServer server) {
    return super.newRaftServerRpc(GuardedRaftServer.wrap(server, misdirectedRequestListener));
  }
}
