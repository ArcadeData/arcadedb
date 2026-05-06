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
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.leader.FollowerInfo;
import org.apache.ratis.server.leader.LeaderState;
import org.apache.ratis.server.leader.LogAppender;

/**
 * {@link GrpcFactory} subclass that returns {@link FixedGrpcLogAppender} instead of the stock
 * {@code GrpcLogAppender}, working around RATIS-2523. See {@link FixedGrpcLogAppender} for the
 * detailed bug description and the conditions under which the fix kicks in.
 * <p>
 * Wired in via {@link FixedGrpcRpcType}. Both classes are intended to be deleted once
 * RATIS-2523 ships upstream.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class FixedGrpcFactory extends GrpcFactory {

  public FixedGrpcFactory(final Parameters parameters) {
    super(parameters);
  }

  @Override
  public LogAppender newLogAppender(final RaftServer.Division server, final LeaderState state, final FollowerInfo f) {
    return new FixedGrpcLogAppender(server, state, f);
  }
}
