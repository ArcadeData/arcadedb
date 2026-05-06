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
import org.apache.ratis.rpc.RpcFactory;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.rpc.SupportedRpcType;

/**
 * Custom {@link RpcType} that registers {@link FixedGrpcFactory} as the Ratis transport factory.
 * The Ratis startup code calls {@link RpcType#valueOf(String)} on the configured type name; if
 * the name is not a {@link SupportedRpcType}, Ratis treats it as a fully-qualified class name
 * and reflectively instantiates it via its no-arg constructor. This class therefore needs a
 * public no-arg constructor.
 * <p>
 * The {@link #name()} method intentionally returns {@code "GRPC"} so any code in our HA layer
 * that compares against {@code SupportedRpcType.GRPC.name()} keeps working.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class FixedGrpcRpcType implements RpcType {

  public FixedGrpcRpcType() {
  }

  @Override
  public String name() {
    return SupportedRpcType.GRPC.name();
  }

  @Override
  public RpcFactory newFactory(final Parameters parameters) {
    return new FixedGrpcFactory(parameters);
  }
}
