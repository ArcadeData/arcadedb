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

import org.apache.ratis.grpc.server.GrpcServices;
import org.apache.ratis.thirdparty.io.grpc.ServerInterceptor;
import org.apache.ratis.thirdparty.io.grpc.ServerTransportFilter;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyServerBuilder;

import java.util.EnumSet;

/**
 * {@link GrpcServices.Customizer} that installs the configured server-side transport filters and call interceptors
 * on the Ratis Netty gRPC server builder. Ratis 3.2.2 routes all service types (ADMIN, CLIENT,
 * SERVER) through the same listener, so one customizer covers every inbound RPC.
 * <p>
 * A transport filter gates a connection once, when it is established; an interceptor is consulted on every RPC. The
 * peer allowlist needs both (issue #7250): the filter to refuse a connection, the interceptor to revoke one that was
 * established before its address stopped being admitted. Interceptors registered on the builder are server-wide -
 * {@code ServerImpl} applies them to every call whatever order the services were added in - so the two are installed
 * together here rather than per service.
 */
final class RaftGrpcServicesCustomizer implements GrpcServices.Customizer {

  private static final ServerTransportFilter[] NO_FILTERS      = new ServerTransportFilter[0];
  private static final ServerInterceptor[]     NO_INTERCEPTORS = new ServerInterceptor[0];

  private final ServerTransportFilter[] filters;
  private final ServerInterceptor[]     interceptors;

  RaftGrpcServicesCustomizer(final ServerTransportFilter[] filters, final ServerInterceptor[] interceptors) {
    this.filters = filters == null ? NO_FILTERS : filters;
    this.interceptors = interceptors == null ? NO_INTERCEPTORS : interceptors;
  }

  @Override
  public NettyServerBuilder customize(final NettyServerBuilder builder, final EnumSet<GrpcServices.Type> types) {
    NettyServerBuilder result = builder;
    for (final ServerTransportFilter f : filters)
      result = result.addTransportFilter(f);
    for (final ServerInterceptor i : interceptors)
      result = result.intercept(i);
    return result;
  }
}
