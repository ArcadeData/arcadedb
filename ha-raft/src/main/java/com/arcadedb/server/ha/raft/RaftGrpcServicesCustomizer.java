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
import java.util.concurrent.TimeUnit;

/**
 * {@link GrpcServices.Customizer} that installs the configured server-side transport filters and call interceptors
 * on the Ratis Netty gRPC server builder. Ratis (3.3.0, the version {@code ha-raft/pom.xml} pins) routes ADMIN,
 * CLIENT and SERVER through one listener unless {@code raft.grpc.admin.port} or {@code raft.grpc.client.port} names
 * a different port; either way this customizer covers every inbound RPC, because {@code GrpcServicesImpl.buildServer}
 * runs it on each builder it constructs rather than only on the SERVER one.
 * <p>
 * A transport filter gates a connection once, when it is established; an interceptor is consulted on every RPC. The
 * peer allowlist needs both (issue #7250): the filter to refuse a connection, the interceptor to revoke one that was
 * established before its address stopped being admitted. Interceptors registered on the builder are server-wide -
 * {@code ServerImpl} applies them to every call whatever order the services were added in - so the two are installed
 * together here rather than per service.
 * <p>
 * It also carries the connection-lifetime bounds the Raft listener otherwise has none of (issues #7316, #7339).
 * Neither of the two surfaces above can close a connection: {@code ServerTransportFilter} is handed no reference to
 * the transport it admits, and a {@code ServerCall} reaches only its own HTTP/2 stream. So a peer whose reach a
 * revocation took away kept its socket until something else dropped it. The only knobs gRPC exposes for that are
 * builder-wide, and {@code GrpcServicesImpl.newNettyServerBuilder} sets none of them, which is why this customizer is
 * where the windows are applied.
 * <p>
 * The two windows answer different halves of the same question, and a deployment can want either, both or neither:
 * <ul>
 * <li>{@code maxConnectionIdle} (#7316) is measured from the moment the connection's last RPC finished, so it reaps
 * the connections that went quiet and never touches one that is carrying traffic - including the leader's
 * {@code AppendEntries} stream, which is why it was safe to default it on.</li>
 * <li>{@code maxConnectionAge} (#7339) is armed once, when the connection is established, and fires on schedule
 * whatever the connection is carrying. It is the only bound that closes a peer which keeps <i>starting</i> RPCs: each
 * one opens and closes an HTTP/2 stream and pushes the idle deadline forward by the whole window, refused ones
 * included. It recycles healthy connections on the same period, so it defaults to off.</li>
 * </ul>
 */
final class RaftGrpcServicesCustomizer implements GrpcServices.Customizer {

  private static final ServerTransportFilter[] NO_FILTERS      = new ServerTransportFilter[0];
  private static final ServerInterceptor[]     NO_INTERCEPTORS = new ServerInterceptor[0];

  private final ServerTransportFilter[] filters;
  private final ServerInterceptor[]     interceptors;
  private final long                    maxConnectionIdleMs;
  private final long                    maxConnectionAgeMs;
  private final long                    maxConnectionAgeGraceMs;

  /**
   * @param maxConnectionIdleMs     how long a connection may carry no RPC before the server closes it, or {@code 0}
   *                                or less to leave the idle window unbounded, which is what Ratis does on its own
   * @param maxConnectionAgeMs      how long a connection may live whatever it is carrying, or {@code 0} or less to
   *                                leave its age unbounded
   * @param maxConnectionAgeGraceMs how long the RPCs still running on a connection that reached that age have to
   *                                finish; read only when {@code maxConnectionAgeMs} is positive, and clamped at 0
   *                                so a negative value from a configuration file cannot throw out of
   *                                {@code NettyServerBuilder.maxConnectionAgeGrace}'s argument check at startup
   */
  RaftGrpcServicesCustomizer(final ServerTransportFilter[] filters, final ServerInterceptor[] interceptors,
      final long maxConnectionIdleMs, final long maxConnectionAgeMs, final long maxConnectionAgeGraceMs) {
    this.filters = filters == null ? NO_FILTERS : filters;
    this.interceptors = interceptors == null ? NO_INTERCEPTORS : interceptors;
    this.maxConnectionIdleMs = maxConnectionIdleMs;
    this.maxConnectionAgeMs = maxConnectionAgeMs;
    this.maxConnectionAgeGraceMs = Math.max(0L, maxConnectionAgeGraceMs);
  }

  @Override
  public NettyServerBuilder customize(final NettyServerBuilder builder, final EnumSet<GrpcServices.Type> types) {
    NettyServerBuilder result = builder;
    for (final ServerTransportFilter f : filters)
      result = result.addTransportFilter(f);
    for (final ServerInterceptor i : interceptors)
      result = result.intercept(i);
    // Not gated on the allowlist: this is a connection-lifetime bound, and Ratis leaves the Raft listener without
    // one whether or not the allowlist is installed. gRPC clamps anything under a second up to a second and treats
    // anything from 1000 days up as "disabled", so the only value handled here is the one that means off.
    if (maxConnectionIdleMs > 0)
      result = result.maxConnectionIdle(maxConnectionIdleMs, TimeUnit.MILLISECONDS);
    // The age bound (#7339): the idle window above is pushed forward by every RPC, so it never closes a peer that
    // keeps starting them. This one is armed once, in NettyServerHandler.handlerAdded, and fires on schedule.
    // The grace is always passed alongside it rather than left at gRPC's default, which is infinite: under an
    // infinite grace a connection carrying a stream that does not end - which is exactly what a leader's
    // AppendEntries is - would wait out its GOAWAY forever, and the bound would not bound. gRPC still reads a
    // CONFIGURED grace of 1000 days or more as infinite (AS_LARGE_AS_INFINITE), which the setting's description
    // says; what this line rules out is arriving there by not setting it.
    if (maxConnectionAgeMs > 0)
      result = result.maxConnectionAge(maxConnectionAgeMs, TimeUnit.MILLISECONDS)
          .maxConnectionAgeGrace(maxConnectionAgeGraceMs, TimeUnit.MILLISECONDS);
    return result;
  }
}
