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
package com.arcadedb.remote.grpc;

import com.arcadedb.server.grpc.GrpcPackedArrays;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;

/**
 * The client end of the packed array negotiation of issue #11714, one per {@link RemoteGrpcDatabase} and channel generation (a
 * reconnect may reach another server, so it starts over with a new instance):
 * <ul>
 * <li>every call asks for {@code float[]} / {@code double[]} results packed ({@link GrpcPackedArrays#HEADER}), which
 * {@code ProtoUtils} decodes to the same arrays;</li>
 * <li>{@link #serverDecodesPackedArrays()} answers whether this database may SEND them packed: only once a response of the
 * server announced it with the same header. A server older than the packed kinds reads one as an unknown oneof member, a
 * null, and would store that, so until the announcement arrives (the first call included) vectors travel as lists, which
 * every server decodes.</li>
 * </ul>
 * The response headers decide, each time they arrive: a server swapped for an older one behind the same connection stops the
 * packed requests at its first answer. A response that carries no headers (a call that failed before any message: trailers
 * only, or a status the transport made up) can only confirm support through its trailers, never withdraw it, so a client-side
 * deadline or a refused authentication does not send the next call back to lists.
 */
class PackedArraysNegotiationInterceptor implements ClientInterceptor {

  private static final Metadata CONSENT = consent();

  private volatile boolean serverDecodesPackedArrays;

  boolean serverDecodesPackedArrays() {
    return serverDecodesPackedArrays;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(final MethodDescriptor<ReqT, RespT> method,
      final CallOptions callOptions, final Channel next) {
    return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
      @Override
      public void start(final Listener<RespT> responseListener, final Metadata headers) {
        headers.merge(CONSENT);
        super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(responseListener) {
          @Override
          public void onHeaders(final Metadata responseHeaders) {
            serverDecodesPackedArrays = announces(responseHeaders);
            super.onHeaders(responseHeaders);
          }

          @Override
          public void onClose(final Status status, final Metadata trailers) {
            if (announces(trailers))
              serverDecodesPackedArrays = true;
            super.onClose(status, trailers);
          }
        }, headers);
      }
    };
  }

  private static boolean announces(final Metadata metadata) {
    return metadata != null && "true".equalsIgnoreCase(metadata.get(GrpcPackedArrays.HEADER));
  }

  private static Metadata consent() {
    final Metadata consent = new Metadata();
    consent.put(GrpcPackedArrays.HEADER, "true");
    return consent;
  }
}
