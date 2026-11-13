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
package com.arcadedb.server.grpc;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

import java.util.concurrent.Callable;

/**
 * Both halves of the packed array negotiation of issue #11714, for every call:
 * <ul>
 * <li><b>Announcement.</b> Every response carries {@link GrpcPackedArrays#HEADER}{@code : true}, in its headers and in its
 * trailers (a call that fails before any message sends trailers only), telling the client this server decodes
 * {@code float_array_value} / {@code double_array_value}. A client sends a {@code float[]} packed only after a response said
 * so: a server older than the packed kinds reads one as an unknown oneof member, which is a {@code null}, and stores it.</li>
 * <li><b>Consent.</b> Whether the call sent the same header, the caller's consent to receive a {@code float[]} or
 * {@code double[]} result packed rather than as a {@code list_value}. A client generated from an older proto reads an unknown
 * oneof member as {@code null}, so sending it unasked would turn every stored vector into a null on its end.</li>
 * </ul>
 * The encoders read the consent through {@link #requested()}, from the gRPC {@link Context}. Work an RPC runs on a client
 * transaction's dedicated thread has no gRPC context, so {@code ArcadeDbGrpcService} captures the answer on the call's thread
 * and hands it over with {@link #callWith(boolean, Callable)}.
 */
class GrpcPackedArraysInterceptor implements ServerInterceptor {

  private static final Context.Key<Boolean> PACKED_ARRAYS_KEY = Context.key("arcadedb-packed-arrays");
  // Derived from ROOT, not from a call's context: on a transaction's thread it carries the consent and nothing else
  private static final Context              PACKED_CONTEXT    = Context.ROOT.withValue(PACKED_ARRAYS_KEY, Boolean.TRUE);

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> call, final Metadata headers,
      final ServerCallHandler<ReqT, RespT> next) {
    final ServerCall<ReqT, RespT> announcing = new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
      @Override
      public void sendHeaders(final Metadata responseHeaders) {
        responseHeaders.put(GrpcPackedArrays.HEADER, "true");
        super.sendHeaders(responseHeaders);
      }

      @Override
      public void close(final Status status, final Metadata trailers) {
        trailers.put(GrpcPackedArrays.HEADER, "true");
        super.close(status, trailers);
      }
    };
    if (!"true".equalsIgnoreCase(headers.get(GrpcPackedArrays.HEADER)))
      return next.startCall(announcing, headers);
    return Contexts.interceptCall(Context.current().withValue(PACKED_ARRAYS_KEY, Boolean.TRUE), announcing, headers, next);
  }

  /** Whether the current call asked for packed array results. */
  static boolean requested() {
    return PACKED_ARRAYS_KEY.get() == Boolean.TRUE;
  }

  /** Runs {@code task} with the consent {@link #requested()} answered on the call's thread, on a thread that has no context. */
  static <T> T callWith(final boolean packedArrays, final Callable<T> task) throws Exception {
    return packedArrays ? PACKED_CONTEXT.call(task) : task.call();
  }
}
