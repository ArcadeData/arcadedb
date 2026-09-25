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

import com.arcadedb.database.ProtocolContext;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

import java.util.function.Supplier;

/**
 * Tags every callback of every gRPC call as a {@code grpc} request in {@link ProtocolContext} (issue #8363).
 * <p>
 * The tag used to be set by hand, and only by the RPCs that run a query, because query metrics were its only
 * reader. It has a second reader now: {@code RaftReplicatedDatabase} refuses a CLIENT request on a database whose
 * directory is being replaced from the leader's snapshot, and tells a client from the engine's own threads by this
 * tag. An RPC that did not set it - {@code LookupByRid}, {@code CreateRecord}, {@code UpdateRecord},
 * {@code DeleteRecord}, {@code BeginTransaction} - read as engine work and was served from the copy the cluster
 * was discarding. Setting it here, once, for every listener callback, reaches every RPC of every service
 * registered on the server, including the ones added after this was written.
 * <p>
 * The previous tag is restored after each callback rather than cleared, so a callback that runs on a thread
 * already carrying a tag leaves it as it found it. The RPCs that still set and clear the tag themselves are
 * unaffected: their clear happens inside the callback, and this puts back what was there before the callback.
 * <p>
 * Work an RPC hands to a client transaction's dedicated thread runs outside these callbacks, so
 * {@code ArcadeDbGrpcService} tags that thread's tasks itself.
 */
class GrpcProtocolContextInterceptor implements ServerInterceptor {

  static final String GRPC_PROTOCOL = "grpc";

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> call, final Metadata headers,
      final ServerCallHandler<ReqT, RespT> next) {
    final ServerCall.Listener<ReqT> delegate = taggedStart(() -> next.startCall(call, headers));
    return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(delegate) {
      @Override
      public void onMessage(final ReqT message) {
        tagged(() -> super.onMessage(message));
      }

      @Override
      public void onHalfClose() {
        tagged(super::onHalfClose);
      }

      @Override
      public void onCancel() {
        tagged(super::onCancel);
      }

      @Override
      public void onComplete() {
        tagged(super::onComplete);
      }

      @Override
      public void onReady() {
        tagged(super::onReady);
      }
    };
  }

  private static void tagged(final Runnable callback) {
    final String previous = ProtocolContext.get();
    ProtocolContext.set(GRPC_PROTOCOL);
    try {
      callback.run();
    } finally {
      ProtocolContext.set(previous);
    }
  }

  private static <T> T taggedStart(final Supplier<T> callback) {
    final String previous = ProtocolContext.get();
    ProtocolContext.set(GRPC_PROTOCOL);
    try {
      return callback.get();
    } finally {
      ProtocolContext.set(previous);
    }
  }
}
