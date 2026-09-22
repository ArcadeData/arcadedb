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

import com.arcadedb.server.grpc.TransactionProtocol;
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
 * Client end of {@link TransactionProtocol#SESSION_PARTIAL_COMMIT_TRAILER} (issue #8134).
 * <p>
 * It is attached to the two DATA-PLANE stubs in {@code RemoteGrpcDatabase.rebuildStubs()}, so a server that
 * says "your own transaction published a commit under this call" is heard whichever data-plane RPC said it -
 * and on the call that FAILED as readily as on the one that succeeded, because a trailer arrives with either
 * close while a response message arrives with only one. The admin-plane stub, built per call by
 * {@code newAdminBlockingStub}, is deliberately not wrapped: the admin service registers no transaction-scoped
 * dispatch at all, so no admin RPC can run work inside a caller's transaction and none can earn the trailer.
 * <p>
 * Latching, never clearing: the teardown a failed attempt performs on its way out (a rollback, which is itself
 * a call) must not be able to un-say a verdict an earlier call in the same transaction reached. What clears it
 * is beginning the next transaction, which is where {@code RemoteDatabase} puts the reset.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SessionPartialCommitInterceptor implements ClientInterceptor {

  private final RemoteGrpcDatabase database;

  SessionPartialCommitInterceptor(final RemoteGrpcDatabase database) {
    this.database = database;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(final MethodDescriptor<ReqT, RespT> method,
      final CallOptions callOptions, final Channel next) {

    return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
      @Override
      public void start(final Listener<RespT> responseListener, final Metadata headers) {
        super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(responseListener) {
          @Override
          public void onClose(final Status status, final Metadata trailers) {
            // Only ever sent with "true"; its absence is the negative, so presence is the whole test.
            if (trailers != null && trailers.containsKey(TransactionProtocol.SESSION_PARTIAL_COMMIT_TRAILER))
              database.latchSessionPartialCommit();

            super.onClose(status, trailers);
          }
        }, headers);
      }
    };
  }
}
