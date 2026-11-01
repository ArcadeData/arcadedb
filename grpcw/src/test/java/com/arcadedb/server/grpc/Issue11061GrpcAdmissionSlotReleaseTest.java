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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.query.sql.executor.QueryAdmissionGate;
import io.grpc.Attributes;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.Status;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11061: everything {@link GrpcAdmissionInterceptor} does after admitting a call - listing it in the running
 * statements, watching its Context for a cancellation - runs inside the {@code try} whose {@code finally} gives the slot
 * back. Both used to run before it, so a failure there escaped {@code onHalfClose} with the slot held: gRPC then closes
 * the raw call, not the {@code AdmittedCall} whose {@code close()} is the other release point, and the slot stayed taken
 * for the life of the process. An interceptor built without a server (which keeps no registry) was that failure:
 * {@code register()} dereferenced the server unguarded while the rest of the class guards it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11061GrpcAdmissionSlotReleaseTest {
  private final QueryAdmissionGate gate = QueryAdmissionGate.getInstance();

  @BeforeEach
  void gateWithOneSlotAndNoQueue() {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.setValue(1);
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.setValue(0L);
  }

  @AfterEach
  void restoreGate() {
    GlobalConfiguration.QUERY_MAX_CONCURRENT.reset();
    GlobalConfiguration.QUERY_QUEUE_TIMEOUT.reset();
  }

  @Test
  void aCallAdmittedWithoutAServerIsServedAndGivesItsSlotBack() throws Exception {
    // Each call on a thread of its own: a thread that holds a slot shares it with the next call it admits, which would hide a
    // slot left behind
    for (int i = 0; i < 3; i++) {
      final RecordingCall call = new RecordingCall();
      CompletableFuture.runAsync(() -> {
        final ServerCall.Listener<ExecuteQueryRequest> listener = new GrpcAdmissionInterceptor(null).interceptCall(call,
            new Metadata(), (c, h) -> new ServerCall.Listener<>() {
              @Override
              public void onHalfClose() {
                c.sendMessage(ExecuteQueryResponse.getDefaultInstance());
                c.close(Status.OK, new Metadata());
              }
            });
        listener.onMessage(ExecuteQueryRequest.getDefaultInstance());
        listener.onHalfClose();
      }).get(30, TimeUnit.SECONDS);

      assertThat(call.status).as("call %d", i).isNotNull();
      assertThat(call.status.getCode()).as("call %d was refused by a slot left behind", i).isEqualTo(Status.Code.OK);
      assertThat(gate.getRunning()).as("slots held after call %d", i).isZero();
    }
  }

  private static final class RecordingCall extends ServerCall<ExecuteQueryRequest, ExecuteQueryResponse> {
    private volatile Status status;

    @Override
    public void request(final int numMessages) {
    }

    @Override
    public void sendHeaders(final Metadata headers) {
    }

    @Override
    public void sendMessage(final ExecuteQueryResponse message) {
    }

    @Override
    public void close(final Status status, final Metadata trailers) {
      this.status = status;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public MethodDescriptor<ExecuteQueryRequest, ExecuteQueryResponse> getMethodDescriptor() {
      return ArcadeDbServiceGrpc.getExecuteQueryMethod();
    }

    @Override
    public Attributes getAttributes() {
      return Attributes.EMPTY;
    }
  }
}
