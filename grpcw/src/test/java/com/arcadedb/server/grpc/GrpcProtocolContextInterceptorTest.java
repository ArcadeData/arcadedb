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
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Issue #8363: {@code RaftReplicatedDatabase} refuses a CLIENT request on a database whose directory is being
 * replaced from the leader's snapshot, and tells a client from the engine's own threads by {@link ProtocolContext}.
 * Several gRPC RPCs never set that tag, so they read as engine work and were served from the discarded copy. The
 * interceptor tags every callback of every call, and must hand the thread back as it found it.
 */
class GrpcProtocolContextInterceptorTest {

  @AfterEach
  void clearTag() {
    ProtocolContext.clear();
  }

  @Test
  @SuppressWarnings("unchecked")
  void everyCallbackRunsTaggedAsGrpcAndTheThreadIsHandedBackUntouched() {
    final List<String> seen = new ArrayList<>();
    final ServerCall.Listener<Object> recording = new ServerCall.Listener<>() {
      @Override
      public void onMessage(final Object message) {
        seen.add("onMessage=" + ProtocolContext.get());
      }

      @Override
      public void onHalfClose() {
        seen.add("onHalfClose=" + ProtocolContext.get());
        // An RPC that still sets and clears the tag itself, the way executeQuery does.
        ProtocolContext.set("grpc");
        ProtocolContext.clear();
      }

      @Override
      public void onCancel() {
        seen.add("onCancel=" + ProtocolContext.get());
      }

      @Override
      public void onComplete() {
        seen.add("onComplete=" + ProtocolContext.get());
      }

      @Override
      public void onReady() {
        seen.add("onReady=" + ProtocolContext.get());
      }
    };
    final ServerCallHandler<Object, Object> handler = (call, headers) -> {
      seen.add("startCall=" + ProtocolContext.get());
      return recording;
    };

    final ServerCall.Listener<Object> listener = new GrpcProtocolContextInterceptor()
        .interceptCall(mock(ServerCall.class), new Metadata(), handler);
    assertThat(ProtocolContext.get()).isEqualTo(ProtocolContext.INTERNAL);

    listener.onReady();
    listener.onMessage("request");
    listener.onHalfClose();
    listener.onComplete();
    listener.onCancel();

    assertThat(seen).containsExactly("startCall=grpc", "onReady=grpc", "onMessage=grpc", "onHalfClose=grpc",
        "onComplete=grpc", "onCancel=grpc");
    assertThat(ProtocolContext.get()).as("a pooled gRPC thread is not left tagged for its next task")
        .isEqualTo(ProtocolContext.INTERNAL);
  }

  @Test
  @SuppressWarnings("unchecked")
  void aThreadAlreadyTaggedKeepsItsOwnTag() {
    final ServerCall.Listener<Object> noop = new ServerCall.Listener<>() {
    };
    final ServerCall.Listener<Object> listener = new GrpcProtocolContextInterceptor()
        .interceptCall(mock(ServerCall.class), new Metadata(), (call, headers) -> noop);

    ProtocolContext.set("http");
    listener.onHalfClose();
    assertThat(ProtocolContext.get()).isEqualTo("http");
  }
}
