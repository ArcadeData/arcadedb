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

import com.arcadedb.remote.grpc.utils.ProtoUtils;
import com.arcadedb.server.grpc.ArcadeDbServiceGrpc;
import com.arcadedb.server.grpc.GrpcPackedArrays;
import com.arcadedb.server.grpc.GrpcValue;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11714: a server older than the packed kinds reads a {@code float_array_value} as an unknown oneof member, a null, and
 * stores it. The client therefore sends a {@code float[]} packed only after a response of the server announced that it decodes
 * them, and lists until then: the first call, every call to a server that never announces, and every call after a server that
 * stopped announcing (an older one swapped in behind the same address) answered.
 */
class PackedArraysNegotiationIssue11714Test {

  private static final float[] VECTOR = { 0.25f, -0.5f, 0.75f };

  private final PackedArraysNegotiationInterceptor negotiation = new PackedArraysNegotiationInterceptor();
  private final AnsweringChannel                   server      = new AnsweringChannel();

  @Test
  void aClientThatHasNotHeardTheServerSendsLists() {
    assertThat(negotiation.serverDecodesPackedArrays()).isFalse();
    assertThat(encode().getKindCase()).isEqualTo(GrpcValue.KindCase.LIST_VALUE);
  }

  @Test
  void aServerThatNeverAnnouncesKeepsGettingLists() {
    call(new Metadata(), Status.OK, new Metadata());
    call(null, Status.UNAVAILABLE, new Metadata());

    assertThat(negotiation.serverDecodesPackedArrays()).isFalse();
    assertThat(encode().getKindCase()).isEqualTo(GrpcValue.KindCase.LIST_VALUE);
  }

  @Test
  void afterTheAnnouncementTheClientSendsPacked() {
    call(announcement(), Status.OK, announcement());

    assertThat(negotiation.serverDecodesPackedArrays()).isTrue();
    final GrpcValue packed = encode();
    assertThat(packed.getKindCase()).isEqualTo(GrpcValue.KindCase.FLOAT_ARRAY_VALUE);
    assertThat(GrpcPackedArrays.toFloats(packed.getFloatArrayValue())).containsExactly(VECTOR);
  }

  @Test
  void aCallThatFailedBeforeAnyMessageAnnouncesThroughItsTrailers() {
    call(null, Status.PERMISSION_DENIED, announcement());

    assertThat(negotiation.serverDecodesPackedArrays()).isTrue();
  }

  @Test
  void aResponseWithoutHeadersDoesNotWithdrawTheSupport() {
    call(announcement(), Status.OK, announcement());
    // A deadline, a refused connection, an outer interceptor's refusal: no headers, and trailers that say nothing
    call(null, Status.DEADLINE_EXCEEDED, new Metadata());

    assertThat(negotiation.serverDecodesPackedArrays()).isTrue();
  }

  @Test
  void anOlderServerAnsweringBehindTheSameAddressSendsTheClientBackToLists() {
    call(announcement(), Status.OK, announcement());
    call(new Metadata(), Status.OK, new Metadata());

    assertThat(negotiation.serverDecodesPackedArrays()).isFalse();
    assertThat(encode().getKindCase()).isEqualTo(GrpcValue.KindCase.LIST_VALUE);
  }

  @Test
  void everyCallAsksForPackedResults() {
    call(new Metadata(), Status.OK, new Metadata());

    assertThat(server.sentHeaders.get(GrpcPackedArrays.HEADER)).isEqualTo("true");
  }

  private GrpcValue encode() {
    return ProtoUtils.toGrpcValue(VECTOR, negotiation.serverDecodesPackedArrays());
  }

  private void call(final Metadata responseHeaders, final Status status, final Metadata trailers) {
    server.responseHeaders = responseHeaders;
    server.status = status;
    server.trailers = trailers;
    negotiation.interceptCall(ArcadeDbServiceGrpc.getExecuteCommandMethod(), CallOptions.DEFAULT, server)
        .start(new ClientCall.Listener<>() {
        }, new Metadata());
  }

  private static Metadata announcement() {
    final Metadata metadata = new Metadata();
    metadata.put(GrpcPackedArrays.HEADER, "true");
    return metadata;
  }

  /** Answers every call at once with the headers (none when {@code null}), status and trailers it was given. */
  private static final class AnsweringChannel extends Channel {
    private Metadata responseHeaders;
    private Status   status;
    private Metadata trailers;
    private Metadata sentHeaders;

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(final MethodDescriptor<ReqT, RespT> method, final CallOptions callOptions) {
      return new ClientCall<>() {
        @Override
        public void start(final Listener<RespT> listener, final Metadata headers) {
          sentHeaders = headers;
          if (responseHeaders != null)
            listener.onHeaders(responseHeaders);
          listener.onClose(status, trailers);
        }

        @Override
        public void request(final int numMessages) {
        }

        @Override
        public void cancel(final String message, final Throwable cause) {
        }

        @Override
        public void halfClose() {
        }

        @Override
        public void sendMessage(final ReqT message) {
        }
      };
    }

    @Override
    public String authority() {
      return "answering";
    }
  }
}
