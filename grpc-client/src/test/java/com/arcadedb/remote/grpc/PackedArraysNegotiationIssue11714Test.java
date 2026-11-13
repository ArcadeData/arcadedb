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
import com.arcadedb.server.grpc.ExecuteCommandRequest;
import com.arcadedb.server.grpc.GrpcPackedArrays;
import com.arcadedb.server.grpc.GrpcValue;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11714: a server older than the packed kinds reads a {@code float_array_value} as an unknown oneof member, a null, and
 * stores it. The client therefore sends a {@code float[]} packed only after a response of the server announced that it decodes
 * them, and lists until then: the first call, every call to a server that never announces, every call after a server that
 * stopped announcing (an older one swapped in behind the same address) answered, and the first call on the channel a
 * {@code close()} / {@code start()} cycle of the {@link RemoteGrpcServer} put in place.
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

  /**
   * What one server announced says nothing about the server a new channel reaches. Before the negotiation was bound to the
   * channel generation, the first call after a reconnect to an older server still sent the vector packed, which that server
   * stores as a null.
   */
  @Test
  void theFirstCallAfterAReconnectSendsListsUntilTheNewServerAnnounces() {
    final ReconnectingServer remote = new ReconnectingServer(true);
    try (final RemoteGrpcDatabase database = remote.database()) {
      execute(database);
      assertThat(remote.sentVector().getKindCase()).as("nothing announced yet").isEqualTo(GrpcValue.KindCase.LIST_VALUE);
      execute(database);
      assertThat(remote.sentVector().getKindCase()).as("announced").isEqualTo(GrpcValue.KindCase.FLOAT_ARRAY_VALUE);

      remote.reconnect(false);

      execute(database);
      assertThat(remote.sentVector().getKindCase()).as("first call on the new channel").isEqualTo(GrpcValue.KindCase.LIST_VALUE);
      execute(database);
      assertThat(remote.sentVector().getKindCase()).as("an older server never announces").isEqualTo(GrpcValue.KindCase.LIST_VALUE);

      remote.reconnect(true);

      execute(database);
      assertThat(remote.sentVector().getKindCase()).as("a newer server, not heard yet").isEqualTo(GrpcValue.KindCase.LIST_VALUE);
      execute(database);
      assertThat(remote.sentVector().getKindCase()).as("the newer server announced").isEqualTo(GrpcValue.KindCase.FLOAT_ARRAY_VALUE);
    }
  }

  /**
   * A call issued on the replaced channel can answer after the reconnect. Its announcement speaks for the server behind the
   * old channel, so the next call, on the new one, still goes as lists.
   */
  @Test
  void anAnnouncementArrivingOnTheReplacedChannelIsIgnored() throws InterruptedException {
    final ReconnectingServer remote = new ReconnectingServer(true);
    try (final RemoteGrpcDatabase database = remote.database()) {
      remote.holdNextAnswer();
      final Thread inFlight = new Thread(() -> execute(database), "issue-11714-in-flight");
      inFlight.start();
      assertThat(remote.sent.await(10, TimeUnit.SECONDS)).as("the call reached the old channel").isTrue();

      remote.reconnect(false);
      remote.release.countDown();
      inFlight.join(TimeUnit.SECONDS.toMillis(10));
      assertThat(inFlight.isAlive()).as("the held call answered").isFalse();

      execute(database);
      assertThat(remote.sentVector().getKindCase()).isEqualTo(GrpcValue.KindCase.LIST_VALUE);
    }
  }

  private static void execute(final RemoteGrpcDatabase database) {
    database.execSql("INSERT INTO Embedding SET v = :v", Map.of("v", VECTOR), 10_000);
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

  /**
   * A {@link RemoteGrpcServer} whose channel is a fake that answers every call at once, and whose
   * {@link #reconnect(boolean) reconnect} does what a {@code close()} / {@code start()} cycle does to the generation. The fake
   * is never dialed, so the gRPC port is never addressed.
   */
  private static final class ReconnectingServer extends RemoteGrpcServer {
    private static final String HOST = InetAddress.getLoopbackAddress().getHostAddress();

    private volatile long                  generation = 1;
    private volatile boolean               announces;
    private volatile ExecuteCommandRequest lastRequest;
    private volatile CountDownLatch        sent;
    private volatile CountDownLatch        release;
    private volatile boolean               holding;

    private ReconnectingServer(final boolean announces) {
      super(HOST, 0, "root", "root", true, List.of());
      this.announces = announces;
    }

    /**
     * The database's constructor asks its HTTP port for the cluster configuration and falls back to a direct connection when
     * the port refuses it: the port of a probe that was bound and released, which nothing listens on, is the quiet path.
     */
    private RemoteGrpcDatabase database() {
      final int refusingHttpPort;
      try (final ServerSocket probe = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
        refusingHttpPort = probe.getLocalPort();
      } catch (final IOException e) {
        throw new UncheckedIOException(e);
      }
      return new RemoteGrpcDatabase(this, HOST, 0, refusingHttpPort, "db11714", "root", "root");
    }

    /** What a {@code close()} / {@code start()} cycle does: a new channel generation, to the server now behind the address. */
    private void reconnect(final boolean newServerAnnounces) {
      announces = newServerAnnounces;
      generation++;
    }

    /** The next call blocks after its request was sent, until {@link #release} opens, and only then answers. */
    private void holdNextAnswer() {
      sent = new CountDownLatch(1);
      release = new CountDownLatch(1);
      holding = true;
    }

    private GrpcValue sentVector() {
      return lastRequest.getParametersMap().get("v");
    }

    @Override
    public long channelGeneration() {
      return generation;
    }

    /** A channel to the server behind the address when it was built, which announces or not for good. */
    @Override
    public Channel channel() {
      final boolean serverAnnounces = announces;
      return new Channel() {
        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(final MethodDescriptor<ReqT, RespT> method, final CallOptions callOptions) {
          // The blocking stub waits on the executor it put in the call options: the answer has to go through it
          final Executor executor = callOptions.getExecutor() != null ? callOptions.getExecutor() : Runnable::run;
          return new ClientCall<>() {
            private Listener<RespT> listener;

            @Override
            public void start(final Listener<RespT> responseListener, final Metadata headers) {
              listener = responseListener;
            }

            @Override
            public void sendMessage(final ReqT message) {
              if (message instanceof ExecuteCommandRequest request)
                lastRequest = request;
            }

            @Override
            public void halfClose() {
              if (holding) {
                holding = false;
                sent.countDown();
                try {
                  release.await(10, TimeUnit.SECONDS);
                } catch (final InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
              }
              final Metadata answer = serverAnnounces ? announcement() : new Metadata();
              final RespT response = method.parseResponse(new ByteArrayInputStream(new byte[0]));
              executor.execute(() -> {
                listener.onHeaders(answer);
                listener.onMessage(response);
                listener.onClose(Status.OK, new Metadata());
              });
            }

            @Override
            public void request(final int numMessages) {
            }

            @Override
            public void cancel(final String message, final Throwable cause) {
            }
          };
        }

        @Override
        public String authority() {
          return "reconnecting";
        }
      };
    }
  }
}
