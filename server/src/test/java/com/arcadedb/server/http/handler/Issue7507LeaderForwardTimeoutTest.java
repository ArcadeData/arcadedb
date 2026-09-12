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
package com.arcadedb.server.http.handler;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7507: {@link LeaderCommandForwarder} dialled the leader with a bare
 * {@code HttpClient.newHttpClient()} and built the request without {@code .timeout(...)}, so a leader that
 * accepted the connection and then never answered held the calling Undertow worker thread until the OS tore
 * the socket down.
 * <p>
 * The invariant these tests pin: every forward carries a finite response deadline and the client carries a
 * finite connect timeout, and a blown deadline is answered 504 rather than parking the caller.
 */
class Issue7507LeaderForwardTimeoutTest {

  /**
   * The tripwire between "the deadline fired" and "the call is unbounded". The deadline under test is 1 s, so
   * anything under this bound proves the former; a stalled leader without the fix sits here until the OS gives
   * up on the socket, which is minutes.
   */
  private static final long GAVE_UP_BOUND_MS = 30_000L;

  private static ContextConfiguration configuration(final long readTimeoutMs, final long longCommandTimeoutMs,
      final long connectTimeoutMs) {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.HA_PROXY_READ_TIMEOUT, readTimeoutMs);
    cfg.setValue(GlobalConfiguration.HA_PROXY_LONG_COMMAND_TIMEOUT, longCommandTimeoutMs);
    cfg.setValue(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT, connectTimeoutMs);
    return cfg;
  }

  /**
   * The reported failure, end to end over a real socket: a leader that accepts the connection and never
   * answers. Before the fix this parked the caller; now the deadline fires and the follower answers 504.
   */
  @Test
  void aLeaderThatAcceptsAndNeverAnswersIsGivenUpOnAndAnswered504() throws Exception {
    try (final StalledLeader leader = new StalledLeader()) {
      final LeaderCommandForwarder.Transport transport =
          new LeaderCommandForwarder.Transport(configuration(1_000L, 60_000L, 5_000L));

      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      final ExecutionResponse response = transport.send(transport.client(),
          transport.newRequest(URI.create("http://" + leader.address() + "/api/v1/server"), "POST", "{}", false).build(),
          leader.address(), false);
      watch.assertGaveUpWithin(GAVE_UP_BOUND_MS,
          "a 1s forward deadline from the unbounded wait a leader that never answers used to produce");

      assertThat(response.getCode()).isEqualTo(504);
      final JSONObject body = new JSONObject(response.getResponse());
      assertThat(body.getString("error")).contains(leader.address());
      assertThat(body.getString("error")).contains(GlobalConfiguration.HA_PROXY_READ_TIMEOUT.getKey());
      assertThat(leader.acceptedConnections()).isGreaterThanOrEqualTo(1);
      assertThat(leader.firstConnectionClosedByClientWithin(10_000L))
          .as("giving up must tear the connection down, not just release this thread: a leader that wedges "
              + "repeatedly would otherwise trade a parked worker for a leaked socket")
          .isTrue();
    }
  }

  /**
   * The half of the wait a {@code HttpRequest.timeout} does not cover: the leader answers the headers - status
   * line, {@code Content-Length}, blank line - and then stops writing part-way through the body. The JDK's
   * request timeout has already been satisfied by the headers at that point, so {@code HttpClient.send} would
   * block on the stalled body indefinitely; the forwarder bounds the whole exchange instead.
   */
  @Test
  void aLeaderThatAnswersTheHeadersAndThenStallsMidBodyIsAlsoGivenUpOn() throws Exception {
    try (final StalledLeader leader = new StalledLeader(true)) {
      final LeaderCommandForwarder.Transport transport =
          new LeaderCommandForwarder.Transport(configuration(1_000L, 60_000L, 5_000L));

      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      final ExecutionResponse response = transport.send(transport.client(),
          transport.newRequest(URI.create("http://" + leader.address() + "/api/v1/server"), "POST", "{}", false).build(),
          leader.address(), false);
      watch.assertGaveUpWithin(GAVE_UP_BOUND_MS,
          "a 1s forward deadline from the unbounded wait a body that stalls after the headers used to produce");

      assertThat(response.getCode()).isEqualTo(504);
      assertThat(new JSONObject(response.getResponse()).getString("error")).contains(leader.address());
    }
  }

  /**
   * Every request the forwarder can build carries a deadline: {@code newRequest} is the only way
   * {@code forwardIfReplica} constructs one, so no call site can opt out of it.
   */
  @Test
  void everyForwardedRequestCarriesTheDefaultDeadline() {
    final LeaderCommandForwarder.Transport transport =
        new LeaderCommandForwarder.Transport(configuration(7_000L, 60_000L, 5_000L));

    assertThat(transport.newRequest(URI.create("http://leader:2480/api/v1/server/users?name=bob"), "DELETE", null, false)
        .build().timeout()).contains(Duration.ofMillis(7_000L));
    assertThat(transport.newRequest(URI.create("http://leader:2480/api/v1/server/users"), "POST", "{\"name\":\"bob\"}", false)
        .build().timeout()).contains(Duration.ofMillis(7_000L));
    assertThat(transport.newRequest(URI.create("http://leader:2480/api/v1/server/users"), "PUT", "{\"name\":\"bob\"}", false)
        .build().timeout()).contains(Duration.ofMillis(7_000L));
  }

  /**
   * {@code restore backup}, {@code restore database} and {@code import database} legitimately run for minutes,
   * so they get their own, larger deadline instead of being aborted by the 30 s one.
   */
  @Test
  void longRunningCommandsGetTheLongerDeadline() {
    final LeaderCommandForwarder.Transport transport =
        new LeaderCommandForwarder.Transport(configuration(7_000L, 600_000L, 5_000L));

    assertThat(transport.newRequest(URI.create("http://leader:2480/api/v1/server"), "POST", "{}", true)
        .build().timeout()).contains(Duration.ofMillis(600_000L));
    assertThat(transport.responseTimeout(true)).isGreaterThan(transport.responseTimeout(false));
  }

  /**
   * Which commands claim the longer deadline. The classification lives in {@code PostServerCommandHandler}
   * because that is where the command grammar lives, and it has to match the set of commands that forward at
   * all, otherwise a long restore silently gets the 30 s budget.
   */
  @Test
  void onlyRestoreAndImportAreClassifiedAsLongRunning() {
    assertThat(PostServerCommandHandler.isLongRunningForwardedCommand("restore backup mybackup.zip")).isTrue();
    assertThat(PostServerCommandHandler.isLongRunningForwardedCommand("restore database mydb")).isTrue();
    assertThat(PostServerCommandHandler.isLongRunningForwardedCommand("import database file:///data.csv")).isTrue();

    assertThat(PostServerCommandHandler.isLongRunningForwardedCommand("create database mydb")).isFalse();
    assertThat(PostServerCommandHandler.isLongRunningForwardedCommand("drop database mydb")).isFalse();
    assertThat(PostServerCommandHandler.isLongRunningForwardedCommand("create user { \"name\": \"bob\" }")).isFalse();
    assertThat(PostServerCommandHandler.isLongRunningForwardedCommand("drop user bob")).isFalse();
  }

  /**
   * The connect timeout bounds the other half of the failure: a leader whose host black-holes the SYN. It is
   * immutable once the client is built, so it is asserted on the client rather than over a socket - an address
   * that reliably black-holes a connect is not something a unit test can conjure portably.
   */
  @Test
  void theClientCarriesTheConfiguredConnectTimeout() {
    final LeaderCommandForwarder.Transport transport =
        new LeaderCommandForwarder.Transport(configuration(7_000L, 60_000L, 2_500L));

    assertThat(transport.client().connectTimeout()).contains(Duration.ofMillis(2_500L));
  }

  /**
   * A leader this node cannot even reach is a different failure from one that will not answer, and it points at
   * a different setting. It gets its own 504, saying the command certainly did not run - which, unlike the
   * response-deadline case, is something this node can promise.
   */
  @Test
  void aLeaderThatCannotBeConnectedToIsAnsweredWithItsOwnGatewayTimeout() {
    final LeaderCommandForwarder.Transport transport =
        new LeaderCommandForwarder.Transport(configuration(7_000L, 60_000L, 2_500L));

    final ExecutionResponse response = transport.couldNotConnect(transport.client(), "leader.example:2480");

    assertThat(response.getCode()).isEqualTo(504);
    final String error = new JSONObject(response.getResponse()).getString("error");
    assertThat(error).contains("leader.example:2480");
    assertThat(error).contains(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT.getKey());
    assertThat(error).contains("2500 ms");
    assertThat(error).doesNotContain(GlobalConfiguration.HA_PROXY_READ_TIMEOUT.getKey());
  }

  /**
   * The deadlines are re-read per forward, so {@code SET SERVER SETTING arcadedb.ha.proxyReadTimeout} takes
   * effect without a restart. (The connect timeout cannot: an {@link java.net.http.HttpClient}'s is fixed at
   * build time.)
   */
  @Test
  void responseDeadlineIsReReadOnEveryForward() {
    final ContextConfiguration cfg = configuration(7_000L, 60_000L, 5_000L);
    final LeaderCommandForwarder.Transport transport = new LeaderCommandForwarder.Transport(cfg);

    assertThat(transport.responseTimeout(false)).isEqualTo(Duration.ofMillis(7_000L));
    cfg.setValue(GlobalConfiguration.HA_PROXY_READ_TIMEOUT, 11_000L);
    assertThat(transport.responseTimeout(false)).isEqualTo(Duration.ofMillis(11_000L));
  }

  /**
   * Zero and negative are not a back door to the unbounded behaviour this issue removed: they clamp to the
   * smallest deadline the JDK accepts rather than disabling the bound.
   */
  @Test
  void zeroOrNegativeTimeoutClampsInsteadOfDisablingTheBound() {
    final LeaderCommandForwarder.Transport transport =
        new LeaderCommandForwarder.Transport(configuration(0L, -1L, 0L));

    assertThat(transport.responseTimeout(false)).isPositive();
    assertThat(transport.responseTimeout(true)).isPositive();
    assertThat(transport.client().connectTimeout()).isPresent();
    assertThat(transport.client().connectTimeout().get()).isPositive();
    // and a request can actually be built from it - Duration.ZERO would throw
    assertThat(transport.newRequest(URI.create("http://leader:2480/api/v1/server"), "POST", "{}", false)
        .build().timeout()).isPresent();
  }

  /**
   * A server socket standing in for a wedged leader. In the default mode it accepts the connection and answers
   * nothing at all; with {@code answerHeadersThenStall} it writes a complete, well-formed response head that
   * promises a hundred bytes and then delivers five of them, which is the case an {@code HttpRequest.timeout}
   * alone does not catch.
   */
  private static final class StalledLeader implements AutoCloseable {
    private final ServerSocket   serverSocket;
    private final Thread         acceptor;
    private final List<Socket>   accepted = new ArrayList<>();
    private final CountDownLatch started  = new CountDownLatch(1);

    StalledLeader() throws IOException, InterruptedException {
      this(false);
    }

    StalledLeader(final boolean answerHeadersThenStall) throws IOException, InterruptedException {
      serverSocket = new ServerSocket(0, 16, InetAddress.getLoopbackAddress());
      acceptor = new Thread(() -> {
        started.countDown();
        while (!serverSocket.isClosed()) {
          try {
            final Socket socket = serverSocket.accept();
            synchronized (accepted) {
              accepted.add(socket);
            }
            if (answerHeadersThenStall)
              answerHeadersThenStall(socket);
          } catch (final IOException e) {
            return; // socket closed, we are done
          }
        }
      }, "issue7507-stalled-leader");
      acceptor.setDaemon(true);
      acceptor.start();
      started.await(10, TimeUnit.SECONDS);
    }

    private static void answerHeadersThenStall(final Socket socket) throws IOException {
      final byte[] request = new byte[8192];
      socket.getInputStream().read(request);
      final OutputStream out = socket.getOutputStream();
      out.write(("HTTP/1.1 200 OK\r\n"
          + "Content-Type: application/json\r\n"
          + "Content-Length: 100\r\n"
          + "\r\n").getBytes(StandardCharsets.US_ASCII));
      out.write("{\"res\"".getBytes(StandardCharsets.US_ASCII));
      out.flush();
      // and never another byte: the socket stays open until close() tears it down
    }

    String address() {
      return serverSocket.getInetAddress().getHostAddress() + ":" + serverSocket.getLocalPort();
    }

    /**
     * Blocks until the client end of the first accepted connection is closed, or the bound expires. Reads from
     * this side of the socket: EOF means the peer closed it. Only valid in the default mode, where nothing else
     * touches the socket's input stream.
     */
    boolean firstConnectionClosedByClientWithin(final long boundMs) throws IOException {
      final Socket socket;
      synchronized (accepted) {
        if (accepted.isEmpty())
          return false;
        socket = accepted.getFirst();
      }
      socket.setSoTimeout((int) boundMs);
      final byte[] drain = new byte[4096];
      try {
        int read;
        while ((read = socket.getInputStream().read(drain)) != -1)
          if (read == 0)
            break;
        return true;
      } catch (final SocketTimeoutException e) {
        return false;
      } catch (final SocketException e) {
        // "connection reset" is the peer tearing it down just as abruptly, which is the same answer
        return true;
      }
    }

    int acceptedConnections() {
      synchronized (accepted) {
        return accepted.size();
      }
    }

    @Override
    public void close() throws IOException {
      serverSocket.close();
      synchronized (accepted) {
        for (final Socket socket : accepted)
          try {
            socket.close();
          } catch (final IOException ignored) {
            // best effort
          }
      }
    }
  }
}
