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
package com.arcadedb.server.http;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins the behaviour of the shared {@link FakeLeader} fixture (issue #8691), so that the tests migrated onto it keep
 * exercising the leader they were written against: one that stays silent, one that drains and never answers, one that
 * drops every connection, and one that answers with a script.
 */
class FakeLeaderTest {

  private static final byte[] REQUEST = "POST /api/v1/batch/db HTTP/1.1\r\nHost: leader\r\nContent-Length: 2\r\n\r\n{}"
      .getBytes(StandardCharsets.US_ASCII);

  @Test
  void bindsTheLiteralIpv4LoopbackSoTheAddressIsAValidUriAuthority() throws IOException {
    try (final FakeLeader leader = FakeLeader.silent()) {
      // getLoopbackAddress() is ::1 under java.net.preferIPv6Addresses=true, and "::1:port" is not a URI authority
      assertThat(leader.address()).matches("127\\.0\\.0\\.1:\\d+");
      assertThat(leader.port()).isPositive();
    }
  }

  @Test
  void aSilentLeaderAcceptsButNeverAnswers() throws Exception {
    try (final FakeLeader leader = FakeLeader.silent(); final Socket client = connect(leader)) {
      client.getOutputStream().write(REQUEST);
      client.setSoTimeout(300);
      assertThatThrownBy(() -> client.getInputStream().read()).isInstanceOf(SocketTimeoutException.class);

      assertThat(leader.awaitFirstConnection(5, TimeUnit.SECONDS)).isTrue();
      assertThat(leader.acceptedConnections()).isEqualTo(1);
    }
  }

  @Test
  void aSilentLeaderSeesTheClientCloseItsConnection() throws Exception {
    try (final FakeLeader leader = FakeLeader.silent()) {
      final Socket client = connect(leader);
      assertThat(leader.awaitFirstConnection(5, TimeUnit.SECONDS)).isTrue();
      assertThat(leader.firstConnectionClosedByClientWithin(200L)).as("the client has not closed yet").isFalse();

      client.close();
      assertThat(leader.firstConnectionClosedByClientWithin(5_000L)).isTrue();
    }
  }

  @Test
  void aDrainingLeaderReadsTheRequestAndRecordsTheClientClosing() throws Exception {
    try (final FakeLeader leader = FakeLeader.draining()) {
      final Socket client = connect(leader);
      client.getOutputStream().write(REQUEST);
      client.setSoTimeout(300);
      assertThatThrownBy(() -> client.getInputStream().read()).isInstanceOf(SocketTimeoutException.class);
      assertThat(leader.awaitConnectionClosedByClient(200, TimeUnit.MILLISECONDS)).isFalse();

      client.close();
      assertThat(leader.awaitConnectionClosedByClient(5, TimeUnit.SECONDS)).isTrue();
    }
  }

  @Test
  void aDroppingLeaderClosesEveryConnectionItAccepts() throws Exception {
    try (final FakeLeader leader = FakeLeader.dropping()) {
      for (int i = 0; i < 2; i++)
        try (final Socket client = connect(leader)) {
          client.setSoTimeout(5_000);
          assertThat(readOrReset(client.getInputStream())).as("connection %d is closed by the leader", i).isEqualTo(-1);
        }
      assertThat(leader.acceptedConnections()).isEqualTo(2);
    }
  }

  @Test
  void aScriptedLeaderAnswersAfterTheRequestHeadersAndThenWaitsForTheClient() throws Exception {
    try (final FakeLeader leader = FakeLeader.scripted(out -> out.write("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"
        .getBytes(StandardCharsets.US_ASCII)))) {
      final Socket client = connect(leader);
      client.getOutputStream().write(REQUEST);
      assertThat(leader.awaitRequestReceived(5, TimeUnit.SECONDS)).isTrue();
      assertThat(leader.awaitAnswered(5, TimeUnit.SECONDS)).isTrue();

      client.setSoTimeout(5_000);
      final byte[] answer = client.getInputStream().readNBytes(40);
      assertThat(new String(answer, StandardCharsets.US_ASCII)).startsWith("HTTP/1.1 200 OK").endsWith("ok");
      assertThat(leader.awaitConnectionClosedByClient(200, TimeUnit.MILLISECONDS)).isFalse();

      client.close();
      assertThat(leader.awaitConnectionClosedByClient(5, TimeUnit.SECONDS)).isTrue();
    }
  }

  @Test
  void aScriptedLeaderDoesNotAnswerARequestWhoseHeadersNeverEnd() throws Exception {
    try (final FakeLeader leader = FakeLeader.scripted(out -> out.write("unexpected".getBytes(StandardCharsets.US_ASCII)));
        final Socket client = connect(leader)) {
      client.getOutputStream().write("POST / HTTP/1.1\r\nHost: leader\r\n".getBytes(StandardCharsets.US_ASCII));
      assertThat(leader.awaitRequestReceived(300, TimeUnit.MILLISECONDS)).isFalse();
      assertThat(leader.awaitAnswered(10, TimeUnit.MILLISECONDS)).isFalse();
    }
  }

  @Test
  void closingTheLeaderClosesTheConnectionsItAccepted() throws Exception {
    final FakeLeader leader = FakeLeader.silent();
    try (final Socket client = connect(leader)) {
      assertThat(leader.awaitFirstConnection(5, TimeUnit.SECONDS)).isTrue();
      leader.close();

      // A follower that never gave up is released by the leader going away, rather than holding its worker forever
      client.setSoTimeout(5_000);
      assertThat(readOrReset(client.getInputStream())).isEqualTo(-1);
    }
  }

  @Test
  void teardownIsNotMistakenForTheClientClosingTheConnection() throws Exception {
    // Scripted rather than draining so the test can wait until the reader is parked in the drain loop: closing the
    // leader before the reader starts would pass without exercising the read that teardown makes throw
    final FakeLeader leader = FakeLeader.scripted(out -> out.write("HTTP/1.1 200 OK\r\n\r\n".getBytes(StandardCharsets.US_ASCII)));
    try (final Socket client = connect(leader)) {
      client.getOutputStream().write(REQUEST);
      assertThat(leader.awaitAnswered(5, TimeUnit.SECONDS)).isTrue();
      leader.close();
      assertThat(leader.awaitConnectionClosedByClient(300, TimeUnit.MILLISECONDS)).isFalse();
    }
  }

  @Test
  void aWaitThatCanNeverFireForTheModeIsRefused() throws Exception {
    try (final FakeLeader leader = FakeLeader.draining()) {
      assertThatThrownBy(() -> leader.firstConnectionClosedByClientWithin(10L)).isInstanceOf(IllegalStateException.class);
      assertThatThrownBy(() -> leader.awaitAnswered(10, TimeUnit.MILLISECONDS)).isInstanceOf(IllegalStateException.class);
      assertThatThrownBy(() -> leader.awaitRequestReceived(10, TimeUnit.MILLISECONDS)).isInstanceOf(IllegalStateException.class);
    }
    try (final FakeLeader leader = FakeLeader.dropping()) {
      assertThatThrownBy(() -> leader.awaitConnectionClosedByClient(10, TimeUnit.MILLISECONDS))
          .isInstanceOf(IllegalStateException.class);
    }
  }

  @Test
  void aScriptThatThrowsIsReportedRatherThanReadAsATimeout() throws Exception {
    try (final FakeLeader leader = FakeLeader.scripted(out -> {
      throw new IllegalArgumentException("broken script");
    }); final Socket client = connect(leader)) {
      client.getOutputStream().write(REQUEST);
      assertThat(leader.awaitRequestReceived(5, TimeUnit.SECONDS)).isTrue();
      assertThatThrownBy(() -> leader.awaitAnswered(5, TimeUnit.SECONDS)).isInstanceOf(IllegalStateException.class)
          .hasRootCauseMessage("broken script");
    }
  }

  @Test
  void aSilentLeaderDoesNotReportItsOwnTeardownAsTheClientClosing() throws Exception {
    final FakeLeader leader = FakeLeader.silent();
    try (final Socket ignored = connect(leader)) {
      assertThat(leader.awaitFirstConnection(5, TimeUnit.SECONDS)).isTrue();
      // The caller is (most likely) parked in the read when teardown closes the socket under it; if it has not got
      // there yet the answer must be the same
      final CompletableFuture<Boolean> closedByClient = CompletableFuture.supplyAsync(() -> {
        try {
          return leader.firstConnectionClosedByClientWithin(30_000L);
        } catch (final IOException e) {
          throw new UncheckedIOException(e);
        }
      });
      Thread.sleep(200);
      leader.close();
      assertThat(closedByClient.get(30, TimeUnit.SECONDS)).isFalse();
      assertThat(leader.firstConnectionClosedByClientWithin(10L)).isFalse();
    }
  }

  private static Socket connect(final FakeLeader leader) throws IOException {
    return new Socket("127.0.0.1", leader.port());
  }

  /** A reset is the peer closing the connection just as abruptly as a FIN, so both read as end of stream. */
  private static int readOrReset(final InputStream in) throws IOException {
    try {
      return in.read();
    } catch (final SocketException e) {
      return -1;
    }
  }
}
