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
package com.arcadedb.remote;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.network.BoundedHttpExchange;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.timeseries.TimeSeriesPoint;
import com.arcadedb.remote.timeseries.TimeSeriesQuery;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Regression test for issue #8473: the Java remote client read a server's answer with {@code HttpClient.send} bounded
 * only by the request timeout ({@code HttpRequest.Builder.timeout}), and what that covers depends on the JDK:
 * <ul>
 * <li>on JDK 21-25 it stops at the response HEADERS, so a server that answers {@code 200} with a
 * {@code Content-Length} and then stalls inside its body parks the application thread with no bound at all;</li>
 * <li>on JDK 26+ it covers the whole body, so on a STREAMED answer ({@code queryStream}, a batch load with a progress
 * listener) it caps the total length of a stream that is working.</li>
 * </ul>
 * Each buffered entry point is driven against a server that stalls inside its body, and each streamed one against a
 * server that stalls mid-stream, one that never sends its headers, and one that streams for several budgets without
 * ever going silent. Without the fix the first kind hangs on JDK 21-25 and the last fails on JDK 26+.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue8473RemoteStalledAnswerTest {

  private static final int    BUDGET_MS        = 1_000;
  /** The tripwire between "the bound fired" and "the read is unbounded" (forever, without the fix). */
  private static final long   GAVE_UP_BOUND_MS = 15_000L;
  /** A hang detector, not a latency bound: how long the test waits before calling the read unbounded. */
  private static final long   HANG_DETECT_MS   = 60_000L;
  /** Headers promising 100 bytes, then five of them, then silence. */
  private static final String STALLED_BODY     =
      "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{\"res";
  private static final String STREAM_HEADERS   =
      "HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n";

  private final List<AutoCloseable> toClose = new ArrayList<>();

  @AfterEach
  void closeAll() throws Exception {
    for (int i = toClose.size() - 1; i >= 0; i--)
      toClose.get(i).close();
  }

  // ------------------------------------------------------------------------------------------------------------
  // The shared helper
  // ------------------------------------------------------------------------------------------------------------

  @Test
  void sendGivesUpOnAServerThatStallsInsideItsBody() throws Exception {
    final ScriptedServer server = server(out -> write(out, STALLED_BODY));
    { // JDK17: HttpClient is AutoCloseable only since Java 21
      final HttpClient client = HttpClient.newHttpClient();
      // No request timeout at all: the bound under test is the helper's own, on every JDK.
      final HttpRequest request = HttpRequest.newBuilder(URI.create("http://" + server.address() + "/")).GET().build();

      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      final Throwable thrown = failureOf(
          () -> BoundedHttpExchange.send(client, request, HttpResponse.BodyHandlers.ofString(), BUDGET_MS, "custom"),
          server);
      watch.assertGaveUpWithin(GAVE_UP_BOUND_MS, "a 1s deadline over the whole exchange from an unbounded body read");

      assertThat(thrown).isInstanceOf(HttpTimeoutException.class).hasMessage("custom");
      assertThat(server.closedByClient.await(GAVE_UP_BOUND_MS, TimeUnit.MILLISECONDS))
          .as("the exchange is cancelled, not only abandoned by the calling thread").isTrue();
    }
  }

  @Test
  void silenceBoundedReadsAStreamLongerThanTheBudgetThatIsNeverSilent() throws Exception {
    final byte[] data = "0123456789".getBytes(StandardCharsets.UTF_8);
    final InputStream slow = new InputStream() {
      private int next;

      @Override
      public int read() throws IOException {
        if (next == data.length)
          return -1;
        sleep(BUDGET_MS / 4);
        return data[next++];
      }

      /** One byte per call, as a network stream hands over whatever has arrived rather than waiting to fill. */
      @Override
      public int read(final byte[] b, final int off, final int len) throws IOException {
        if (len == 0)
          return 0;
        final int value = read();
        if (value < 0)
          return -1;
        b[off] = (byte) value;
        return 1;
      }
    };
    // Ten reads of a quarter budget each: two and a half budgets in total, never one of silence.
    try (final InputStream in = BoundedHttpExchange.silenceBounded(slow, BUDGET_MS)) {
      assertThat(new String(in.readAllBytes(), StandardCharsets.UTF_8)).isEqualTo("0123456789");
    }
  }

  @Test
  void silenceBoundedPassesThroughAQuietlyFinishedStream() throws Exception {
    try (final InputStream in = BoundedHttpExchange.silenceBounded(new ByteArrayInputStream(new byte[] { 1, 2, 3 }),
        BUDGET_MS)) {
      assertThat(in.readAllBytes()).containsExactly(1, 2, 3);
    }
  }

  // ------------------------------------------------------------------------------------------------------------
  // RemoteDatabase and RemoteServer: buffered reads
  // ------------------------------------------------------------------------------------------------------------

  @Test
  void dropIsBounded() throws Exception {
    assertBuffered((db, srv) -> db.drop());
  }

  @Test
  void beginIsBounded() throws Exception {
    assertBuffered((db, srv) -> db.begin());
  }

  @Test
  void commitIsBounded() throws Exception {
    assertBuffered((db, srv) -> {
      db.setSessionId("AS-1");
      db.commit();
    });
  }

  @Test
  void rollbackIsBounded() throws Exception {
    assertBuffered((db, srv) -> {
      db.setSessionId("AS-1");
      db.rollback();
    });
  }

  @Test
  void getProgressIsBounded() throws Exception {
    assertBuffered((db, srv) -> db.getProgress());
  }

  @Test
  void timeSeriesWriteIsBounded() throws Exception {
    assertBuffered((db, srv) -> db.timeSeriesWrite(List.of(TimeSeriesPoint.of("weather", 1_000L, Map.of("t", 1.0)))));
  }

  @Test
  void timeSeriesLatestIsBounded() throws Exception {
    assertBuffered((db, srv) -> db.timeSeriesLatest("weather"));
  }

  @Test
  void timeSeriesQueryIsBounded() throws Exception {
    assertBuffered((db, srv) -> db.timeSeriesQuery(new TimeSeriesQuery("weather")));
  }

  @Test
  void bufferedBatchIsBounded() throws Exception {
    assertBuffered((db, srv) -> db.sendBatch("{\"@type\":\"vertex\",\"@class\":\"V\"}\n", Map.of()));
  }

  @Test
  void serverDropIsBounded() throws Exception {
    assertBuffered((db, srv) -> srv.drop("db"));
  }

  @Test
  void serverCreateUserIsBounded() throws Exception {
    assertBuffered((db, srv) -> srv.createUser("user", "password1234", Map.of("db", "admin")));
  }

  @Test
  void serverDropUserIsBounded() throws Exception {
    assertBuffered((db, srv) -> srv.dropUser("user"));
  }

  // ------------------------------------------------------------------------------------------------------------
  // RemoteDatabase: streamed reads
  // ------------------------------------------------------------------------------------------------------------

  @Test
  void aStreamedQueryWhoseServerStallsMidStreamIsBounded() throws Exception {
    final ScriptedServer server = server(out -> {
      write(out, STREAM_HEADERS);
      writeChunk(out, "{\"record\":{\"n\":0}}\n");
    });
    final TestableDatabase db = database(server);

    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    final Throwable thrown = failureOf(() -> {
      try (final ResultSet rs = db.queryStream("sql", "select from V", Map.of())) {
        while (rs.hasNext())
          rs.next();
      }
      return null;
    }, server);
    watch.assertGaveUpWithin(GAVE_UP_BOUND_MS, "a 1s bound on the server's silence from an unbounded body read");

    assertThat(causes(thrown)).hasAtLeastOneElementOfType(HttpTimeoutException.class);
  }

  /** The request timeout is gone from the streamed request, so the header wait has to be bounded some other way. */
  @Test
  void aStreamedQueryWhoseServerNeverAnswersIsBounded() throws Exception {
    final ScriptedServer server = server(out -> {
      // headers never come
    });
    final TestableDatabase db = database(server);

    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    final Throwable thrown = failureOf(() -> db.queryStream("sql", "select from V", Map.of()), server);
    watch.assertGaveUpWithin(GAVE_UP_BOUND_MS, "a 1s bound on the wait for the response headers");

    assertThat(causes(thrown)).hasAtLeastOneElementOfType(HttpTimeoutException.class);
  }

  /** The JDK 26+ half: a stream that runs for several budgets but is never silent for one is read whole. */
  @Test
  void aStreamedQueryLongerThanTheBudgetButNeverSilentIsReadWhole() throws Exception {
    final int rows = 10;
    final ScriptedServer server = server(out -> {
      write(out, STREAM_HEADERS);
      for (int i = 0; i < rows; i++) {
        writeChunk(out, "{\"record\":{\"n\":" + i + "}}\n");
        sleep(BUDGET_MS / 4);
      }
      writeChunk(out, "{\"stats\":{}}\n");
      write(out, "0\r\n\r\n");
    });
    final TestableDatabase db = database(server);

    final int read = callWithin(() -> {
      int count = 0;
      try (final ResultSet rs = db.queryStream("sql", "select from V", Map.of())) {
        while (rs.hasNext()) {
          assertThat(((Number) rs.next().getProperty("n")).intValue()).isEqualTo(count);
          count++;
        }
      }
      return count;
    }, server);

    assertThat(read).isEqualTo(rows);
  }

  @Test
  void aStreamedBatchWhoseServerStallsMidStreamIsBounded() throws Exception {
    final ScriptedServer server = server(out -> {
      write(out, STREAM_HEADERS);
      writeChunk(out, "{\"progress\":{\"verticesCreated\":1}}\n");
    });
    final TestableDatabase db = database(server);
    final List<Object> progress = new CopyOnWriteArrayList<>();

    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    final Throwable thrown = failureOf(
        () -> db.sendBatch("{\"@type\":\"vertex\",\"@class\":\"V\"}\n", Map.of(), progress::add), server);
    watch.assertGaveUpWithin(GAVE_UP_BOUND_MS, "a 1s bound on the server's silence from an unbounded body read");

    assertThat(causes(thrown)).hasAtLeastOneElementOfType(HttpTimeoutException.class);
    assertThat(progress).as("the progress line before the stall is still delivered").hasSize(1);
  }

  @Test
  void aStreamedBatchLongerThanTheBudgetButNeverSilentIsReadWhole() throws Exception {
    final int lines = 10;
    final ScriptedServer server = server(out -> {
      write(out, STREAM_HEADERS);
      for (int i = 0; i < lines; i++) {
        writeChunk(out, "{\"progress\":{\"verticesCreated\":" + i + "}}\n");
        sleep(BUDGET_MS / 4);
      }
      writeChunk(out, "{\"summary\":{\"verticesCreated\":" + lines + "}}\n");
      write(out, "0\r\n\r\n");
    });
    final TestableDatabase db = database(server);
    final List<Object> progress = new CopyOnWriteArrayList<>();

    final Object summary = callWithin(
        () -> db.sendBatch("{\"@type\":\"vertex\",\"@class\":\"V\"}\n", Map.of(), progress::add), server);

    assertThat(summary.toString()).contains("\"verticesCreated\":" + lines);
    assertThat(progress).hasSize(lines);
  }

  // ------------------------------------------------------------------------------------------------------------
  // The open transaction still travels with the requests built without the request timeout
  // ------------------------------------------------------------------------------------------------------------

  /**
   * The streamed requests are built through the overload that leaves the request timeout off. The session header that
   * binds a request to the caller's open transaction has to be added there too, or a streamed query or batch load after
   * {@code begin()} silently runs outside the transaction (found in review of this fix).
   */
  @Test
  void aStreamedQueryInsideATransactionCarriesTheSession() throws Exception {
    final ScriptedServer server = server(out -> {
      write(out, STREAM_HEADERS);
      writeChunk(out, "{\"stats\":{}}\n");
      write(out, "0\r\n\r\n");
    });
    final TestableDatabase db = database(server);
    db.setSessionId("AS-8473");

    callWithin(() -> {
      try (final ResultSet rs = db.queryStream("sql", "select from V", Map.of())) {
        while (rs.hasNext())
          rs.next();
      }
      try (final ResultSet rs = db.commandStream("sql", "select from V", Map.of())) {
        while (rs.hasNext())
          rs.next();
      }
      return null;
    }, server);

    assertThat(server.requests).hasSize(2);
    for (final String request : server.requests)
      assertThat(request.toLowerCase()).contains(RemoteDatabase.ARCADEDB_SESSION_ID.toLowerCase() + ": as-8473");
  }

  @Test
  void aBatchLoadInsideATransactionCarriesTheSession() throws Exception {
    final ScriptedServer server = server(out -> {
      write(out, STREAM_HEADERS);
      writeChunk(out, "{\"summary\":{\"verticesCreated\":1}}\n");
      write(out, "0\r\n\r\n");
    });
    final TestableDatabase db = database(server);
    db.setSessionId("AS-8473");

    callWithin(() -> db.sendBatch("{\"@type\":\"vertex\",\"@class\":\"V\"}\n", Map.of(), progress -> {
    }), server);
    callWithin(() -> db.sendBatch("{\"@type\":\"vertex\",\"@class\":\"V\"}\n", Map.of()), server);

    assertThat(server.requests).hasSize(2);
    for (final String request : server.requests)
      assertThat(request.toLowerCase()).contains(RemoteDatabase.ARCADEDB_SESSION_ID.toLowerCase() + ": as-8473");
  }

  // ------------------------------------------------------------------------------------------------------------
  // Fixtures
  // ------------------------------------------------------------------------------------------------------------

  @FunctionalInterface
  private interface Script {
    void answer(OutputStream out) throws Exception;
  }

  @FunctionalInterface
  private interface EntryPoint {
    void call(TestableDatabase database, TestableServer server) throws Exception;
  }

  /**
   * Drives one buffered entry point against a server that stalls inside its body. The client's watchdog budget is
   * shortened by overriding {@code sendWithWatchdog(HttpRequest)}: the override is reached only when the entry point
   * sends through the watchdog, so a site still calling {@code httpClient.send} directly hangs here, which is the bug.
   */
  private void assertBuffered(final EntryPoint entryPoint) throws Exception {
    final ScriptedServer server = server(out -> write(out, STALLED_BODY));
    final TestableDatabase db = database(server);
    final TestableServer srv = new TestableServer(server.port);
    toClose.add(srv::close);

    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    final Throwable thrown = failureOf(() -> {
      entryPoint.call(db, srv);
      return null;
    }, server);
    watch.assertGaveUpWithin(GAVE_UP_BOUND_MS, "a 1s watchdog over the whole exchange from an unbounded body read");

    assertThat(causes(thrown)).hasAtLeastOneElementOfType(HttpTimeoutException.class);
    assertThat(server.closedByClient.await(GAVE_UP_BOUND_MS, TimeUnit.MILLISECONDS))
        .as("the exchange is cancelled, not only abandoned by the calling thread").isTrue();
  }

  static final class TestableDatabase extends RemoteDatabase {
    TestableDatabase(final int port) {
      super("127.0.0.1", port, "db", "root", "test", new ContextConfiguration());
      setTimeout(BUDGET_MS);
    }

    @Override
    long streamSilenceMs() {
      return BUDGET_MS;
    }

    @Override
    void requestClusterConfiguration() {
      // No cluster: the scripted server answers only the request under test
    }

    @Override
    HttpResponse<String> sendWithWatchdog(final HttpRequest request) throws IOException, InterruptedException {
      return sendWithWatchdog(request, BUDGET_MS);
    }
  }

  static final class TestableServer extends RemoteServer {
    TestableServer(final int port) {
      super("127.0.0.1", port, "root", "test", new ContextConfiguration());
    }

    @Override
    void requestClusterConfiguration() {
      // No cluster: the scripted server answers only the request under test
    }

    @Override
    HttpResponse<String> sendWithWatchdog(final HttpRequest request) throws IOException, InterruptedException {
      return sendWithWatchdog(request, BUDGET_MS);
    }
  }

  private TestableDatabase database(final ScriptedServer server) {
    final TestableDatabase db = new TestableDatabase(server.port);
    toClose.add(() -> {
      if (db.isOpen())
        db.close();
    });
    return db;
  }

  private ScriptedServer server(final Script script) throws IOException {
    final ScriptedServer server = new ScriptedServer(script);
    toClose.add(server);
    return server;
  }

  /** Every cause in the chain, the throwable itself first. */
  private static List<Throwable> causes(final Throwable thrown) {
    final List<Throwable> chain = new ArrayList<>();
    for (Throwable t = thrown; t != null && !chain.contains(t); t = t.getCause())
      chain.add(t);
    return chain;
  }

  /** Runs {@code call} expecting it to fail, and fails the test instead of hanging if it never returns. */
  private static Throwable failureOf(final Callable<?> call, final ScriptedServer server) throws Exception {
    try {
      callWithin(call, server);
    } catch (final Throwable t) {
      return t;
    }
    return fail("expected the call to fail on the stalled server");
  }

  private static <T> T callWithin(final Callable<T> call, final ScriptedServer server) throws Exception {
    final FutureTask<T> task = new FutureTask<>(call);
    final Thread thread = new Thread(task, "issue8473-caller");
    thread.setDaemon(true);
    thread.start();
    try {
      return task.get(HANG_DETECT_MS, TimeUnit.MILLISECONDS);
    } catch (final TimeoutException e) {
      thread.interrupt();
      server.close();
      return fail("the call is unbounded: still blocked after " + HANG_DETECT_MS + " ms");
    } catch (final ExecutionException e) {
      if (e.getCause() instanceof Exception ex)
        throw ex;
      throw e;
    }
  }

  private static void write(final OutputStream out, final String text) throws IOException {
    out.write(text.getBytes(StandardCharsets.UTF_8));
    out.flush();
  }

  private static void writeChunk(final OutputStream out, final String text) throws IOException {
    final byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
    write(out, Integer.toHexString(bytes.length) + "\r\n");
    out.write(bytes);
    write(out, "\r\n");
  }

  private static void sleep(final long ms) {
    try {
      Thread.sleep(ms);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * A server that reads each request whole, answers it with a script, and then holds the connection open until the
   * client closes it - which it records, so a test can tell a cancelled exchange from an abandoned one.
   */
  private static final class ScriptedServer implements AutoCloseable {
    final         CountDownLatch  closedByClient = new CountDownLatch(1);
    final         int             port;
    private final ServerSocket    socket;
    final         List<String>    requests       = new CopyOnWriteArrayList<>();
    private final List<Socket>    accepted       = new CopyOnWriteArrayList<>();
    private final Script          script;

    ScriptedServer(final Script script) throws IOException {
      this.script = script;
      this.socket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
      this.port = socket.getLocalPort();
      final Thread acceptor = new Thread(this::acceptLoop, "issue8473-server");
      acceptor.setDaemon(true);
      acceptor.start();
    }

    String address() {
      return "127.0.0.1:" + port;
    }

    private void acceptLoop() {
      while (!socket.isClosed()) {
        try {
          final Socket connection = socket.accept();
          accepted.add(connection);
          final Thread handler = new Thread(() -> serve(connection), "issue8473-connection");
          handler.setDaemon(true);
          handler.start();
        } catch (final IOException e) {
          return;
        }
      }
    }

    private void serve(final Socket connection) {
      try {
        final InputStream in = connection.getInputStream();
        requests.add(readRequest(in));
        script.answer(connection.getOutputStream());
        // Hold the connection until the client closes it.
        while (in.read() >= 0) {
          // discard
        }
        closedByClient.countDown();
      } catch (final SocketException e) {
        closedByClient.countDown();
      } catch (final Exception ignored) {
        // The test asserts on the client side
      }
    }

    /**
     * Reads the request line, the headers and a {@code Content-Length} body, so the answer follows the request, and
     * returns the request line and the headers.
     */
    private static String readRequest(final InputStream in) throws IOException {
      final StringBuilder headers = new StringBuilder();
      while (!headers.toString().endsWith("\r\n\r\n")) {
        final int b = in.read();
        if (b < 0)
          return headers.toString();
        headers.append((char) b);
      }
      long length = 0;
      for (final String line : headers.toString().split("\r\n"))
        if (line.toLowerCase().startsWith("content-length:"))
          length = Long.parseLong(line.substring("content-length:".length()).trim());
      for (long i = 0; i < length; i++)
        if (in.read() < 0)
          break;
      return headers.toString();
    }

    @Override
    public void close() throws IOException {
      socket.close();
      for (final Socket s : accepted)
        s.close();
    }
  }
}
