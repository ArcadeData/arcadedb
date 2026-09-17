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
package com.arcadedb.integration.importer;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.StallAwareStopwatch;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7500, the diagnostic half, end to end through the importer.
 * <p>
 * Before #7494 a remote source that went quiet mid-stream ended the content sniff with a WRONG answer - a partial
 * first line read as the whole of it. #7494 made the sniff read rather than guess, which is right, and what it
 * exposed is that nothing must WAIT FOREVER either. The deadline is the connection's, and it now comes from
 * {@link GlobalConfiguration#NETWORK_REMOTE_FETCH_READ_TIMEOUT}.
 * <p>
 * What reached the operator when it fired was {@code "Error on parsing source 'null'"}: {@code Importer.source} is
 * assigned only once the sniff has SUCCEEDED, so a failure while reading the source named it {@code null}, and the
 * cause's message - the only part saying what went wrong - appeared nowhere in the text. That is the same class of
 * unhelpful error #7346 and #7461 were about, and this pins the answer: the URL, the wait and the setting.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7500StalledRemoteImportIsReportedTest {

  private static final int    TIMEOUT_MS    = 300;
  private static final String DATABASE_PATH = "target/databases/test-import-7500-stalled";

  private HttpServer     origin;
  private String         url;
  private CountDownLatch release;
  private AtomicBoolean  stallAfterSniff;
  private Object         previousTimeout;
  private Object         previousBlocking;

  @BeforeEach
  void startOrigin() throws IOException {
    previousTimeout = GlobalConfiguration.NETWORK_REMOTE_FETCH_READ_TIMEOUT.getValue();
    previousBlocking = GlobalConfiguration.SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS.getValue();
    GlobalConfiguration.NETWORK_REMOTE_FETCH_READ_TIMEOUT.setValue(TIMEOUT_MS);
    // The origin is on loopback, which the SSRF guard blocks by default. That guard is not what is under test.
    GlobalConfiguration.SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS.setValue(false);

    release = new CountDownLatch(1);
    stallAfterSniff = new AtomicBoolean(false);
    origin = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    origin.createContext("/data", exchange -> {
      // Declares far more than it sends, then goes quiet holding the socket open: a hung HTTP connection, or a
      // proxy that keeps the socket after the origin dies.
      exchange.sendResponseHeaders(200, 1_000_000);
      // ENOUGH ROWS THAT THE SNIFF AND THE ANALYSIS BOTH FINISH BEFORE THE SILENCE STARTS, SO THE TIMEOUT LANDS
      // INSIDE THE FORMAT'S OWN READ LOOP AND ARRIVES AT load() ALREADY WRAPPED
      final StringBuilder head = new StringBuilder("id,name\n1,Jay\n");
      if (stallAfterSniff.get())
        for (int i = 2; i < 200; i++)
          head.append(i).append(",name").append(i).append('\n');
      exchange.getResponseBody().write(head.toString().getBytes(StandardCharsets.UTF_8));
      exchange.getResponseBody().flush();
      try {
        release.await(30, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      exchange.close();
    });
    origin.start();
    url = "http://127.0.0.1:" + origin.getAddress().getPort() + "/data";

    FileUtils.deleteRecursively(new File(DATABASE_PATH));
  }

  @AfterEach
  void stopOrigin() {
    release.countDown();
    if (origin != null)
      origin.stop(0);
    GlobalConfiguration.NETWORK_REMOTE_FETCH_READ_TIMEOUT.setValue(previousTimeout);
    GlobalConfiguration.SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS.setValue(previousBlocking);

    final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
    if (factory.exists())
      factory.open().drop();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    TestHelper.checkActiveDatabases();
  }

  /**
   * {@code @Timeout} is the hang detector; the {@link StallAwareStopwatch} bound is the assertion - a tripwire
   * between a bounded read and the unbounded one this issue is about, sized far above the configured timeout so a
   * JVM stall cannot turn it red.
   */
  @Test
  @Timeout(120)
  void aStalledRemoteSourceFailsWithAnErrorThatNamesTheUrlTheWaitAndTheSetting() {
    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();

    assertThatThrownBy(() -> new Importer(new String[] { "-url", url, "-database", DATABASE_PATH, "-documentType",
        "Doc", "-forceDatabaseCreate", "true" }).setAllowLocalUrls(true).load())
        .isInstanceOf(ImportException.class)
        .as("the source is named - it used to be 'null', because it is recorded only once the sniff succeeds")
        .hasMessageContaining(url)
        .as("and so is what actually went wrong, rather than only that something did")
        .hasMessageContaining(String.valueOf(TIMEOUT_MS))
        .hasMessageContaining(GlobalConfiguration.NETWORK_REMOTE_FETCH_READ_TIMEOUT.getKey())
        .as("the timeout keeps its type, so a caller that already distinguishes a stall from a broken source can")
        .hasRootCauseInstanceOf(SocketTimeoutException.class);

    stopwatch.assertGaveUpWithin(60_000, "a bounded remote read from one that waits for as long as the socket lives");
  }

  /**
   * The same source, stalling LATER: far enough in that the sniff succeeds and the CSV format is already consuming
   * the stream when the read times out. The format wraps that in its own
   * {@code ImportException("Error on importing CSV", timeout)}, so the timeout is no longer the outermost throwable
   * - and reporting only the outermost message threw away the wait and the setting name that the fetch layer had
   * gone to the trouble of putting in the cause (CodeRabbit on PR #7755).
   */
  @Test
  @Timeout(120)
  void aTimeoutWrappedByTheFormatStillNamesTheWaitAndTheSetting() {
    stallAfterSniff.set(true);

    assertThatThrownBy(() -> new Importer(new String[] { "-url", url, "-database", DATABASE_PATH, "-documentType",
        "Doc", "-forceDatabaseCreate", "true" }).setAllowLocalUrls(true).load())
        .isInstanceOf(ImportException.class)
        .as("the wrapper the format put on is kept - it says WHERE the import was")
        .hasMessageContaining(url)
        .as("and the cause it wrapped is reported too, rather than swallowed by it")
        .hasMessageContaining(String.valueOf(TIMEOUT_MS))
        .hasMessageContaining(GlobalConfiguration.NETWORK_REMOTE_FETCH_READ_TIMEOUT.getKey())
        .hasRootCauseInstanceOf(SocketTimeoutException.class);
  }
}
