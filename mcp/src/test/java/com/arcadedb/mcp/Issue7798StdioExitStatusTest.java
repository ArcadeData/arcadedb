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
package com.arcadedb.mcp;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7798: the stdio server is always launched by a supervisor - mcp-proxy, Claude Desktop, Cursor, a systemd
 * unit, a container runtime - and every one of them reads the exit status. The catch-all arm of
 * {@code MCPStdioServer.main} printed the stack trace and let {@code main} return, so a wrong root password, a
 * port already bound or an unreadable database directory exited <b>0</b>: the supervisor read a clean shutdown,
 * and there was no restart, no crash-loop backoff and no alert for a server that is simply not there.
 * <p>
 * The second half is the {@code System.exit(1)} calls that sat INSIDE the {@code try}: they skip the
 * {@code finally}, so the "MCP plugin is not installed" path tore the JVM down with the server it had just
 * started still running.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7798StdioExitStatusTest {

  /** A server that never boots anything: only its lifecycle is under test here. */
  private static class StubServer extends ArcadeDBServer {
    private final RuntimeException startFailure;
    private final AtomicInteger    stops = new AtomicInteger();

    StubServer(final RuntimeException startFailure) {
      super(new ContextConfiguration());
      this.startFailure = startFailure;
    }

    @Override
    public void start() {
      if (startFailure != null)
        throw startFailure;
    }

    @Override
    public void stop() {
      stops.incrementAndGet();
    }
  }

  @Test
  void aFatalStartupFailureExitsNonZeroAndStopsTheServer() {
    final StubServer server = new StubServer(new IllegalStateException("port 2480 already bound"));

    final ByteArrayOutputStream err = new ByteArrayOutputStream();
    final int status = runCapturingStdErr(err, () -> MCPStdioServer.startup(server, "anyPassword", emptyStdin(),
        discardedStdout()));

    assertThat(status).as("a supervisor must see a crash, not a clean shutdown").isEqualTo(1);
    assertThat(server.stops.get()).as("the failure path still runs the finally").isEqualTo(1);
    assertThat(err.toString(StandardCharsets.UTF_8))
        .as("the status must come from the reported failure, not from some other arm")
        .contains("port 2480 already bound");
  }

  @Test
  void aMissingMCPPluginExitsNonZeroAndStillStopsTheStartedServer() {
    // A server that starts but carries no MCPPlugin: MCPPlugin.of() answers null, which used to
    // System.exit(1) from inside the try and skip server.stop() on a server that WAS started.
    final StubServer server = new StubServer(null);

    final ByteArrayOutputStream err = new ByteArrayOutputStream();
    final int status = runCapturingStdErr(err, () -> MCPStdioServer.startup(server, "anyPassword", emptyStdin(),
        discardedStdout()));

    assertThat(status).isEqualTo(1);
    assertThat(server.stops.get()).as("the started server must be stopped before the JVM exits").isEqualTo(1);
    assertThat(err.toString(StandardCharsets.UTF_8)).contains("the MCP plugin is not installed");
  }

  /**
   * {@code startup} reports on {@code System.err} by contract - it owns stdout for the JSON-RPC stream - so the
   * test reads it back to pin WHICH arm produced the status.
   */
  private static int runCapturingStdErr(final ByteArrayOutputStream sink, final IntSupplier block) {
    final PrintStream previous = System.err;
    System.setErr(new PrintStream(sink, true, StandardCharsets.UTF_8));
    try {
      return block.getAsInt();
    } finally {
      System.setErr(previous);
    }
  }

  private static ByteArrayInputStream emptyStdin() {
    return new ByteArrayInputStream(new byte[0]);
  }

  private static PrintStream discardedStdout() {
    return new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8);
  }
}
