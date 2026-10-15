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
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestServerHelper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.EOFException;
import java.nio.file.Path;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10092: an MCP tool failure whose exception carries no message ({@code ConcurrentModificationException},
 * {@code NoSuchElementException}, a no-arg {@code IllegalStateException}, an {@code EOFException}) answered the agent
 * with a content block whose {@code text} was JSON null and logged the word {@code null} with no stack trace. MCP types
 * the block's text as a string, so a strict client rejected the block and a lenient one showed "null": the agent was told
 * the tool failed and nothing about why, on a transport that answers HTTP 200 for a tool error.
 * <p>
 * The text now falls back to the exception's type, like the gRPC surface does, and the log entry names the failure and
 * carries the stack trace that is the only other clue to where it came from.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10092MCPNullMessageFailureTest {
  @TempDir
  Path root;

  private MCPDispatcher dispatcher(final boolean production) {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_MODE, production ? "production" : "development");
    final ArcadeDBServer server = TestServerHelper.unstartedServer(root, configuration);
    return new MCPDispatcher(server, new MCPConfiguration(root.resolve("config")), "http");
  }

  @Test
  void aFailureWithoutMessageIsNamedByItsTypeInDevelopmentMode() {
    final MCPDispatcher dispatcher = dispatcher(false);

    assertThat(dispatcher.failureMessage("tools/call 'query'", new ConcurrentModificationException()))
        .isEqualTo("ConcurrentModificationException");
    assertThat(dispatcher.failureMessage("tools/call 'query'", new NoSuchElementException()))
        .isEqualTo("NoSuchElementException");
    assertThat(dispatcher.failureMessage("tools/call 'query'", new IllegalStateException()))
        .isEqualTo("IllegalStateException");
    assertThat(dispatcher.failureMessage("tools/call 'query'", new EOFException())).isEqualTo("EOFException");
    assertThat(dispatcher.failureMessage("tools/call 'query'", new InterruptedException())).isEqualTo("InterruptedException");
  }

  @Test
  void aBlankMessageIsNoMessageEither() {
    assertThat(dispatcher(false).failureMessage("tools/call 'query'", new IllegalStateException("  "))).isEqualTo("IllegalStateException");
  }

  @Test
  void aRealMessageIsUnchanged() {
    assertThat(dispatcher(false).failureMessage("tools/call 'query'", new IllegalStateException("a real message")))
        .isEqualTo("a real message");
  }

  @Test
  void productionModeStillAnswersThePlaceholderAndNeverNull() {
    assertThat(dispatcher(true).failureMessage("tools/call 'query'", new NoSuchElementException()))
        .isEqualTo(ArcadeDBServer.CONCEALED_ERROR_MESSAGE);
  }

  @Test
  void theLogNamesTheFailureAndCarriesItsTrace() {
    final RecordingLogger logger = new RecordingLogger();
    final Logger previous = LogManager.instance().getLogger();
    LogManager.instance().setLogger(logger);
    try {
      dispatcher(false).failureMessage("tools/call 'query'", new NoSuchElementException());
    } finally {
      LogManager.instance().setLogger(previous);
    }

    assertThat(logger.lines).anySatisfy(line -> assertThat(line).contains("tools/call 'query'").contains("NoSuchElementException")
        .doesNotContain("error: null"));
    assertThat(logger.traced).as("the stack trace is the only clue to where a message-less failure came from").isTrue();
  }

  private static class RecordingLogger implements Logger {
    private final List<String> lines = new CopyOnWriteArrayList<>();
    private volatile boolean traced;

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
        final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11,
        final Object arg12, final Object arg13, final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
      lines.add(String.format(message, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15,
          arg16, arg17));
      if (exception != null)
        traced = true;
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object... args) {
      lines.add(String.format(message, args));
      if (exception != null)
        traced = true;
    }

    @Override
    public void flush() {
    }
  }
}
