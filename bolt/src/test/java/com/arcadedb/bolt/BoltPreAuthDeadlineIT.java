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
package com.arcadedb.bolt;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.bolt.message.BoltMessage;
import com.arcadedb.log.DefaultLogger;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * The BOLT listener inherits the pre-authentication guarantees of the gate every binary listener takes its connections from:
 * a connection that never authenticates is closed at the deadline even when it is never idle (a NOOP chunk every few hundred
 * milliseconds keeps a socket busy for ever at two bytes a time), and junk from many unauthenticated connections writes a
 * bounded log rather than a WARNING per connection.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BoltPreAuthDeadlineIT extends BaseBoltServerTest {

  private static final long DEADLINE_MS = 2_000;

  private final AtomicInteger executorRecords = new AtomicInteger();

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
    GlobalConfiguration.NETWORK_PREAUTH_TIMEOUT.setValue(DEADLINE_MS);
  }

  @BeforeEach
  @Override
  public void beginTest() {
    super.beginTest();
    LogManager.instance().setLogger(countingLogger());
  }

  @AfterEach
  @Override
  public void endTest() {
    try {
      // process-wide: put the default back, or every later log-asserting test in this fork captures nothing
      LogManager.instance().setLogger(new DefaultLogger());
      GlobalConfiguration.SERVER_PLUGINS.setValue("");
      GlobalConfiguration.NETWORK_PREAUTH_TIMEOUT.reset();
    } finally {
      super.endTest();
    }
  }

  @Test
  void aNegotiatedConnectionThatNeverLogsOnIsClosedAtTheDeadline() throws Exception {
    try (final Socket socket = new Socket("localhost", getServerBoltPort())) {
      socket.setSoTimeout(500);
      final OutputStream out = socket.getOutputStream();
      out.write(handshake());
      out.flush();
      final byte[] negotiated = socket.getInputStream().readNBytes(4);
      assertThat(negotiated).hasSize(4);

      // a hang detector, not a latency bound: the deadline is 2 s and the reaper ticks every second
      final long giveUpAt = System.currentTimeMillis() + 30_000;
      while (System.currentTimeMillis() < giveUpAt) {
        try {
          // a NOOP chunk: valid, unanswered, and never idle
          out.write(new byte[] { 0, 0 });
          out.flush();
        } catch (final SocketException e) {
          return;
        }
        if (isClosedByServer(socket))
          return;
      }
      fail("the server kept a connection that never authenticated open past its pre-authentication deadline");
    }
  }

  @Test
  void anAuthenticatedConnectionOutlivesTheDeadline() throws Exception {
    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      Thread.sleep(DEADLINE_MS + 2_000);
      bolt.run("RETURN 1 AS one");
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
    }
  }

  @Test
  void junkFromManyUnauthenticatedConnectionsWritesABoundedLog() throws Exception {
    final int before = executorRecords.get();
    for (int i = 0; i < 30; i++)
      try (final Socket socket = new Socket("localhost", getServerBoltPort())) {
        socket.setSoTimeout(10_000);
        // not the BOLT magic, not TLS, not HTTP
        socket.getOutputStream().write(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 });
        socket.getOutputStream().flush();
        drain(socket.getInputStream());
      }
    assertThat(executorRecords.get() - before).as("records written for 30 refused connections").isLessThanOrEqualTo(1);
  }

  private static byte[] handshake() {
    final ByteBuffer handshake = ByteBuffer.allocate(20);
    handshake.put((byte) 0x60).put((byte) 0x60).put((byte) 0xB0).put((byte) 0x17);
    handshake.putInt(0x00000405).putInt(0x00000404).putInt(0x00000003).putInt(0);
    return handshake.array();
  }

  private static boolean isClosedByServer(final Socket socket) throws IOException {
    try {
      return socket.getInputStream().read() == -1;
    } catch (final SocketTimeoutException e) {
      return false;
    } catch (final SocketException e) {
      return true;
    }
  }

  private static void drain(final InputStream in) throws IOException {
    final byte[] buffer = new byte[256];
    try {
      while (in.read(buffer) != -1) {
        // until the server closes
      }
    } catch (final SocketException e) {
      // a reset is a close too
    }
  }

  private Logger countingLogger() {
    final Logger delegate = new DefaultLogger();
    return new Logger() {
      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception,
          final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
          final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11,
          final Object arg12, final Object arg13, final Object arg14, final Object arg15, final Object arg16,
          final Object arg17) {
        count(requester, level);
        delegate.log(requester, level, message, exception, context, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10,
            arg11, arg12, arg13, arg14, arg15, arg16, arg17);
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception,
          final String context, final Object... args) {
        count(requester, level);
        delegate.log(requester, level, message, exception, context, args);
      }

      @Override
      public boolean isLoggable(final Object requester, final Level level) {
        return delegate.isLoggable(requester, level);
      }

      @Override
      public void flush() {
        delegate.flush();
      }

      private void count(final Object requester, final Level level) {
        if (requester instanceof BoltNetworkExecutor && level.intValue() >= Level.INFO.intValue())
          executorRecords.incrementAndGet();
      }
    };
  }
}
