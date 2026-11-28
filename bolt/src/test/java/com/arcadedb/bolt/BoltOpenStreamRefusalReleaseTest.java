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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.bolt.message.BoltMessage;
import com.arcadedb.bolt.message.DiscardMessage;
import com.arcadedb.bolt.message.PullMessage;
import com.arcadedb.bolt.packstream.PackStreamReader;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.server.FakeArcadeDBServer;
import com.arcadedb.server.FakeServerSecurity;
import com.arcadedb.server.TestServerHelper;
import com.arcadedb.server.security.ServerSecurityException;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.Socket;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A PULL or a DISCARD refused for the user of the connection releases what the connection holds at the moment it is refused,
 * not at the RESET that follows: every open stream is closed and the explicit transaction is rolled back. Driven on the
 * handlers themselves, with a database that counts its rollbacks, because neither is visible from the other end of the wire.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class BoltOpenStreamRefusalReleaseTest {

  /** A result set that counts its closes. */
  private static final class CountingResultSet extends IteratorResultSet {
    private final AtomicInteger closes = new AtomicInteger();

    CountingResultSet() {
      super(Collections.emptyIterator());
    }

    @Override
    public void close() {
      closes.incrementAndGet();
    }
  }

  @Test
  void aUserThatNoLongerExistsIsRefusedAndItsWorkReleased() throws Exception {
    final FakeServerSecurity security = FakeServerSecurity.create().fails("revalidate", new ServerSecurityException("gone"));

    final Outcome outcome = run(new PullMessage(Map.of("n", -1L)), security, "probe");

    assertThat(outcome.code).isEqualTo(BoltErrorCodes.AUTHENTICATION_ERROR);
    assertReleased(outcome);
    assertThat(field(outcome.executor, "user")).as("the connection stands for no user any more").isNull();
  }

  @Test
  void aUserWithoutTheDatabaseIsRefusedAndItsWorkReleased() throws Exception {
    final FakeServerSecurity security = FakeServerSecurity.create().on("revalidate", args -> args[0]);

    final Outcome outcome = run(new DiscardMessage(Map.of("n", -1L)), security, "elsewhere");

    assertThat(outcome.code).isEqualTo(BoltErrorCodes.FORBIDDEN_ERROR);
    assertReleased(outcome);
  }

  private static void assertReleased(final Outcome outcome) throws Exception {
    assertThat(outcome.stateAfter).isEqualTo("FAILED");
    assertThat(outcome.firstStream.closes.get()).as("first stream closed").isEqualTo(1);
    assertThat(outcome.secondStream.closes.get()).as("second stream closed").isEqualTo(1);
    assertThat(outcome.rollbacks.get()).as("the explicit transaction is rolled back").isEqualTo(1);
    assertThat(field(outcome.executor, "explicitTransaction")).isEqualTo(false);
    assertThat((Map<?, ?>) field(outcome.executor, "openStreams")).as("nothing is left registered").isEmpty();
  }

  /** What an executor with two open streams in an explicit transaction did with {@code message}. */
  private record Outcome(BoltNetworkExecutor executor, String code, String stateAfter, CountingResultSet firstStream,
                         CountingResultSet secondStream, AtomicInteger rollbacks) {
  }

  /**
   * Runs {@code message} through the dispatcher of an executor in TX_STREAMING, holding two streams, whose user is granted
   * {@code grantedDatabase} only and whose database is named {@code probe}.
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static Outcome run(final BoltMessage message, final FakeServerSecurity security, final String grantedDatabase)
      throws Exception {
    final FakeArcadeDBServer server = FakeArcadeDBServer.create();
    server.security(security);
    server.returns("getConfiguration", new ContextConfiguration());

    final AtomicInteger rollbacks = new AtomicInteger();
    final CountingResultSet first = new CountingResultSet();
    final CountingResultSet second = new CountingResultSet();

    try (final Socket socket = new Socket()) {
      final BoltNetworkExecutor executor = new BoltNetworkExecutor(server, socket, null);
      final ByteArrayOutputStream wire = new ByteArrayOutputStream();

      set(executor, "output", new BoltChunkedOutput(wire));
      set(executor, "protocolVersion", 0x00000405);
      set(executor, "database", countingDatabase(rollbacks));
      set(executor, "user", TestServerHelper.securityUser("probeUser", grantedDatabase));
      set(executor, "explicitTransaction", true);
      set(executor, "state", Enum.valueOf((Class<Enum>) Class.forName("com.arcadedb.bolt.BoltNetworkExecutor$State"), "TX_STREAMING"));

      final Map<Long, BoltQueryStream> open = (Map<Long, BoltQueryStream>) field(executor, "openStreams");
      final BoltQueryStream firstStream = new BoltQueryStream(0);
      firstStream.resultSet = first;
      open.put(0L, firstStream);
      final BoltQueryStream secondStream = new BoltQueryStream(1);
      secondStream.resultSet = second;
      open.put(1L, secondStream);

      final Method processMessage = BoltNetworkExecutor.class.getDeclaredMethod("processMessage", BoltMessage.class);
      processMessage.setAccessible(true);
      processMessage.invoke(executor, message);

      final byte[] reply = new BoltChunkedInput(new ByteArrayInputStream(wire.toByteArray())).readMessage();
      assertThat(reply[1]).as("the answer is a FAILURE").isEqualTo(BoltMessage.FAILURE);
      final PackStreamReader reader = new PackStreamReader(reply);
      reader.readRawByte();
      reader.readRawByte();
      final String code = String.valueOf(((Map<String, Object>) reader.readValue()).get("code"));

      return new Outcome(executor, code, String.valueOf(field(executor, "state")), first, second, rollbacks);
    }
  }

  /** A database named {@code probe} that counts the rollbacks asked of it. */
  private static DatabaseInternal countingDatabase(final AtomicInteger rollbacks) {
    final InvocationHandler handler = (proxy, method, args) -> switch (method.getName()) {
      case "rollbackAllNested" -> {
        rollbacks.incrementAndGet();
        yield null;
      }
      case "isOpen" -> true;
      case "getName" -> "probe";
      // No database context is registered under this path, so nothing is bound to one
      case "getDatabasePath" -> "probe";
      case "toString" -> "countingDatabase";
      case "hashCode" -> System.identityHashCode(proxy);
      case "equals" -> proxy == args[0];
      default -> throw new UnsupportedOperationException(
          "the handler under test called Database." + method.getName() + "(), which this test does not model");
    };
    return (DatabaseInternal) Proxy.newProxyInstance(BoltOpenStreamRefusalReleaseTest.class.getClassLoader(),
        new Class<?>[] { DatabaseInternal.class }, handler);
  }

  private static Object field(final Object target, final String name) throws Exception {
    final Field field = BoltNetworkExecutor.class.getDeclaredField(name);
    field.setAccessible(true);
    return field.get(target);
  }

  private static void set(final Object target, final String name, final Object value) throws Exception {
    final Field field = BoltNetworkExecutor.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }
}
