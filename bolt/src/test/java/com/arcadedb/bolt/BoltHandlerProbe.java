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
import com.arcadedb.bolt.message.BeginMessage;
import com.arcadedb.bolt.message.BoltMessage;
import com.arcadedb.bolt.packstream.PackStreamReader;
import com.arcadedb.database.Database;
import com.arcadedb.server.ArcadeDBServer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.Socket;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Drives one of {@code BoltNetworkExecutor}'s private request handlers and reports the FAILURE the connection
 * would have received.
 * <p>
 * It exists because the thing under test is <i>which handler sends which status code</i>, and that is not
 * provable by calling the classifier: the two handlers issue #7915 is about did call a classifier-shaped thing -
 * a hand-written constant - and agreed with the classifier on nine categories out of ten. A test that asserted
 * over {@code classifyExecutionError} alone would have passed on the broken build. So the real handler runs,
 * against a database whose {@code begin}/{@code commit}/{@code rollback} raise the exception under test, and the
 * assertion is made on the bytes that reach the wire.
 * <p>
 * The socket is deliberately unconnected: the handlers never touch it, only the constructor does (for the thread
 * name), and the FAILURE is collected from the {@code BoltChunkedOutput} this class installs over a buffer.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class BoltHandlerProbe {

  /** The states {@code BoltNetworkExecutor.State} declares, named here because the enum is private. */
  static final String READY    = "READY";
  static final String TX_READY = "TX_READY";

  private BoltHandlerProbe() {
    // utility class
  }

  /**
   * Runs {@code handlerName} on an executor whose database throws {@code failure} from every transaction call,
   * and returns the code of the FAILURE it sent.
   *
   * @param handlerName the private no-argument handler to invoke ("handleBegin", "handleCommit", "handleRollback")
   * @param state       the state to put the executor in first, as one of the constants above
   * @param failure     what the database raises from begin/commit/rollback
   */
  static String failureCodeOf(final String handlerName, final String state, final RuntimeException failure)
      throws Exception {
    return failureMetadataOf(handlerName, state, failure).get("code").toString();
  }

  @SuppressWarnings("unchecked")
  static Map<String, Object> failureMetadataOf(final String handlerName, final String state,
      final RuntimeException failure) throws Exception {
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getConfiguration()).thenReturn(new ContextConfiguration());

    try (final Socket socket = new Socket()) {
      final BoltNetworkExecutor executor = new BoltNetworkExecutor(server, socket, null);
      final ByteArrayOutputStream wire = new ByteArrayOutputStream();

      set(executor, "output", new BoltChunkedOutput(wire));
      set(executor, "protocolVersion", 0x00000405);
      set(executor, "database", throwingDatabase(failure));
      set(executor, "state", stateNamed(state));

      // handleBegin takes the BEGIN message, the other two take nothing; invoked with the real signature so
      // this probe drives the production handler rather than a test-only overload of it.
      final boolean takesMessage = "handleBegin".equals(handlerName);
      final Method handler = takesMessage
          ? BoltNetworkExecutor.class.getDeclaredMethod(handlerName, BeginMessage.class)
          : BoltNetworkExecutor.class.getDeclaredMethod(handlerName);
      handler.setAccessible(true);
      if (takesMessage)
        handler.invoke(executor, new BeginMessage(Map.of()));
      else
        handler.invoke(executor);

      final byte[] message = new BoltChunkedInput(new ByteArrayInputStream(wire.toByteArray())).readMessage();
      if (message[1] != BoltMessage.FAILURE)
        throw new IllegalStateException(handlerName + " answered signature 0x" + Integer.toHexString(message[1] & 0xFF)
            + " instead of FAILURE");

      final PackStreamReader reader = new PackStreamReader(message);
      reader.readRawByte(); // structure header
      reader.readRawByte(); // signature
      return (Map<String, Object>) reader.readValue();
    }
  }

  /**
   * A {@link Database} that answers the handful of questions {@code ensureDatabase()} asks of an already-resolved
   * handle and raises {@code failure} from the three transaction methods. A dynamic proxy rather than a mock so
   * the default for every other method of a very wide interface is an explicit, loud failure.
   */
  private static Database throwingDatabase(final RuntimeException failure) {
    final InvocationHandler handler = (proxy, method, args) -> switch (method.getName()) {
      case "begin", "commit", "rollback" -> throw failure;
      case "isOpen" -> true;
      case "getName" -> "probe";
      case "toString" -> "throwingDatabase";
      case "hashCode" -> System.identityHashCode(proxy);
      case "equals" -> proxy == args[0];
      default -> throw new UnsupportedOperationException(
          "the handler under test called Database." + method.getName() + "(), which this probe does not model");
    };
    return (Database) Proxy.newProxyInstance(BoltHandlerProbe.class.getClassLoader(), new Class<?>[] { Database.class },
        handler);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static Object stateNamed(final String name) throws Exception {
    final Class<?> stateEnum = Class.forName("com.arcadedb.bolt.BoltNetworkExecutor$State");
    return Enum.valueOf((Class<Enum>) stateEnum, name);
  }

  private static void set(final Object target, final String field, final Object value) throws Exception {
    final java.lang.reflect.Field f = BoltNetworkExecutor.class.getDeclaredField(field);
    f.setAccessible(true);
    f.set(target, value);
  }
}
