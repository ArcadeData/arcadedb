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
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.QueryAdmissionGate;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.FakeArcadeDBServer;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.Socket;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Closing the result streams of a connection: a stream gives back its admission slot whatever its result set did when it was
 * closed, every stream of a connection is closed even when one of them raises an {@link Error}, and the rest of the
 * connection's teardown still runs.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class BoltStreamTeardownTest {
  private final QueryAdmissionGate gate = QueryAdmissionGate.getInstance();

  /** A result set that counts its closes and, when told to, raises an {@link Error} from them. */
  private static final class CountingResultSet extends IteratorResultSet {
    private final AtomicInteger closes = new AtomicInteger();
    private final boolean       failing;

    CountingResultSet(final boolean failing) {
      super(Collections.emptyIterator());
      this.failing = failing;
    }

    @Override
    public void close() {
      closes.incrementAndGet();
      if (failing)
        throw new LinkageError("simulated");
    }
  }

  @Test
  void aStreamGivesItsSlotBackWhenItsResultSetRaisesAnErrorOnClose() {
    final int running = gate.getRunning();
    final BoltQueryStream stream = new BoltQueryStream(0);
    stream.resultSet = new CountingResultSet(true);
    stream.admission = gate.admit();
    assertThat(gate.getRunning()).as("the slot is taken").isEqualTo(running + 1);

    assertThat(catchThrowable(() -> stream.close(this, "test"))).isInstanceOf(LinkageError.class);

    assertThat(gate.getRunning()).as("the slot is given back").isEqualTo(running);
    assertThat(stream.admission).isNull();
    assertThat((Object) stream.resultSet).isNull();

    // A SECOND CLOSE FINDS NOTHING LEFT TO RELEASE
    stream.close(this, "test again");
    assertThat(gate.getRunning()).isEqualTo(running);
  }

  @Test
  void everyStreamOfAConnectionIsClosedWhenOneOfThemRaisesAnError() throws Exception {
    final CountingResultSet first = new CountingResultSet(true);
    final CountingResultSet second = new CountingResultSet(false);
    final CountingResultSet third = new CountingResultSet(true);

    try (final Socket socket = new Socket()) {
      final BoltNetworkExecutor executor = executorWith(socket, first, second, third);

      final Throwable raised = catchThrowable(() -> invoke(executor, "closeAllStreams", "test"));

      assertThat(raised).as("the first Error is the one raised").isInstanceOf(LinkageError.class);
      assertThat(raised.getSuppressed()).as("the second one is kept with it").hasSize(1);
      assertThat(first.closes.get()).isEqualTo(1);
      assertThat(second.closes.get()).as("a stream after the failing one").isEqualTo(1);
      assertThat(third.closes.get()).isEqualTo(1);
      assertThat(openStreamsOf(executor)).as("nothing is left registered").isEmpty();
    }
  }

  @Test
  void theRestOfTheConnectionTeardownRunsWhenAStreamRaisesAnError() throws Exception {
    final CountingResultSet first = new CountingResultSet(true);
    final CountingResultSet second = new CountingResultSet(false);

    try (final Socket socket = new Socket()) {
      final BoltNetworkExecutor executor = executorWith(socket, first, second);

      final Throwable raised = catchThrowable(() -> invoke(executor, "cleanup"));

      assertThat(raised).isInstanceOf(LinkageError.class);
      assertThat(second.closes.get()).as("a stream after the failing one").isEqualTo(1);
      assertThat(socket.isClosed()).as("the connection's socket is closed").isTrue();
    }
  }

  @Test
  void aTeardownWithNothingRaisedClosesEveryStreamOnce() throws Exception {
    final CountingResultSet first = new CountingResultSet(false);
    final CountingResultSet second = new CountingResultSet(false);

    try (final Socket socket = new Socket()) {
      final BoltNetworkExecutor executor = executorWith(socket, first, second);

      invoke(executor, "cleanup");

      assertThat(first.closes.get()).isEqualTo(1);
      assertThat(second.closes.get()).isEqualTo(1);
      assertThat(openStreamsOf(executor)).isEmpty();
      assertThat(socket.isClosed()).isTrue();
    }
  }

  /** An executor, on an unconnected socket, with one open stream per result set, in the order given. */
  private static BoltNetworkExecutor executorWith(final Socket socket, final ResultSet... resultSets) throws Exception {
    final FakeArcadeDBServer server = FakeArcadeDBServer.create();
    server.returns("getConfiguration", new ContextConfiguration());
    final BoltNetworkExecutor executor = new BoltNetworkExecutor(server, socket, null);

    final Map<Long, BoltQueryStream> open = openStreamsOf(executor);
    long qid = 0;
    for (final ResultSet resultSet : resultSets) {
      final BoltQueryStream stream = new BoltQueryStream(qid);
      stream.resultSet = resultSet;
      open.put(qid++, stream);
    }
    return executor;
  }

  @SuppressWarnings("unchecked")
  private static Map<Long, BoltQueryStream> openStreamsOf(final BoltNetworkExecutor executor) throws Exception {
    final Field field = BoltNetworkExecutor.class.getDeclaredField("openStreams");
    field.setAccessible(true);
    return (Map<Long, BoltQueryStream>) field.get(executor);
  }

  /** Calls a private method of the executor, raising what the method raised rather than the reflection's wrapper of it. */
  private static void invoke(final BoltNetworkExecutor executor, final String name, final String... phase) throws Exception {
    final Method method = phase.length == 0 ?
        BoltNetworkExecutor.class.getDeclaredMethod(name) :
        BoltNetworkExecutor.class.getDeclaredMethod(name, String.class);
    method.setAccessible(true);
    try {
      if (phase.length == 0)
        method.invoke(executor);
      else
        method.invoke(executor, phase[0]);
    } catch (final InvocationTargetException e) {
      if (e.getCause() instanceof Error error)
        throw error;
      throw e;
    }
  }
}
