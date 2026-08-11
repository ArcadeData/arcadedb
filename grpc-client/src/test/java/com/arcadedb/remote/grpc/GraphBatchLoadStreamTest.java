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
package com.arcadedb.remote.grpc;

import com.arcadedb.exception.TimeoutException;
import com.arcadedb.remote.RemoteException;
import com.arcadedb.server.grpc.GraphBatchChunk;
import com.arcadedb.server.grpc.GraphBatchResult;
import io.grpc.Status;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The parts of {@link GraphBatchLoadStream} an integration test cannot reach: what a sender parked waiting for
 * the call to drain does when the call never drains, dies underneath it, or its thread is interrupted. Against a
 * live server the readiness wait is either instant or never observed, so these paths - the ones that decide
 * whether a bulk load fails or hangs - had no coverage at all.
 * <p>
 * Everything here drives the class against a hand-written {@link ClientCallStreamObserver} rather than a server,
 * which is what makes "never becomes ready" and "fails while a sender is parked" expressible at all.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GraphBatchLoadStreamTest {

  /** Long enough that a wait genuinely parks, short enough to be a test. */
  private static final long READY_WAIT_MS      = 500;
  /**
   * Used where the test has to tell a sender that was woken from one that merely timed out. The readiness
   * budget is set far above the time the assertion allows, so falling through it cannot be mistaken for a
   * notification: only an actual wake-up finishes in time.
   */
  private static final long LONG_READY_WAIT_MS = 10_000;
  private static final long PROMPT_MS          = 2_000;

  @Test
  void sendsStraightThroughWhileTheCallIsReady() {
    final FakeCall call = new FakeCall(true);
    final GraphBatchLoadStream stream = open(call);

    stream.send(chunk());
    stream.send(chunk());

    assertThat(call.sent).as("a ready call carries every chunk without parking").hasSize(2);
  }

  /**
   * The point of the readiness wait: a call that is not draining must hold the sender back rather than let it
   * pile chunks into the channel's buffer, which is what runs a bulk load out of heap.
   */
  @Test
  void aParkedSenderResumesWhenTheCallDrains() throws Exception {
    final FakeCall call = new FakeCall(false);
    final GraphBatchLoadStream stream = open(call, TimeUnit.SECONDS.toMillis(30), LONG_READY_WAIT_MS);

    final CountDownLatch sent = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread sender = new Thread(() -> {
      try {
        stream.send(chunk());
        sent.countDown();
      } catch (final Throwable t) {
        failure.set(t);
        sent.countDown();
      }
    });
    sender.start();

    assertThat(sent.await(200, TimeUnit.MILLISECONDS)).as("a call that is not ready must hold the sender").isFalse();
    assertThat(call.sent).isEmpty();

    call.becomeReady();

    assertThat(sent.await(PROMPT_MS, TimeUnit.MILLISECONDS))
        .as("the sender must resume when the call drains, not sit out the readiness budget").isTrue();
    assertThat(failure.get()).isNull();
    assertThat(call.sent).hasSize(1);
    sender.join(TimeUnit.SECONDS.toMillis(5));
  }

  /**
   * A call that dies while a sender is parked wakes it with the reason. Without that the sender waits out the
   * full readiness timeout on a call that is never coming back, and the caller waits with it.
   */
  @Test
  void aParkedSenderIsWokenByTheCallFailing() throws Exception {
    final FakeCall call = new FakeCall(false);
    final GraphBatchLoadStream stream = open(call, TimeUnit.SECONDS.toMillis(30), LONG_READY_WAIT_MS);

    final CountDownLatch finished = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread sender = new Thread(() -> {
      try {
        stream.send(chunk());
      } catch (final Throwable t) {
        failure.set(t);
      } finally {
        finished.countDown();
      }
    });
    sender.start();

    Thread.sleep(100); // let it park
    call.observer.onError(Status.INTERNAL.withDescription("server said no").asRuntimeException());

    assertThat(finished.await(PROMPT_MS, TimeUnit.MILLISECONDS))
        .as("the failure must wake the sender, not leave it to fall out of the readiness budget").isTrue();
    assertThat(failure.get()).as("and it must fail with the reason, not a timeout").isNotNull();
    assertThat(failure.get()).isNotInstanceOf(TimeoutException.class);
    assertThat(call.sent).as("nothing may be pushed onto a call that already failed").isEmpty();
    sender.join(TimeUnit.SECONDS.toMillis(5));
  }

  /** A call that never drains has to end as a timeout rather than a thread parked for good. */
  @Test
  void aSenderGivesUpWhenTheCallNeverDrains() {
    final FakeCall call = new FakeCall(false);
    final GraphBatchLoadStream stream = open(call);

    assertThatThrownBy(() -> stream.send(chunk()))
        .isInstanceOf(TimeoutException.class)
        .hasMessageContaining("waiting for the server to consume");

    assertThat(call.sent).isEmpty();
  }

  /** Interrupting a parked sender must surface as a failure and leave the flag set for the caller above. */
  @Test
  void anInterruptedSenderFailsAndKeepsTheInterruptFlag() throws Exception {
    final FakeCall call = new FakeCall(false);
    final GraphBatchLoadStream stream = open(call, TimeUnit.SECONDS.toMillis(30), LONG_READY_WAIT_MS);

    final CountDownLatch finished = new CountDownLatch(1);
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final AtomicBoolean stillInterrupted = new AtomicBoolean();
    final Thread sender = new Thread(() -> {
      try {
        stream.send(chunk());
      } catch (final Throwable t) {
        failure.set(t);
        stillInterrupted.set(Thread.currentThread().isInterrupted());
      } finally {
        finished.countDown();
      }
    });
    sender.start();

    Thread.sleep(100); // let it park
    sender.interrupt();

    assertThat(finished.await(PROMPT_MS, TimeUnit.MILLISECONDS))
        .as("an interrupt must break the wait immediately, not wait out the readiness budget").isTrue();
    assertThat(failure.get()).isInstanceOf(RemoteException.class);
    assertThat(failure.get()).hasMessageContaining("Interrupted");
    assertThat(stillInterrupted).as("the interrupt must not be swallowed on the way out").isTrue();
    sender.join(TimeUnit.SECONDS.toMillis(5));
  }

  @Test
  void completeReturnsTheServersResult() {
    final FakeCall call = new FakeCall(true);
    final GraphBatchLoadStream stream = open(call);

    call.observer.onNext(GraphBatchResult.newBuilder().setVerticesCreated(7).setEdgesCreated(3).build());
    call.observer.onCompleted();

    final GraphBatchResult result = stream.complete();
    assertThat(result.getVerticesCreated()).isEqualTo(7);
    assertThat(result.getEdgesCreated()).isEqualTo(3);
    assertThat(call.halfClosed).as("a healthy call is half-closed to end the load").isTrue();
  }

  /**
   * Half-closing a call the server has already failed would raise gRPC's own complaint on top of the real
   * failure and bury it. The failure is what the caller needs.
   */
  @Test
  void completeReportsAFailedCallWithoutHalfClosingIt() {
    final FakeCall call = new FakeCall(true);
    final GraphBatchLoadStream stream = open(call);

    call.observer.onError(Status.INTERNAL.withDescription("server said no").asRuntimeException());

    assertThatThrownBy(stream::complete).isInstanceOf(RuntimeException.class);
    assertThat(call.halfClosed).as("a call that already failed must not be half-closed again").isFalse();
  }

  /** A server that ends the stream without answering is a protocol failure, not an empty result. */
  @Test
  void completeRejectsAStreamThatEndedWithoutAResult() {
    final FakeCall call = new FakeCall(true);
    final GraphBatchLoadStream stream = open(call);

    call.observer.onCompleted();

    assertThatThrownBy(stream::complete)
        .isInstanceOf(RemoteException.class)
        .hasMessageContaining("without reporting its result");
  }

  /** The load's own deadline: a server that never answers must not park the caller for ever, and must be cut loose. */
  @Test
  void completeTimesOutAndCancelsTheCall() {
    final FakeCall call = new FakeCall(true);
    final GraphBatchLoadStream stream = open(call, 300);

    assertThatThrownBy(stream::complete)
        .isInstanceOf(TimeoutException.class)
        .hasMessageContaining("withTimeout()");

    assertThat(call.cancelled).as("a load given up on must release the server's batch slot").isTrue();
  }

  private GraphBatchLoadStream open(final FakeCall call) {
    return open(call, TimeUnit.SECONDS.toMillis(30));
  }

  private GraphBatchLoadStream open(final FakeCall call, final long timeoutMs) {
    return open(call, timeoutMs, READY_WAIT_MS);
  }

  private GraphBatchLoadStream open(final FakeCall call, final long timeoutMs, final long readyWaitMs) {
    final GraphBatchLoadStream stream = new GraphBatchLoadStream(timeoutMs, readyWaitMs);
    final ClientResponseObserver<GraphBatchChunk, GraphBatchResult> response = stream.responseObserver();
    response.beforeStart(call);
    // The fake call stands in for the server too: a test terminates the load through this.
    call.observer = response;
    stream.start(call);
    return stream;
  }

  private static GraphBatchChunk chunk() {
    return GraphBatchChunk.newBuilder().setDatabase("test").build();
  }

  /**
   * A call whose readiness is decided by the test rather than by a transport. Doubles as the request observer,
   * so what the loader pushes is recorded here too.
   */
  private static final class FakeCall extends ClientCallStreamObserver<GraphBatchChunk> {
    private final    List<GraphBatchChunk>            sent     = new ArrayList<>();
    private volatile boolean                          ready;
    private volatile Runnable                         onReady;
    private volatile boolean                          halfClosed;
    private volatile boolean                          cancelled;
    private volatile StreamObserver<GraphBatchResult> observer;

    private FakeCall(final boolean ready) {
      this.ready = ready;
    }

    private void becomeReady() {
      ready = true;
      final Runnable handler = onReady;
      if (handler != null)
        handler.run();
    }

    @Override
    public boolean isReady() {
      return ready;
    }

    @Override
    public void setOnReadyHandler(final Runnable onReadyHandler) {
      this.onReady = onReadyHandler;
    }

    @Override
    public void cancel(final String message, final Throwable cause) {
      cancelled = true;
    }

    @Override
    public void onNext(final GraphBatchChunk value) {
      sent.add(value);
    }

    @Override
    public void onError(final Throwable t) {
    }

    @Override
    public void onCompleted() {
      halfClosed = true;
    }

    @Override
    public void disableAutoRequestWithInitial(final int request) {
    }

    @Override
    public void disableAutoInboundFlowControl() {
    }

    @Override
    public void request(final int count) {
    }

    @Override
    public void setMessageCompression(final boolean enable) {
    }
  }
}
