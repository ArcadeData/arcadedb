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
package com.arcadedb.server.grpc;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7308: {@link GrpcProgressStream} claims in its javadoc that its {@code synchronized} methods
 * are what keep two progress writers from interleaving into a corrupt frame - an import samples
 * {@code ImporterContext} on a timer thread while the importer logs on the calling thread, and a
 * parallel restore logs from its worker pool (issue #6086). A {@link StreamObserver} is not safe for
 * concurrent use, so that claim is load-bearing rather than decorative.
 * <p>
 * The claim is asserted directly here rather than through a live restore, which could not make the
 * two writers overlap on demand. The observer counts concurrent entries into {@code onNext} and fails
 * the test if it is ever entered twice at once: without the lock this trips, with it the count never
 * leaves 1.
 */
class GrpcProgressStreamConcurrencyTest {

  private static final int EVENTS_PER_WRITER = 2_000;

  /**
   * An observer that behaves like the real one in the way that matters: it is not thread-safe, and it
   * says so loudly instead of quietly corrupting its state.
   */
  private static final class ConcurrencyCheckingObserver implements StreamObserver<RestoreProgress> {
    private final List<RestoreProgress> received      = new ArrayList<>();
    private final AtomicInteger         inFlight      = new AtomicInteger();
    private final AtomicInteger         maxInFlight   = new AtomicInteger();
    private final AtomicInteger         completions   = new AtomicInteger();
    private final AtomicReference<Throwable> error    = new AtomicReference<>();
    private volatile int                afterComplete = 0;

    @Override
    public void onNext(final RestoreProgress value) {
      final int concurrent = inFlight.incrementAndGet();
      maxInFlight.accumulateAndGet(concurrent, Math::max);
      try {
        if (completions.get() > 0)
          afterComplete++;
        // Not a thread-safe list: this is the write the lock has to serialise.
        received.add(value);
      } finally {
        inFlight.decrementAndGet();
      }
    }

    @Override
    public void onError(final Throwable t) {
      error.set(t);
    }

    @Override
    public void onCompleted() {
      completions.incrementAndGet();
    }
  }

  @Test
  void twoConcurrentWritersNeverEnterTheObserverAtOnce() throws Exception {
    final ConcurrencyCheckingObserver observer = new ConcurrencyCheckingObserver();

    GrpcProgressStream.stream(observer,
        message -> RestoreProgress.newBuilder().setMessage(message).build(),
        (parsed, vertices, edges) -> RestoreProgress.newBuilder().setMessage("counters " + parsed).build(),
        report -> RestoreProgress.newBuilder().setCompleted(true).setMessage("done").build(),
        listener -> {
          // The importer's counter timer, in miniature: a second thread reporting while the calling
          // thread reports its own log lines.
          final Thread counters = new Thread(() -> {
            for (int i = 0; i < EVENTS_PER_WRITER; i++)
              listener.onImportCounters(i, i, i);
          }, "test-counter-writer");
          counters.start();

          for (int i = 0; i < EVENTS_PER_WRITER; i++)
            listener.onProgress("line " + i);

          counters.join();
          return null;
        },
        e -> Status.INTERNAL.withDescription(e.getMessage()).asException());

    assertThat(observer.error.get()).isNull();
    assertThat(observer.maxInFlight.get()).as("two writers must never be inside onNext at the same time").isEqualTo(1);
    assertThat(observer.received).hasSize(EVENTS_PER_WRITER * 2 + 1);
    assertThat(observer.completions.get()).isEqualTo(1);
    assertThat(observer.afterComplete).as("no message may follow the completion").isZero();
    assertThat(observer.received.getLast().getCompleted()).isTrue();
    assertThat(observer.received.subList(0, observer.received.size() - 1)).allMatch(p -> !p.getCompleted());
  }

  /**
   * The other half of the contract: a failure ends the stream with an error status and no completion,
   * so a client that read a completed message has a result that succeeded. Progress already delivered
   * stays delivered - it happened.
   */
  @Test
  void aFailingBodyEndsTheStreamWithAnErrorAndNoCompletion() {
    final ConcurrencyCheckingObserver observer = new ConcurrencyCheckingObserver();

    GrpcProgressStream.stream(observer,
        message -> RestoreProgress.newBuilder().setMessage(message).build(), null,
        report -> RestoreProgress.newBuilder().setCompleted(true).build(),
        listener -> {
          listener.onProgress("started");
          throw new IllegalStateException("restore blew up");
        },
        e -> Status.INTERNAL.withDescription(e.getMessage()).asException());

    assertThat(observer.received).hasSize(1);
    assertThat(observer.received.getFirst().getCompleted()).isFalse();
    assertThat(observer.completions.get()).isZero();
    assertThat(observer.error.get()).hasMessageContaining("restore blew up");
  }

  /**
   * An {@link Error} - an {@code OutOfMemoryError} part-way through a large restore is the realistic
   * one - must still end the call, and must still propagate.
   * <p>
   * Ending the call is not tidiness: these RPCs carry no deadline by design, because a restore runs
   * for as long as the data takes, so a client blocked reading this stream has nothing else that
   * would ever wake it. An Error that left the stream open would be a permanent hang rather than a
   * bounded failure. Propagating it afterwards is the other half: an Error is not
   * {@code GrpcProgressStream}'s to absorb.
   */
  @Test
  void anErrorFromTheBodyEndsTheStreamAndStillPropagates() {
    final ConcurrencyCheckingObserver observer = new ConcurrencyCheckingObserver();

    assertThatThrownBy(() -> GrpcProgressStream.stream(observer,
        message -> RestoreProgress.newBuilder().setMessage(message).build(), null,
        report -> RestoreProgress.newBuilder().setCompleted(true).build(),
        listener -> {
          listener.onProgress("started");
          throw new OutOfMemoryError("restore ran out of heap");
        },
        e -> Status.INTERNAL.withDescription(e.getMessage()).asException()))
        .isInstanceOf(OutOfMemoryError.class)
        .hasMessage("restore ran out of heap");

    assertThat(observer.error.get()).as("the call must have been ended, or a client would wait forever")
        .isNotNull();
    assertThat(observer.completions.get()).as("an Error is not a completion").isZero();
  }

  /** A restore reports no counters, so a stream built without a counter mapper simply drops them. */
  @Test
  void aStreamWithNoCounterMapperIgnoresCounterEvents() {
    final ConcurrencyCheckingObserver observer = new ConcurrencyCheckingObserver();

    GrpcProgressStream.stream(observer,
        message -> RestoreProgress.newBuilder().setMessage(message).build(), null,
        report -> RestoreProgress.newBuilder().setCompleted(true).build(),
        listener -> {
          listener.onImportCounters(1, 2, 3);
          listener.onProgress("the only message");
          return null;
        },
        e -> Status.INTERNAL.withDescription(e.getMessage()).asException());

    assertThat(observer.received).hasSize(2);
    assertThat(observer.received.getFirst().getMessage()).isEqualTo("the only message");
  }
}
