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
import com.arcadedb.server.grpc.GraphBatchProtocol;
import com.arcadedb.server.grpc.GraphBatchResult;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The client end of one {@code GraphBatchLoad} call: chunks are pushed in, one {@link GraphBatchResult} comes
 * back when the stream ends. Written for a bulk load rather than a handful of messages, which is what the two
 * things it does beyond opening the stream are for (issue #6070).
 * <p>
 * The first is backpressure. A client-streaming RPC accepts every {@code onNext} whether or not the transport
 * can carry it, buffering the excess in the channel, so a loader pushing millions of records as fast as it can
 * build them runs the client out of heap long before the server is the bottleneck. {@link #send} therefore
 * waits for the call to report itself ready, which is the only signal gRPC gives that the outbound buffer has
 * drained.
 * <p>
 * The second is what happens when the server fails the load. The batch commits incrementally, so an error is
 * not a rollback: the server reports what it had already committed on the trailers of the failed call, and that
 * is folded into the exception message here rather than dropped, because a caller that only knows "the load
 * failed" has no way to reconcile short of re-sending everything.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GraphBatchLoadStream {

  /**
   * How long {@link #send} waits for the call to become ready again before giving up. Generous because the
   * server pulls a chunk at a time and a chunk can take a while to load; short enough that a connection that
   * died without notifying leaves the caller with an error instead of a permanently parked thread.
   */
  private static final long READY_WAIT_TIMEOUT_MS = 5 * 60 * 1000L;

  private final long timeoutMs;
  /** Same as {@link #READY_WAIT_TIMEOUT_MS}, kept per-instance so a test can wait a plausible moment instead of five minutes. */
  private final long readyWaitTimeoutMs;

  private final CountDownLatch                    done             = new CountDownLatch(1);
  private final AtomicReference<GraphBatchResult> resultRef        = new AtomicReference<>();
  private final AtomicReference<GraphBatchResult> partialResultRef = new AtomicReference<>();
  private final AtomicReference<Throwable>        errorRef         = new AtomicReference<>();

  /** Guards the wait/notify pairing between a sender parked on readiness and the gRPC thread reporting it. */
  private final Object readyLock = new Object();

  private volatile ClientCallStreamObserver<GraphBatchChunk> requestStream;

  private StreamObserver<GraphBatchChunk> request;

  GraphBatchLoadStream(final long timeoutMs) {
    this(timeoutMs, READY_WAIT_TIMEOUT_MS);
  }

  GraphBatchLoadStream(final long timeoutMs, final long readyWaitTimeoutMs) {
    this.timeoutMs = timeoutMs;
    this.readyWaitTimeoutMs = readyWaitTimeoutMs;
  }

  /**
   * The observer handed to the stub. Captures the call so {@link #send} can see its readiness, and wakes any
   * parked sender both when the call becomes ready and when it terminates.
   */
  ClientResponseObserver<GraphBatchChunk, GraphBatchResult> responseObserver() {
    return new ClientResponseObserver<>() {
      @Override
      public void beforeStart(final ClientCallStreamObserver<GraphBatchChunk> stream) {
        requestStream = stream;
        stream.setOnReadyHandler(() -> {
          synchronized (readyLock) {
            readyLock.notifyAll();
          }
        });
      }

      @Override
      public void onNext(final GraphBatchResult value) {
        resultRef.set(value);
      }

      @Override
      public void onError(final Throwable t) {
        errorRef.set(t);
        done.countDown();
        // A sender parked waiting for readiness will never be woken by a call that just died.
        synchronized (readyLock) {
          readyLock.notifyAll();
        }
      }

      @Override
      public void onCompleted() {
        done.countDown();
        synchronized (readyLock) {
          readyLock.notifyAll();
        }
      }
    };
  }

  void start(final StreamObserver<GraphBatchChunk> request) {
    this.request = request;
  }

  /**
   * Pushes one chunk, waiting first for the call to be able to carry it.
   */
  void send(final GraphBatchChunk chunk) {
    awaitReady();
    request.onNext(chunk);
  }

  private void awaitReady() {
    final ClientCallStreamObserver<GraphBatchChunk> stream = requestStream;
    if (stream == null)
      // beforeStart always runs before the stub returns the request observer, so this only happens if the call
      // never started at all; let onNext fail with the real reason rather than block on a readiness that is
      // never coming.
      return;

    final long deadline = System.currentTimeMillis() + readyWaitTimeoutMs;
    synchronized (readyLock) {
      while (!stream.isReady()) {
        failIfTerminated();

        final long remaining = deadline - System.currentTimeMillis();
        if (remaining <= 0)
          throw new TimeoutException(
              "Graph batch load timed out after " + readyWaitTimeoutMs + "ms waiting for the server to consume "
                  + "the records already sent");
        try {
          readyLock.wait(remaining);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RemoteException("Interrupted while sending a graph batch load chunk", e);
        }
      }
    }
  }

  /**
   * Cancels the call without raising anything of its own, for an unwind that is already carrying a failure.
   */
  void cancelQuietly(final String reason) {
    final ClientCallStreamObserver<GraphBatchChunk> stream = requestStream;
    if (stream == null)
      return;
    try {
      stream.cancel(reason, null);
    } catch (final RuntimeException ignored) {
      // The call was already terminated: nothing left to release.
    }
  }

  /**
   * Ends the stream and waits for the server's totals.
   */
  GraphBatchResult complete() {
    // A call the server has already failed is done: half-closing it would raise gRPC's own "call already
    // closed" on top of the real failure and bury it. Report the failure the load actually died of instead.
    failIfTerminated();

    request.onCompleted();

    final boolean finished;
    try {
      finished = done.await(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RemoteException("Interrupted while waiting for the graph batch load to complete", e);
    }

    if (!finished) {
      // Cancelling releases the server-side batch slot instead of leaving the load hanging on to it.
      final ClientCallStreamObserver<GraphBatchChunk> stream = requestStream;
      if (stream != null)
        stream.cancel("graph batch load timed out on the client", null);
      throw new TimeoutException("Graph batch load timed out after " + timeoutMs + "ms waiting for the server to "
          + "complete. Raise the timeout with withTimeout() if the load legitimately takes longer");
    }

    failIfTerminated();

    final GraphBatchResult result = resultRef.get();
    if (result == null)
      throw new RemoteException("The server completed the graph batch load without reporting its result");

    return result;
  }

  /**
   * What the server had already committed when the load failed, or null if the load did not fail or the server
   * reported nothing. Survives the exception, so a caller can see how much of the load is durable.
   */
  GraphBatchResult getPartialResult() {
    return partialResultRef.get();
  }

  /**
   * Rethrows the failure the call ended with, if it ended with one, having first recorded whatever the server
   * managed to commit before failing.
   */
  private void failIfTerminated() {
    final Throwable error = errorRef.get();
    if (error == null)
      return;

    // The exception keeps the type the rest of this client maps gRPC statuses onto, so a caller can switch on it
    // exactly as for any other operation. The counters do not go in the message: they belong to the batch, which
    // reports them through getResult() whether the load succeeded or died half-way.
    partialResultRef.set(readPartialResult(error));
    throw GrpcClientErrorMapper.toException(error);
  }

  /**
   * Reads the partial-commit counters the server puts on the trailers of a failed load. A load that failed is
   * not a load that rolled back: the batch commits incrementally, so whatever it had already flushed is durable
   * and the caller has to be able to tell how much that was.
   */
  private static GraphBatchResult readPartialResult(final Throwable error) {
    if (!(error instanceof StatusRuntimeException statusError))
      return null;

    final Metadata trailers = statusError.getTrailers();
    if (trailers == null)
      return null;

    return trailers.get(GraphBatchProtocol.RESULT_TRAILER);
  }
}
