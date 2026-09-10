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

import com.arcadedb.log.LogManager;
import com.arcadedb.server.ServerControlPlane;
import io.grpc.StatusException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import java.util.function.Function;
import java.util.logging.Level;

/**
 * The server-streaming counterpart of {@link GrpcUnaryCall}, for the control-plane operations that
 * report progress while they run - restore backup, restore database, import database (issue #7308).
 * <p>
 * It plays the part the SSE writer plays on the HTTP side: it is the sink a
 * {@link ServerControlPlane.ProgressListener} writes into, and it terminates the call exactly once,
 * either with a final message carrying {@code completed} or with an error status.
 * <p>
 * <b>Failures never travel as a message.</b> A restore that fails halfway ends the stream with an
 * error status, so a client that read a completed message has a result that succeeded - unlike the
 * SSE contract, where an {@code error} frame arrives inside a 200 response because the HTTP status
 * line was already written by then. gRPC does not have that constraint: the status is a trailer.
 * <p>
 * <b>Every method is synchronized</b> because more than one thread reports progress: an import
 * samples {@code ImporterContext} on a timer thread while the importer logs on the calling thread,
 * and a parallel restore logs from its worker pool (issue #6086). A {@link StreamObserver} is not
 * safe for concurrent use, so the lock is what keeps two progress messages from interleaving into a
 * corrupt frame.
 *
 * @param <T> the progress message type of this stream
 */
final class GrpcProgressStream<T> implements ServerControlPlane.ProgressListener {
  /** The body of a streaming handler: runs the operation, or throws. */
  interface Body {
    /** @return the operation's final report, merged into the completion message, or null when it has none. */
    Object run(ServerControlPlane.ProgressListener listener) throws Exception;
  }

  /** Builds a progress message out of the importer counters, for a stream that reports them. */
  interface CounterMessage<T> {
    T build(long parsed, long vertices, long edges);
  }

  private final StreamObserver<T>   resp;
  private final Function<String, T> lineMessage;
  private final CounterMessage<T>   counterMessage;
  private       boolean             terminated = false;

  private GrpcProgressStream(final StreamObserver<T> resp, final Function<String, T> lineMessage,
      final CounterMessage<T> counterMessage) {
    this.resp = resp;
    this.lineMessage = lineMessage;
    this.counterMessage = counterMessage;
  }

  /**
   * Runs {@code body} against a progress stream and terminates {@code resp} exactly once.
   *
   * @param resp           the observer of the server-streaming call
   * @param lineMessage    builds a non-final progress message out of one log line
   * @param counterMessage builds a progress message out of the importer counters, or null for a
   *                       stream with no counters to report - a restore
   * @param completion     builds the final message from whatever the body returned
   * @param body           the operation, which reports its progress to the listener it is handed
   * @param errorMapper    maps a failure of the body to the status the client receives
   */
  static <T> void stream(final StreamObserver<T> resp, final Function<String, T> lineMessage,
      final CounterMessage<T> counterMessage, final Function<Object, T> completion, final Body body,
      final Function<Exception, StatusException> errorMapper) {
    final GrpcProgressStream<T> stream = new GrpcProgressStream<>(resp, lineMessage, counterMessage);
    try {
      final Object report = body.run(stream);
      stream.complete(completion.apply(report));
    } catch (final Exception e) {
      stream.fail(errorMapper.apply(e));
    }
  }

  @Override
  public synchronized void onProgress(final String message) {
    send(lineMessage.apply(message));
  }

  @Override
  public synchronized void onImportCounters(final long parsed, final long vertices, final long edges) {
    if (counterMessage != null)
      send(counterMessage.build(parsed, vertices, edges));
  }

  /**
   * Sends one progress message, unless the call is already over. A client that cancelled or went
   * away must not turn into a failure of the operation itself: the restore is running server-side
   * and has no interruption point, so the only sane response is to stop reporting.
   * <p>
   * The catch is <b>deliberately wider than the {@link #isCancelled()} check above it</b>, and covers
   * a transport failure as well as a cancellation. That is not an oversight of the two cases: what
   * either one means here is the same thing, which is that this call can no longer carry messages.
   * Neither one can undo the work already done, neither can stop the work still running, and failing
   * the operation because its progress could not be reported would turn a delivery problem into data
   * loss. So both stop the reporting and leave the operation alone, and the reason they are told
   * apart at all is the log line. Once this fires, {@code terminated} keeps {@link #complete} and
   * {@link #fail} from writing to a call gRPC has already closed, which would throw again.
   */
  synchronized void send(final T message) {
    if (terminated || isCancelled())
      return;
    try {
      resp.onNext(message);
    } catch (final RuntimeException e) {
      // The call died under us - cancelled, or a transport failure. Stop writing to it; the
      // operation itself carries on to its end.
      terminated = true;
      LogManager.instance().log(this, Level.FINE, "Progress stream can no longer be written to: %s", e.getMessage());
    }
  }

  private synchronized void complete(final T finalMessage) {
    if (terminated)
      return;
    try {
      if (!isCancelled())
        resp.onNext(finalMessage);
      terminated = true;
      resp.onCompleted();
    } catch (final RuntimeException e) {
      terminated = true;
      LogManager.instance().log(this, Level.FINE,
          "Progress stream already closed when its completion was delivered (client cancelled?): %s", e.getMessage());
    }
  }

  private synchronized void fail(final StatusException status) {
    if (terminated)
      return;
    terminated = true;
    try {
      resp.onError(status);
    } catch (final RuntimeException e) {
      LogManager.instance().log(this, Level.FINE,
          "Progress stream already closed when its failure was delivered (client cancelled?): %s", e.getMessage());
    }
  }

  private boolean isCancelled() {
    return resp instanceof ServerCallStreamObserver<T> observer && observer.isCancelled();
  }
}
