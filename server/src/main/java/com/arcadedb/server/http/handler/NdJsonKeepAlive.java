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
package com.arcadedb.server.http.handler;

import io.undertow.server.HttpServerExchange;
import org.xnio.XnioExecutor;
import org.xnio.XnioIoThread;
import org.xnio.XnioWorker;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Drives {@link NdJsonResultStream#keepAlive} for one streamed response (issue #8565): every {@code intervalMs} it
 * gives the stream the chance to say it is still alive, and stops when the stream is closed or this object is.
 * <p>
 * Adds no pool and no thread of its own. The timer is scheduled on the connection's XNIO IO thread, where the write
 * watchdog of {@link WriteBoundedOutputStream} is scheduled too, but the write itself is handed to the XNIO worker
 * pool, the one serving the request: a write can block on a client that stopped reading and the IO thread must never
 * wait on a socket. The write is bounded by the same watchdog as every other write of the response, so a stuck client
 * releases the worker thread after {@code arcadedb.server.httpStreamingWriteTimeout}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class NdJsonKeepAlive implements AutoCloseable {
  private final NdJsonResultStream stream;
  private final XnioIoThread       ioThread;
  private final XnioWorker         worker;
  private final long               intervalMs;

  private volatile boolean         stopped;
  private volatile XnioExecutor.Key timer;

  private NdJsonKeepAlive(final HttpServerExchange exchange, final NdJsonResultStream stream, final long intervalMs) {
    this.stream = stream;
    this.ioThread = exchange.getIoThread();
    this.worker = exchange.getConnection().getWorker();
    this.intervalMs = intervalMs;
  }

  /**
   * Starts the keep-alive of {@code stream}.
   *
   * @param intervalMs how long the stream may be silent, see {@code arcadedb.server.httpStreamingKeepAliveInterval};
   *                   not positive starts nothing
   */
  static NdJsonKeepAlive start(final HttpServerExchange exchange, final NdJsonResultStream stream, final long intervalMs) {
    final NdJsonKeepAlive keepAlive = new NdJsonKeepAlive(exchange, stream, intervalMs);
    if (intervalMs > 0)
      keepAlive.schedule(intervalMs);
    return keepAlive;
  }

  private void schedule(final long delayMs) {
    if (!stopped)
      timer = ioThread.executeAfter(this::dispatch, delayMs, TimeUnit.MILLISECONDS);
  }

  private void dispatch() {
    if (stopped)
      return;
    try {
      worker.execute(this::tick);
    } catch (final RejectedExecutionException e) {
      // The server is shutting down
      stopped = true;
    }
  }

  private void tick() {
    if (stopped)
      return;
    // Checks again when the stream could next have been idle for the interval, not a full interval later
    final long next = stream.keepAlive(intervalMs);
    if (next > 0)
      schedule(next);
    else
      stopped = true;
  }

  @Override
  public void close() {
    stopped = true;
    final XnioExecutor.Key key = timer;
    if (key != null)
      key.remove();
  }
}
