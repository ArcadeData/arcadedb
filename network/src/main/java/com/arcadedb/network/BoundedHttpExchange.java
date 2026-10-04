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
package com.arcadedb.network;

import java.io.IOException;
import java.io.InputStream;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.LongSupplier;

/**
 * Bounds on an outbound {@link HttpClient} exchange that hold on every JDK (issues #8325, #8473).
 * <p>
 * {@code HttpRequest.Builder.timeout} cannot be relied on for them, because what it covers depends on the JDK. On
 * JDK 21-25 it stops at the response HEADERS: a peer that sends headers and then stalls inside its body leaves a
 * {@code BodyHandlers.ofString()} read with no bound at all. On JDK 26+ it covers the body too, which on a streamed
 * answer caps the total length of a stream that is working.
 * <ul>
 * <li>{@link #send} bounds a whole exchange: the whole body for a buffered handler, the headers alone for a streaming
 * one such as {@code ofInputStream()};</li>
 * <li>{@link #sendWhileProgressing} bounds it the same way, but counting from the last time the exchange moved forward
 * rather than from the send, for a request whose own body can take longer than the bound to publish;</li>
 * <li>{@link #silenceBounded} bounds each read of a streamed body, so the stream may last as long as the peer keeps
 * sending.</li>
 * </ul>
 * Lives in {@code network} rather than in {@code server} so the Java remote client can use it too; the server's HA
 * forwards reach it through {@code LeaderDial.sendBounded}.
 */
public final class BoundedHttpExchange {

  /** The shortest bound a caller can ask for: a zero or negative deadline must not become "no deadline". */
  public static final long MIN_DEADLINE_MS = 1L;

  /** The longest {@link #sendWhileProgressing} waits between two samples of its progress counter. */
  private static final long MAX_PROGRESS_SAMPLE_MS = 1_000L;

  private BoundedHttpExchange() {
  }

  /**
   * Sends {@code request} and waits for the answer for at most {@code deadlineMs}, then cancels the exchange and throws
   * {@link HttpTimeoutException}. Otherwise it behaves like {@link HttpClient#send}: the answer, or the exception the
   * exchange failed with, of its own type ({@link java.net.http.HttpConnectTimeoutException},
   * {@link java.net.ConnectException}, ...), so a caller's existing catch arms keep telling those apart.
   * <p>
   * Awaiting the future applies one bound on every JDK, and what it covers is decided by the body handler: the whole
   * body for a buffered handler, which completes the future only once the body is read; the headers alone for a
   * streaming one such as {@code ofInputStream()}, whose body the caller then has to bound itself, for instance with
   * {@link #silenceBounded}. On HTTP/1.1 it covers the publishing of the request body as well, whatever the handler:
   * the JDK client hands back no answer before the body has been published whole (issue #8719), which is what
   * {@link #sendWhileProgressing} is for.
   * <p>
   * The future is cancelled on the deadline and on an interrupt, which aborts the exchange and closes its connection
   * rather than leaving it to the peer.
   *
   * @param deadlineMs the longest to wait, floored at {@link #MIN_DEADLINE_MS}
   */
  public static <T> HttpResponse<T> send(final HttpClient client, final HttpRequest request,
      final HttpResponse.BodyHandler<T> handler, final long deadlineMs) throws IOException, InterruptedException {
    return send(client, request, handler, deadlineMs, null);
  }

  /**
   * As {@link #send(HttpClient, HttpRequest, HttpResponse.BodyHandler, long)}, with the message the
   * {@link HttpTimeoutException} thrown on the deadline carries.
   *
   * @param timeoutMessage the message of the exception thrown on the deadline, or {@code null} for the default one,
   *                       which names the peer and the deadline
   */
  public static <T> HttpResponse<T> send(final HttpClient client, final HttpRequest request,
      final HttpResponse.BodyHandler<T> handler, final long deadlineMs, final String timeoutMessage)
      throws IOException, InterruptedException {
    return await(client.sendAsync(request, handler), request, Math.max(deadlineMs, MIN_DEADLINE_MS), null,
        timeoutMessage);
  }

  /**
   * As {@link #send(HttpClient, HttpRequest, HttpResponse.BodyHandler, long, String)}, for a request whose body can
   * take longer than {@code deadlineMs} to publish (issue #8719): the deadline counts from the last time
   * {@code progress} changed rather than from the send, so an exchange whose upload keeps moving is waited on for as
   * long as it moves, and one that stays still for {@code deadlineMs} is given up on as {@link #send} gives up on it.
   * <p>
   * It exists because on HTTP/1.1 the JDK client completes the future only once the request body has been published
   * whole, even when the peer has already answered: with {@link #send} the deadline caps the length of the upload too.
   * Once the upload has ended the counter stops, so the deadline then bounds the wait for the answer, as with
   * {@link #send}. It is never shorter than {@link #send}'s: the window only ever restarts.
   * <p>
   * The counter is sampled every quarter of the deadline, and no less often than every second, so a stillness is given
   * up on between one deadline and one deadline plus one sample interval after the last change.
   *
   * @param progress a counter that changes whenever the exchange moves forward - for an upload, the bytes read from the
   *                 body being published. Read from the calling thread while another thread advances it, so it must be
   *                 safe to read concurrently.
   */
  public static <T> HttpResponse<T> sendWhileProgressing(final HttpClient client, final HttpRequest request,
      final HttpResponse.BodyHandler<T> handler, final long deadlineMs, final LongSupplier progress,
      final String timeoutMessage) throws IOException, InterruptedException {
    return await(client.sendAsync(request, handler), request, Math.max(deadlineMs, MIN_DEADLINE_MS), progress,
        timeoutMessage);
  }

  /**
   * Waits on {@code pending} until it completes, or until {@code deadline} ms have passed - since the send, or since
   * {@code progress} last changed when there is one - and then cancels it and throws {@link HttpTimeoutException}.
   */
  private static <T> HttpResponse<T> await(final CompletableFuture<HttpResponse<T>> pending, final HttpRequest request,
      final long deadline, final LongSupplier progress, final String timeoutMessage)
      throws IOException, InterruptedException {
    try {
      if (progress == null)
        return pending.get(deadline, TimeUnit.MILLISECONDS);

      final long deadlineNanos = TimeUnit.MILLISECONDS.toNanos(deadline);
      final long sampleNanos = TimeUnit.MILLISECONDS.toNanos(
          Math.max(MIN_DEADLINE_MS, Math.min(deadline / 4, MAX_PROGRESS_SAMPLE_MS)));
      long seen = progress.getAsLong();
      long stillSince = System.nanoTime();
      while (true) {
        final long left = deadlineNanos - (System.nanoTime() - stillSince);
        try {
          return pending.get(Math.max(0L, Math.min(left, sampleNanos)), TimeUnit.NANOSECONDS);
        } catch (final TimeoutException e) {
          final long now = progress.getAsLong();
          if (now != seen) {
            seen = now;
            stillSince = System.nanoTime();
          } else if (left <= sampleNanos)
            // This wait ran to the end of the window, and the counter did not move during it
            throw e;
        }
      }
    } catch (final TimeoutException e) {
      if (!pending.cancel(true))
        // The answer completed between the wait expiring and the cancel. Handed over rather than dropped: dropping it
        // would leave a streaming handler's body, and the connection under it, open with nobody to close it.
        return completed(pending, request);
      final HttpTimeoutException timeout = new HttpTimeoutException(timeoutMessage != null ?
          timeoutMessage :
          progress != null ?
              "no complete answer from " + request.uri().getAuthority() + " and no progress for " + deadline + " ms" :
              "no complete answer from " + request.uri().getAuthority() + " within " + deadline + " ms");
      timeout.initCause(e);
      throw timeout;
    } catch (final InterruptedException e) {
      if (!pending.cancel(true))
        closeBodyOf(pending);
      throw e;
    } catch (final ExecutionException e) {
      throw unwrap(e, request);
    }
  }

  /**
   * Wraps a streamed body so that no single read of it waits longer than {@code silenceMs}; see
   * {@link SilenceBoundedInputStream}. The reads are timed on one daemon thread shared by the whole JVM, which only
   * ever closes a stream that went silent.
   *
   * @param silenceMs the longest the peer may stay silent while a read waits, floored at {@link #MIN_DEADLINE_MS}
   */
  public static SilenceBoundedInputStream silenceBounded(final InputStream body, final long silenceMs) {
    return new SilenceBoundedInputStream(body, Math.max(silenceMs, MIN_DEADLINE_MS), ReadTimer::schedule);
  }

  /**
   * Closes the body of an answer that completed but will never be read, so its connection is released. Never throws:
   * it runs on the way out with an interrupt, which must reach the caller as it is. A future that completed with a
   * failure has no body, and {@code getNow} would rethrow that failure in place of the interrupt.
   */
  public static void closeBodyOf(final CompletableFuture<? extends HttpResponse<?>> done) {
    if (done.isCompletedExceptionally())
      return;
    final HttpResponse<?> response = done.getNow(null);
    if (response != null && response.body() instanceof AutoCloseable body)
      try {
        body.close();
      } catch (final Exception ignored) {
        // Best effort: the caller is already on its way out with the interrupt
      }
  }

  /** The outcome of a future that is already done, failures unwrapped as {@link #send} unwraps them. */
  private static <T> HttpResponse<T> completed(final CompletableFuture<HttpResponse<T>> done, final HttpRequest request)
      throws IOException {
    try {
      return done.get();
    } catch (final ExecutionException e) {
      throw unwrap(e, request);
    } catch (final InterruptedException e) {
      // Not reachable on a completed future; kept so the interrupt is never swallowed.
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while reading the answer of " + request.method() + " " + request.uri(), e);
    }
  }

  /** The exception the exchange failed with, of its own type, so the callers' catch arms can tell them apart. */
  private static IOException unwrap(final ExecutionException e, final HttpRequest request) {
    final Throwable cause = e.getCause();
    if (cause instanceof IOException io)
      return io;
    if (cause instanceof RuntimeException runtime)
      throw runtime;
    if (cause instanceof Error error)
      throw error;
    return new IOException("Error sending " + request.method() + " " + request.uri(), cause);
  }

  /**
   * The timer {@link #silenceBounded} arms before every read. One daemon thread for the JVM, created on first use: its
   * only task is closing a stream whose peer went silent, which returns at once, so it can never fall behind the way a
   * pool running real work could. Cancelled tasks are removed from the queue straight away, so a stream read in many
   * small chunks does not pile up dead entries for the length of the budget. The thread ends after a minute with nothing
   * scheduled, so an application that embeds the remote client does not keep it for good.
   */
  private static final class ReadTimer {
    private static final ScheduledThreadPoolExecutor EXECUTOR;

    static {
      EXECUTOR = new ScheduledThreadPoolExecutor(1, task -> {
        final Thread thread = new Thread(task, "arcadedb-http-read-timer");
        thread.setDaemon(true);
        return thread;
      });
      EXECUTOR.setRemoveOnCancelPolicy(true);
      EXECUTOR.setKeepAliveTime(60, TimeUnit.SECONDS);
      EXECUTOR.allowCoreThreadTimeOut(true);
    }

    static Runnable schedule(final Runnable task, final long delayMs) {
      final ScheduledFuture<?> scheduled = EXECUTOR.schedule(task, delayMs, TimeUnit.MILLISECONDS);
      return () -> scheduled.cancel(false);
    }
  }
}
