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

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Thread-safe wrapper around a gRPC {@link StreamObserver} that serializes every outbound call and
 * enforces the StreamObserver contract: at most one terminal call ({@code onError} or
 * {@code onCompleted}) is delegated, and no {@code onNext} is delegated after a terminal call.
 *
 * <p>gRPC observers are not thread-safe. A streaming RPC handler that dispatches work to executor
 * threads, or that lets a terminal call race the call's cancellation handler, can otherwise
 * interleave terminal calls on the same observer. That produces {@code IllegalStateException}
 * ("call already closed"), duplicate/dropped responses, or a client hang.
 *
 * <p>All calls funnel through a single monitor. Calls that arrive after the stream is already
 * terminated are dropped instead of delegated, so a late or duplicate terminal call is a no-op. If
 * the underlying call was concurrently cancelled or closed by the transport, the
 * {@code IllegalStateException} thrown by the delegate is caught and the wrapper marks itself
 * terminated so no further calls are attempted.
 */
final class SynchronizedStreamObserver<T> implements StreamObserver<T> {
  private final StreamObserver<T> delegate;
  private final AtomicBoolean     completed = new AtomicBoolean(false);
  private final Object            lock      = new Object();

  SynchronizedStreamObserver(final StreamObserver<T> delegate) {
    this.delegate = delegate;
  }

  @Override
  public void onNext(final T value) {
    synchronized (lock) {
      if (completed.get())
        return;
      try {
        delegate.onNext(value);
      } catch (final IllegalStateException | StatusRuntimeException failed) {
        // The write failed: usually the transport already cancelled/closed the call. Best-effort
        // terminate the stream so a still-live call that rejected this one message (e.g. a
        // RESOURCE_EXHAUSTED) does not leave the client hanging without a terminal. compareAndSet
        // skips the terminal if a concurrent markTerminated (cancel handler) already flipped the
        // flag; if the call is already closed, the onError itself is a no-op and is swallowed.
        if (completed.compareAndSet(false, true)) {
          try {
            delegate.onError(failed);
          } catch (final IllegalStateException | StatusRuntimeException ignore) {
            // Call already closed: nothing more to deliver.
          }
        }
      }
    }
  }

  @Override
  public void onError(final Throwable t) {
    synchronized (lock) {
      if (!completed.compareAndSet(false, true))
        return;
      try {
        delegate.onError(t);
      } catch (final IllegalStateException | StatusRuntimeException ignore) {
        // Call already closed/cancelled by the transport: nothing more to deliver.
      }
    }
  }

  @Override
  public void onCompleted() {
    synchronized (lock) {
      if (!completed.compareAndSet(false, true))
        return;
      try {
        delegate.onCompleted();
      } catch (final IllegalStateException | StatusRuntimeException ignore) {
        // Call already closed/cancelled by the transport: nothing more to deliver.
      }
    }
  }

  /**
   * Flags the stream as terminated without delegating any call. Intended for the gRPC call's cancel
   * handler: once the transport has cancelled the call, no further {@code onNext}/{@code onError}/
   * {@code onCompleted} may be delivered, so subsequent calls become no-ops.
   *
   * <p>This is deliberately lock-free (a plain flag flip, not guarded by {@code lock}) so the
   * transport cancel handler never blocks behind an in-flight {@code delegate.onNext()}. A call
   * already past the {@code completed} gate races at worst into {@code delegate.onNext} after close,
   * which throws and is caught.
   */
  void markTerminated() {
    completed.set(true);
  }

  boolean isTerminated() {
    return completed.get();
  }
}
