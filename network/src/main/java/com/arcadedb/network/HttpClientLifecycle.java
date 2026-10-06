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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.net.http.HttpClient;
import java.time.Duration;

/**
 * JDK17 bridge to the {@link HttpClient} lifecycle methods ({@code shutdown()}, {@code shutdownNow()},
 * {@code awaitTermination(Duration)}, {@code isTerminated()}, {@code close()}) that only exist since Java 21.
 * <p>
 * The java17 branch compiles against the Java 17 API, so these are looked up once, reflectively. On a Java 21+
 * runtime every call behaves exactly like the method it stands for. On a Java 17 runtime there is no way to release
 * a client at all: the shutdown calls do nothing, and {@link #isTerminated} / {@link #awaitTermination} answer
 * {@code true} so that callers do not keep retrying or warning about a release that cannot happen. The client's
 * selector thread is a daemon and goes away once the client is no longer reachable.
 */
public final class HttpClientLifecycle {
  private static final MethodHandle SHUTDOWN          = find("shutdown", MethodType.methodType(void.class));
  private static final MethodHandle SHUTDOWN_NOW      = find("shutdownNow", MethodType.methodType(void.class));
  private static final MethodHandle AWAIT_TERMINATION = find("awaitTermination",
      MethodType.methodType(boolean.class, Duration.class));
  private static final MethodHandle IS_TERMINATED     = find("isTerminated", MethodType.methodType(boolean.class));
  private static final MethodHandle CLOSE             = find("close", MethodType.methodType(void.class));

  private HttpClientLifecycle() {
  }

  /** Whether the running JDK can release an {@link HttpClient} (Java 21+). */
  public static boolean isSupported() {
    return SHUTDOWN_NOW != null;
  }

  public static void shutdown(final HttpClient client) {
    invokeVoid(SHUTDOWN, client);
  }

  public static void shutdownNow(final HttpClient client) {
    invokeVoid(SHUTDOWN_NOW, client);
  }

  public static void close(final HttpClient client) {
    invokeVoid(CLOSE, client);
  }

  public static boolean isTerminated(final HttpClient client) {
    if (IS_TERMINATED == null)
      return true;
    try {
      return (boolean) IS_TERMINATED.invokeExact(client);
    } catch (final RuntimeException | Error e) {
      throw e;
    } catch (final Throwable e) {
      throw new IllegalStateException(e);
    }
  }

  public static boolean awaitTermination(final HttpClient client, final Duration duration) throws InterruptedException {
    if (AWAIT_TERMINATION == null)
      return true;
    try {
      return (boolean) AWAIT_TERMINATION.invokeExact(client, duration);
    } catch (final InterruptedException | RuntimeException | Error e) {
      throw e;
    } catch (final Throwable e) {
      throw new IllegalStateException(e);
    }
  }

  private static void invokeVoid(final MethodHandle handle, final HttpClient client) {
    if (handle == null || client == null)
      return;
    try {
      handle.invokeExact(client);
    } catch (final RuntimeException | Error e) {
      throw e;
    } catch (final Throwable e) {
      throw new IllegalStateException(e);
    }
  }

  private static MethodHandle find(final String name, final MethodType type) {
    try {
      return MethodHandles.publicLookup().findVirtual(HttpClient.class, name, type);
    } catch (final NoSuchMethodException | IllegalAccessException e) {
      return null;
    }
  }
}
