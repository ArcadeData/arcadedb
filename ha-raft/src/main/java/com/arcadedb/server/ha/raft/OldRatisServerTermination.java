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
package com.arcadedb.server.ha.raft;

import com.arcadedb.log.LogManager;
import org.apache.ratis.thirdparty.io.grpc.Server;
import org.apache.ratis.util.LifeCycle;

import java.lang.reflect.Field;
import java.lang.reflect.InaccessibleObjectException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.logging.Level;

/**
 * What the in-place Ratis restart ({@code RaftHAServer.restartRatis}) checks about the OLD server before it starts the
 * new one (issue #8900). Without these checks, the new server could start while the old one still answered the leader.
 * <ul>
 *   <li><b>A close already running on another thread.</b> Ratis closes the server itself from the JVM-pause monitor
 *       thread ({@code RaftServerProxy.handleJvmPause}). Its {@code close()} is then a no-op for the restart, but it
 *       still ends with {@code pauseMonitor.stop()}, which INTERRUPTS the monitor thread while that thread waits for
 *       the old gRPC server to terminate. That interrupt made the #8898 close give up
 *       ({@code Interrupted shutdown GrpcServerProtocolService}). {@link #awaitCloseInProgress} lets that close finish
 *       first.</li>
 *   <li><b>A gRPC server that did not terminate.</b> {@code GrpcServicesImpl.closeImpl} swallows an interrupted
 *       {@code awaitTermination}, and the proxy reports CLOSED all the same. {@link #terminateGrpcServers} reads the
 *       old servers and shuts each one down again with a bound. The restart fails, loudly, when one is still
 *       running.</li>
 * </ul>
 */
final class OldRatisServerTermination {

  /** How long the restart waits for a close another thread started. A Ratis close normally takes milliseconds. */
  static final long CLOSE_IN_PROGRESS_WAIT_MS = 30_000L;

  /** How long the restart waits for each old gRPC server to terminate after it shuts it down again. */
  static final long GRPC_TERMINATION_WAIT_MS = 10_000L;

  /** How long the restart waits for the old division's asynchronous close before it only logs it. */
  static final long DIVISION_CLOSE_WAIT_MS = 10_000L;

  private static final long POLL_MS = 50L;

  /** Cached handle on {@code GrpcServicesImpl.servers}; null until resolved or when it cannot be resolved. */
  private static volatile Field serversField;
  private static volatile boolean serversFieldUnavailable;
  private static volatile boolean otherRpcClassReported;

  private OldRatisServerTermination() {
  }

  /**
   * Waits up to {@code timeoutMs} while {@code state} reports {@link LifeCycle.State#CLOSING}: a close is already
   * running on another thread, and calling {@code close()} now would interrupt it. Ends early once {@code cancelled}
   * answers true (a shutdown was requested).
   *
   * @return the last state read, which is still CLOSING if the wait timed out or was interrupted
   */
  static LifeCycle.State awaitCloseInProgress(final Supplier<LifeCycle.State> state, final long timeoutMs,
      final BooleanSupplier cancelled) {
    return awaitWhile(state, s -> s == LifeCycle.State.CLOSING, timeoutMs, cancelled);
  }

  /**
   * Waits up to {@code timeoutMs} for {@code state} to report {@link LifeCycle.State#CLOSED}. Used for the old
   * division, whose close the proxy runs asynchronously.
   *
   * @return the last state read, which is not CLOSED if the wait timed out or was interrupted
   */
  static LifeCycle.State awaitClosed(final Supplier<LifeCycle.State> state, final long timeoutMs,
      final BooleanSupplier cancelled) {
    return awaitWhile(state, s -> s != LifeCycle.State.CLOSED, timeoutMs, cancelled);
  }

  private static LifeCycle.State awaitWhile(final Supplier<LifeCycle.State> state, final Predicate<LifeCycle.State> keepWaiting,
      final long timeoutMs, final BooleanSupplier cancelled) {
    final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    LifeCycle.State current = state.get();
    while (keepWaiting.test(current) && System.nanoTime() < deadline && !cancelled.getAsBoolean()) {
      try {
        Thread.sleep(POLL_MS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return current;
      }
      current = state.get();
    }
    return current;
  }

  /**
   * Shuts down again every gRPC server in {@code servers} that is not terminated yet, and waits up to
   * {@code timeoutMs} for each one to terminate.
   *
   * @return the names of the servers still running when the wait ended; empty when all of them terminated
   */
  static List<String> terminateGrpcServers(final Map<String, Server> servers, final long timeoutMs) {
    final List<String> running = new ArrayList<>();
    if (servers.isEmpty())
      return running;

    // A server whose close is still running on another thread (the bounded wait for it ran out) is shut down again
    // too: shutdownNow() is idempotent, and the point is that nothing of the old instance outlives this method.
    for (final Server server : servers.values())
      if (!server.isTerminated())
        server.shutdownNow();

    final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    boolean interrupted = false;
    for (final Map.Entry<String, Server> entry : servers.entrySet()) {
      final Server server = entry.getValue();
      if (!interrupted && !server.isTerminated())
        try {
          server.awaitTermination(Math.max(0L, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        } catch (final InterruptedException e) {
          interrupted = true;
        }
      if (!server.isTerminated())
        running.add(entry.getKey());
    }
    if (interrupted)
      Thread.currentThread().interrupt();
    return running;
  }

  /**
   * The gRPC servers of a Ratis {@code GrpcServicesImpl} ({@code rpc} is a {@code RaftServerRpc}; typed as Object so
   * the reflective read can be tested without one), read reflectively because Ratis does not publish them. Empty for
   * any other RPC implementation, or when the field is not there (a Ratis upgrade renamed it): the restart then
   * proceeds without the check, as it did before, and says so once at WARNING.
   */
  static Map<String, Server> serversOf(final Object rpc) {
    if (rpc == null || serversFieldUnavailable)
      return Map.of();
    try {
      Field field = serversField;
      if (field == null) {
        field = findField(rpc.getClass(), "servers");
        if (field == null)
          return Map.of(); // a different RPC implementation: nothing to check
        field.setAccessible(true);
        serversField = field;
      } else if (!field.getDeclaringClass().isInstance(rpc)) {
        // A skipped check must not be silent: say so once, at the level an operator reads.
        final boolean first = !otherRpcClassReported;
        otherRpcClassReported = true;
        LogManager.instance().log(OldRatisServerTermination.class, first ? Level.WARNING : Level.FINE,
            "Ratis RPC %s is not the %s the gRPC check reads; an in-place Ratis restart will not verify that its "
                + "gRPC services terminated", rpc.getClass().getName(), field.getDeclaringClass().getName());
        return Map.of();
      }
      final Object value = field.get(rpc);
      if (!(value instanceof Map<?, ?> map))
        return Map.of();
      final Map<String, Server> servers = new LinkedHashMap<>();
      for (final Map.Entry<?, ?> entry : map.entrySet())
        if (entry.getValue() instanceof Server server)
          servers.put(String.valueOf(entry.getKey()), server);
      return servers;
    } catch (final InaccessibleObjectException e) {
      return disableCheck(e);
    } catch (final RuntimeException e) {
      // Not a resolution failure: skip the check this once, keep it for the next restart.
      LogManager.instance().log(OldRatisServerTermination.class, Level.FINE,
          "Cannot read the gRPC servers of the Ratis RPC layer this time: %s", e.toString());
      return Map.of();
    } catch (final ReflectiveOperationException e) {
      return disableCheck(e);
    }
  }

  /** Latches the check off for good: the field cannot be resolved or opened on this Ratis build. */
  private static Map<String, Server> disableCheck(final Exception e) {
    serversFieldUnavailable = true;
    LogManager.instance().log(OldRatisServerTermination.class, Level.WARNING,
        "Cannot read the gRPC servers of the Ratis RPC layer (%s); an in-place Ratis restart will not verify that the "
            + "old server's gRPC services terminated", e.toString());
    return Map.of();
  }

  /** Forgets the cached field and the latch. Tests only: both are process-wide. */
  static void resetForTesting() {
    serversField = null;
    serversFieldUnavailable = false;
    otherRpcClassReported = false;
  }

  private static Field findField(final Class<?> type, final String name) {
    for (Class<?> c = type; c != null; c = c.getSuperclass())
      try {
        return c.getDeclaredField(name);
      } catch (final NoSuchFieldException ignored) {
        // keep looking in the superclass
      }
    return null;
  }
}
