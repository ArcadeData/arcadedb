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
package com.arcadedb.server.http.ws.insert;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.websockets.core.WebSocketChannel;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #7471, item 1: a {@code start} frame racing the channel close opened a session nothing would ever
 * deregister.
 * <p>
 * The close handler hands its cleanup to the Undertow worker pool, and frames are drained by a SEPARATE worker task
 * ({@code FrameQueue.drain}); nothing orders the two. {@code start} guarded against server shutdown and against a
 * second session on the same channel, but never asked whether the channel it was opening the session on was still
 * there - so a {@code start} still queued when the connection died could be drained AFTER
 * {@code closeChannelSessions} had already run and gone. The session was created on a dead channel and held its
 * transaction open until the idle sweep reclaimed it: one leaked open transaction per abrupt disconnect, and an
 * abrupt disconnect is the normal case for a network client.
 * <p>
 * Fixed by deciding the channel claim and the "is this channel still usable" question TOGETHER, in one
 * {@code ConcurrentHashMap.compute} on the channel key that {@code closeChannelSessions} takes as well, with the
 * close marking the channel before it clears the registry. The two orderings are then the only two outcomes: the
 * close finds a session and rolls it back, or the start finds the marker and is refused.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue7471InsertSessionChannelCloseRaceTest extends BaseGraphServerTest {

  @Test
  void aStartDrainedAfterTheChannelCloseIsRefusedInsteadOfLeakingATransaction() {
    final WebSocketInsertSessionManager manager = getServer(0).getHttpServer().getInsertSessionManager();
    final UUID channelId = UUID.randomUUID();
    // Still reporting itself open, which is the courteous-close case: the receive listener's onClose reaches the
    // cleanup while Undertow is completing the closing handshake. An isOpen() check alone would miss this.
    final WebSocketChannel channel = openChannel();

    manager.closeChannelSessions(channel, channelId);

    assertThatThrownBy(() -> manager.start(rootUser(), channel, channelId, getDatabaseName(), null, options(), null))
        .as("a start drained after its connection was cleaned up must not open a session")
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("closing");

    assertThat(manager.getOpenSessionCount()).isZero();
  }

  @Test
  void aStartOnAChannelThatIsSimplyGoneIsRefusedToo() {
    final WebSocketInsertSessionManager manager = getServer(0).getHttpServer().getInsertSessionManager();
    final UUID channelId = UUID.randomUUID();
    // Died before any close handler ran, so there is no marker to find - only a channel that is no longer open.
    final WebSocketChannel channel = openChannel();
    when(channel.isOpen()).thenReturn(false);

    assertThatThrownBy(() -> manager.start(rootUser(), channel, channelId, getDatabaseName(), null, options(), null))
        .isInstanceOf(IllegalStateException.class);

    assertThat(manager.getOpenSessionCount()).isZero();
  }

  /**
   * The control that keeps the two refusals above meaningful: a start on a live channel still opens a session, and
   * the close that follows still rolls it back and deregisters it. Without this, "refuse everything" would pass.
   */
  @Test
  void aStartOnALiveChannelStillOpensASessionTheCloseThenRollsBack() {
    final WebSocketInsertSessionManager manager = getServer(0).getHttpServer().getInsertSessionManager();
    final UUID channelId = UUID.randomUUID();
    final WebSocketChannel channel = openChannel();

    final WebSocketInsertSession session =
        manager.start(rootUser(), channel, channelId, getDatabaseName(), null, options(), null);

    assertThat(session).isNotNull();
    assertThat(manager.getOpenSessionCount()).isEqualTo(1);

    manager.closeChannelSessions(channel, channelId);

    assertThat(manager.getOpenSessionCount()).isZero();
    assertThat(session.isClosed()).isTrue();

    // And the channel is now marked, so a frame still in that connection's queue cannot re-open one behind it.
    assertThatThrownBy(() -> manager.start(rootUser(), channel, channelId, getDatabaseName(), null, options(), null))
        .isInstanceOf(IllegalStateException.class);
    assertThat(manager.getOpenSessionCount()).isZero();
  }

  /**
   * The narrower window the first fix left behind, found in review: {@code closeChannelSessions} running BETWEEN
   * the channel claim and the registration of the session would clear the claim, find nothing in {@code sessions}
   * to roll back - the session is not registered yet - and leave {@code start} to go on and open it anyway. The
   * close fires once per connection, so that session was then orphaned until the idle sweep, which is the exact
   * symptom this test class exists for, on microseconds instead of on an arbitrarily delayed frame.
   * <p>
   * Driven deterministically rather than by repetition: the close is launched from inside the claim itself and
   * waited for until it is BLOCKED on the very map key {@code start} is holding, so it is guaranteed to run in the
   * window the instant the claim is released.
   */
  @Test
  void aCloseLandingBetweenTheClaimAndTheRegistrationLeavesNothingOrphaned() throws Exception {
    final WebSocketInsertSessionManager manager = getServer(0).getHttpServer().getInsertSessionManager();
    final UUID channelId = UUID.randomUUID();
    final WebSocketChannel channel = openChannel();

    final AtomicReference<Thread> closer = new AtomicReference<>();
    // getAttribute is called exactly once by start(), from inside the byChannel claim. Launching the close there
    // and waiting for it to block puts it first in line for the key the claim is about to release.
    when(channel.getAttribute(anyString())).thenAnswer(invocation -> {
      if (closer.get() == null) {
        final Thread thread = new Thread(() -> manager.closeChannelSessions(channel, channelId), "issue7471-closer");
        closer.set(thread);
        thread.start();
        awaitBlockedOnThisThread(thread);
      }
      return null;
    });

    assertThatThrownBy(() -> manager.start(rootUser(), channel, channelId, getDatabaseName(), null, options(), null))
        .as("a start whose connection closed underneath it must refuse rather than return an untracked session")
        .isInstanceOf(IllegalStateException.class);

    closer.get().join(30_000);

    assertThat(manager.getOpenSessionCount())
        .as("no session may be left registered, and none may be left holding a transaction").isZero();
  }

  /**
   * The third ordering, also found in review: the close landing AFTER the session is registered but BEFORE its
   * transaction is begun. {@code cancel()} would then mark the session closed with no transaction to roll back,
   * and {@code begin()} - which took no lock and never asked whether the session was still open - went on to open
   * one regardless. Every later {@code cancel()} returns immediately on the {@code closed} flag, so nothing would
   * ever roll that transaction back, and the session was already out of {@code sessions}, so neither the idle
   * sweep nor server shutdown could reach it either.
   * <p>
   * {@code begin()} now runs under the session lock behind {@code requireOpen()}, so this ordering refuses the
   * start instead of leaving a transaction nobody owns.
   */
  @Test
  void aSessionCancelledBeforeItsTransactionWasBegunRefusesToBeginOne() {
    final WebSocketInsertSessionManager manager = getServer(0).getHttpServer().getInsertSessionManager();
    final UUID channelId = UUID.randomUUID();
    final WebSocketChannel channel = openChannel();

    final WebSocketInsertSession session =
        manager.start(rootUser(), channel, channelId, getDatabaseName(), null, options(), null);

    // What the close hook does to a session in that window: it is registered, so cancel() reaches it, and its
    // transaction does not exist yet, so there is nothing for cancel() to roll back.
    assertThat(session.cancel()).isTrue();
    assertThat(session.isClosed()).isTrue();

    assertThatThrownBy(session::begin)
        .as("a second begin() would open a transaction no later cancel() could roll back, on a session already "
            + "out of the registry the idle sweep reads")
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("closed");

    manager.closeChannelSessions(channel, channelId);
    assertThat(manager.getOpenSessionCount()).isZero();
  }

  /** Waits until {@code thread} is parked on a monitor - the ConcurrentHashMap bin the claim is holding. */
  /**
   * Waits until {@code thread} is blocked on a monitor THIS thread owns, which is the only state that puts it next
   * in line for the per-key critical section the caller is holding.
   * <p>
   * The owner check is the whole point, and the reason this used to be flaky on CI while passing locally (found
   * while it was failing the {@code unit-tests} lane on main): a bare {@code getState() == BLOCKED} is also true
   * of a thread blocked on a CLASS INITIALIZATION monitor, which is exactly where a freshly started thread sits on
   * a cold JVM the first time it walks into {@code closeChannelSessions}. Accepting that let the caller release the
   * key and race ahead of a closer that had not reached it yet, and the test then failed on the assertion rather
   * than on the wait. {@code WAITING} was accepted for the same reason and is likewise not evidence of contention
   * on our key.
   * <p>
   * {@code TERMINATED} stays accepted: a closer that already ran to completion cannot be waited for any longer,
   * and the orderings the test asserts hold either way. The wait is bounded and FAILS rather than falling through,
   * so a window that is never entered is reported as such instead of turning the assertion below into a coin flip.
   */
  private static void awaitBlockedOnThisThread(final Thread thread) throws InterruptedException {
    final ThreadMXBean threads = ManagementFactory.getThreadMXBean();
    final long owner = Thread.currentThread().threadId();

    // A short wait EXPECTED TO SUCCEED long before the bound: generous on purpose, since a wider bound cannot turn
    // a passing run red, and only a window that never opens at all reaches the failure below.
    final long deadline = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < deadline) {
      if (thread.getState() == Thread.State.TERMINATED)
        return;

      final ThreadInfo info = threads.getThreadInfo(thread.threadId());
      if (info != null && info.getThreadState() == Thread.State.BLOCKED && info.getLockOwnerId() == owner)
        return;

      Thread.sleep(1);
    }

    throw new AssertionError("the closer thread never blocked on the key this thread is holding, so the window "
        + "this test drives was never entered (it was " + thread.getState() + ")");
  }

  /** A mocked channel whose attribute map is real, so the close marker behaves as it does on a live connection. */
  private static WebSocketChannel openChannel() {
    final WebSocketChannel channel = mock(WebSocketChannel.class);
    final Map<String, Object> attributes = new ConcurrentHashMap<>();
    when(channel.isOpen()).thenReturn(true);
    when(channel.getAttribute(anyString())).thenAnswer(invocation -> attributes.get(invocation.getArgument(0)));
    when(channel.setAttribute(anyString(), any())).thenAnswer(invocation -> {
      attributes.put(invocation.getArgument(0), invocation.getArgument(1));
      return true;
    });
    return channel;
  }

  private ServerSecurityUser rootUser() {
    return getServer(0).getSecurity().getUser("root");
  }

  private static JSONObject options() {
    return new JSONObject().put("targetType", "Person");
  }

  @Override
  protected void populateDatabase() {
  }
}
