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
        awaitBlocked(thread);
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

  /** Waits until {@code thread} is parked on a monitor - the ConcurrentHashMap bin the claim is holding. */
  private static void awaitBlocked(final Thread thread) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + 10_000;
    while (System.currentTimeMillis() < deadline) {
      final Thread.State state = thread.getState();
      if (state == Thread.State.BLOCKED || state == Thread.State.WAITING || state == Thread.State.TERMINATED)
        return;
      Thread.sleep(1);
    }
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
