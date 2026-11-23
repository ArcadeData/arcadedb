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
import com.arcadedb.server.http.ws.WebSocketEventBus;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.websockets.core.WebSocketChannel;
import org.junit.jupiter.api.Test;
import org.xnio.XnioWorker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The {@code start} frame of a {@code /ws} insert session, driven against the session manager and the protocol
 * directly with the worker held instead of raced.
 * <p>
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
 * <p>
 * Issue #7909, the half of the answer an end-to-end test cannot pin down: a {@code start} frame accepted for
 * execution but NOT yet applied already holds the larger text-frame budget.
 * <p>
 * That half is what lets a client pipeline its first {@code chunk} behind {@code start} without waiting for
 * {@code started}: the chunk's size is decided on the I/O thread, and the session it would be decided from is
 * registered on a worker. Over a real socket the two orderings cannot be told apart - the worker may well have
 * registered the session before the chunk's budget is read, in which case the session half of the answer admits
 * the frame and the in-flight half is never exercised (CodeRabbit on PR #7936). So the worker is held here
 * instead of raced: the queued frame is captured rather than run, which is exactly the state the grant exists
 * to cover, and released afterwards to show the grant is given back.
 * <p>
 * Driven through {@code dispatch} and {@code hasInsertFrameBudget} - the two methods the receive listener
 * actually calls - rather than through a hook added to {@code WebSocketInsertSessionManager.start} for the
 * test's benefit: the property is about what the protocol answers between the two, and holding the worker is
 * enough to ask it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class InsertSessionStartTest extends BaseGraphServerTest {

  // Regression: #7471 a start racing the channel close must not open a session nothing deregisters
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

  // Regression: #7471 a start racing the channel close must not open a session nothing deregisters
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

  // Regression: #7471 a start racing the channel close must not open a session nothing deregisters
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

  // Regression: #7471 a start racing the channel close must not open a session nothing deregisters
  /**
   * The narrower window the first fix left behind, found in review: {@code closeChannelSessions} running BETWEEN
   * the channel claim and the registration of the session would clear the claim, find nothing in {@code sessions}
   * to roll back - the session is not registered yet - and leave {@code start} to go on and open it anyway. The
   * close fires once per connection, so that session was then orphaned until the idle sweep, which is the exact
   * symptom this test class exists for, on microseconds instead of on an arbitrarily delayed frame.
   * <p>
   * Driven through the manager's post-claim hook rather than by timing (issue #7975): the close runs to completion
   * in the window on every run, so the claim re-read is the only thing that can notice it.
   */
  @Test
  void aCloseLandingBetweenTheClaimAndTheRegistrationLeavesNothingOrphaned() {
    final WebSocketInsertSessionManager manager = getServer(0).getHttpServer().getInsertSessionManager();
    final UUID channelId = UUID.randomUUID();
    final WebSocketChannel channel = openChannel();

    final AtomicBoolean closed = new AtomicBoolean();
    // The claim's map bin is already released when the hook runs, so the close can run on this very thread.
    manager.afterChannelClaimForTesting = () -> {
      manager.closeChannelSessions(channel, channelId);
      closed.set(true);
    };

    try {
      assertThatThrownBy(() -> manager.start(rootUser(), channel, channelId, getDatabaseName(), null, options(), null))
          .as("a start whose connection closed underneath it must refuse rather than return an untracked session")
          .isInstanceOf(IllegalStateException.class)
          // The re-read of the claim is what refuses it, not the channel check inside the claim: the close ran
          // after the claim was taken, so the claim is the only place it could have been noticed.
          .hasMessageContaining("closed while the insert session was being opened");
    } finally {
      manager.afterChannelClaimForTesting = null;
    }

    assertThat(closed.get()).as("the close must have run inside the window").isTrue();
    assertThat(manager.getOpenSessionCount())
        .as("no session may be left registered, and none may be left holding a transaction").isZero();
  }

  // Regression: #7471 a start racing the channel close must not open a session nothing deregisters
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

  // Regression: #7909 a start accepted but not yet applied already holds the insert frame budget
  @Test
  void aStartAcceptedButNotYetAppliedAlreadyHoldsTheInsertBudget() {
    final WebSocketInsertProtocol protocol = getServer(0).getHttpServer().getInsertProtocol();
    final List<Runnable> heldByTheWorker = new ArrayList<>();
    final WebSocketChannel channel = channelWhoseWorkerDefers(heldByTheWorker);

    assertThat(protocol.hasInsertFrameBudget(channel))
        .as("a connection that has sent nothing is on the control budget").isFalse();

    protocol.dispatch(channel, "start", startFrame(getDatabaseName()));

    assertThat(heldByTheWorker).as("the frame must be queued, not applied, for this test to mean anything").hasSize(1);
    assertThat(protocol.hasInsertFrameBudget(channel))
        .as("the next frame's size is decided NOW, while the start is still queued").isTrue();

    runHeldFrame(heldByTheWorker);

    assertThat(protocol.hasInsertFrameBudget(channel))
        .as("the start produced a session, so the budget passes to it with no gap").isTrue();
    assertThat(getServer(0).getHttpServer().getInsertSessionManager()
        .hasSessionOnChannel((UUID) channel.getAttribute(WebSocketEventBus.CHANNEL_ID))).isTrue();

    // And it goes back down when that session ends, however it ends - here, with the connection.
    protocol.onChannelClosed(channel, (UUID) channel.getAttribute(WebSocketEventBus.CHANNEL_ID));
    runHeldFrame(heldByTheWorker);

    assertThat(protocol.hasInsertFrameBudget(channel)).isFalse();
  }

  // Regression: #7909 a start accepted but not yet applied already holds the insert frame budget
  /**
   * The same window when the start is going to be REFUSED. The grant is still held while the frame waits - the
   * I/O thread cannot know yet - and is given back the instant the worker decides, which is what stops a refused
   * start from buying the connection a 256x budget for the rest of its life.
   */
  @Test
  void aStartAboutToBeRefusedHoldsTheBudgetOnlyUntilItIsRefused() {
    final WebSocketInsertProtocol protocol = getServer(0).getHttpServer().getInsertProtocol();
    final List<Runnable> heldByTheWorker = new ArrayList<>();
    final WebSocketChannel channel = channelWhoseWorkerDefers(heldByTheWorker);

    protocol.dispatch(channel, "start", startFrame("no-such-database-7909"));
    assertThat(protocol.hasInsertFrameBudget(channel)).isTrue();

    runHeldFrame(heldByTheWorker);

    assertThat(protocol.hasInsertFrameBudget(channel))
        .as("the start produced no session, so the grant is gone with it").isFalse();
    assertThat(getServer(0).getHttpServer().getInsertSessionManager()
        .hasSessionOnChannel((UUID) channel.getAttribute(WebSocketEventBus.CHANNEL_ID))).isFalse();
  }

  /**
   * Runs the frame the mocked worker is holding. The write of the answering frame is expected to fail - a mocked
   * channel has no socket to put one on - and is irrelevant here: what is being asked is what the budget says
   * before and after the frame runs, and the production path swallows a failed send the same way
   * ({@code FrameQueue.drain} logs and carries on).
   */
  private static void runHeldFrame(final List<Runnable> heldByTheWorker) {
    assertThat(heldByTheWorker).isNotEmpty();
    try {
      heldByTheWorker.remove(0).run();
    } catch (final RuntimeException ignoreTheUnwritableAnswer) {
      // See the javadoc.
    }
  }

  private static JSONObject startFrame(final String databaseName) {
    return new JSONObject().put("action", "start").put("database", databaseName)
        .put("options", new JSONObject().put("targetType", "Person"));
  }

  /**
   * A channel with a real attribute map and a worker that CAPTURES what it is given instead of running it, so
   * the window between "the frame was accepted" and "the frame was applied" can be held open for as long as the
   * test needs it.
   */
  private WebSocketChannel channelWhoseWorkerDefers(final List<Runnable> captured) {
    final WebSocketChannel channel = mock(WebSocketChannel.class);
    final Map<String, Object> attributes = new ConcurrentHashMap<>();

    when(channel.isOpen()).thenReturn(true);
    when(channel.getAttribute(anyString())).thenAnswer(invocation -> attributes.get(invocation.getArgument(0)));
    when(channel.setAttribute(anyString(), any())).thenAnswer(invocation -> {
      attributes.put(invocation.getArgument(0), invocation.getArgument(1));
      return true;
    });

    final XnioWorker worker = mock(XnioWorker.class);
    doAnswer(invocation -> {
      captured.add(invocation.getArgument(0));
      return null;
    }).when(worker).execute(any(Runnable.class));
    when(channel.getWorker()).thenReturn(worker);

    attributes.put(WebSocketEventBus.CHANNEL_ID, UUID.randomUUID());
    attributes.put(WebSocketEventBus.USER, getServer(0).getSecurity().getUser("root"));
    return channel;
  }

  @Override
  protected void populateDatabase() {
  }
}
