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
import io.undertow.websockets.core.WebSocketChannel;
import org.junit.jupiter.api.Test;
import org.xnio.XnioWorker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
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
public class Issue7909InFlightStartHoldsTheFrameBudgetTest extends BaseGraphServerTest {

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
