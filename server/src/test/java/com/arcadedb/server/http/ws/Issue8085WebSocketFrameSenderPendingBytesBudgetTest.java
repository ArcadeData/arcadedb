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
package com.arcadedb.server.http.ws;

import io.undertow.websockets.core.WebSocketChannel;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression tests for issue #8085: three of the four {@code /ws} senders - {@code WebSocketInsertProtocol}'s
 * answers and {@code WebSocketReceiveListener}'s acknowledgements/errors - charged nothing against any
 * pending-bytes budget, so a client that never reads them pinned one small queued frame per request on the
 * server's heap forever. Only the change-stream fan-out of {@link WebSocketEventBus} was bounded
 * (issue #6762).
 * <p>
 * Driven directly against {@link WebSocketFrameSender#sendBudgeted}, the one choke point all four senders now
 * share for this accounting, rather than through a real socket: the property under test is the reserve-then-
 * compare arithmetic and the eviction it triggers, not Undertow's own frame writing.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8085WebSocketFrameSenderPendingBytesBudgetTest {

  /**
   * The defect itself, reproduced with a single frame: a budget breach must be caught BEFORE the frame is
   * handed to Undertow, not after. A mocked channel has no real socket to write to, so if this reached
   * {@code WebSockets.sendText} it would throw - the fact that it does not (asserted via
   * {@code assertThatCode(...).doesNotThrowAnyException()}) is itself proof the send was refused early.
   */
  @Test
  void aFrameThatAloneExceedsTheBudgetIsRefusedAndTheChannelIsClosedInsteadOfQueued() throws Exception {
    final WebSocketChannel channel = channelWithRealAttributes();

    final long cap = 32;
    final String oversizedFrame = "x".repeat(64);

    final boolean[] accepted = new boolean[1];
    assertThatCode(() -> accepted[0] = WebSocketFrameSender.sendBudgeted(channel, oversizedFrame, cap))
        .as("a refusal must never reach Undertow's own send path on a channel with nothing behind it")
        .doesNotThrowAnyException();

    assertThat(accepted[0]).isFalse();
    verify(channel).close();
  }

  /**
   * Many frames that are each individually small - the shape of the actual attack: a flood of tiny malformed
   * requests, each answered with a ~130 byte error frame - must still accumulate against ONE connection-wide
   * counter and trip the same eviction once their sum crosses the cap.
   * <p>
   * The 80 bytes already outstanding stand in for two such frames still in flight - queued with Undertow but
   * not yet reported done, exactly what a peer that has stopped reading looks like to this accounting (the
   * same shape {@code Issue6762WebSocketEventBusTest.neverCompletingSubscription()} uses for the event bus's
   * own budget). {@link WebSocketFrameSender#sendBudgeted} releases a reservation the instant a send THROWS
   * synchronously, so driving this by repeating a call that throws against a bare mock would release each
   * reservation immediately and never accumulate anything.
   */
  @Test
  void manySmallFramesAccumulateAndTripTheSameEvictionOnceTheirSumExceedsTheBudget() throws Exception {
    final WebSocketChannel channel = channelWithRealAttributes();
    channel.setAttribute(WebSocketFrameSender.PENDING_BYTES, new AtomicLong(80));

    final long cap = 100;
    final String smallFrame = "x".repeat(40);

    // The third frame (80 + 40 = 120) crosses the cap: refused before any write is attempted, so no exception,
    // and the connection is closed instead of accumulating a further queued frame.
    final boolean[] accepted = new boolean[1];
    assertThatCode(() -> accepted[0] = WebSocketFrameSender.sendBudgeted(channel, smallFrame, cap))
        .doesNotThrowAnyException();

    assertThat(accepted[0]).isFalse();
    verify(channel).close();
  }

  /**
   * A send that throws SYNCHRONOUSLY (never reaching Undertow's callback at all) must give its reservation
   * back rather than leak it for the rest of the connection's life - the asymmetry code review flagged
   * against {@code WebSocketEventBus.publish()}'s own belt-and-suspenders guard around the same kind of call.
   * A mocked channel with no real socket is exactly this failure mode: {@code WebSockets.sendText} throws
   * before ever registering the callback.
   */
  @Test
  void aSynchronousSendFailureReleasesItsReservationInsteadOfLeakingIt() throws Exception {
    final WebSocketChannel channel = channelWithRealAttributes();
    final long cap = 1_000;
    final String frame = "x".repeat(40);

    assertThatThrownBy(() -> WebSocketFrameSender.sendBudgeted(channel, frame, cap))
        .as("the underlying Undertow failure must still surface to the caller, not be swallowed")
        .isInstanceOf(RuntimeException.class);

    final AtomicLong pending = (AtomicLong) channel.getAttribute(WebSocketFrameSender.PENDING_BYTES);
    assertThat(pending.get())
        .as("the reservation must be given back, not leaked, when the send throws synchronously")
        .isZero();
  }

  /** The cap is opt-out, the same contract {@code eventBusMaxPendingBytes} already documents. */
  @Test
  void aZeroCapDisablesTheEviction() throws Exception {
    final WebSocketChannel channel = channelWithRealAttributes();

    final String hugeFrame = "x".repeat(1_000_000);

    try {
      WebSocketFrameSender.sendBudgeted(channel, hugeFrame, 0);
    } catch (final RuntimeException ignoredUnwritableFrame) {
      // The write itself fails against a mock with no socket; the cap check ran first and let it through.
    }

    verify(channel, never()).close();
  }

  /** A closed channel is refused up front: no point reserving bytes towards a peer that is already gone. */
  @Test
  void aClosedChannelIsRefusedWithoutTouchingTheBudget() throws Exception {
    final WebSocketChannel channel = mock(WebSocketChannel.class);
    when(channel.isOpen()).thenReturn(false);

    final boolean accepted = WebSocketFrameSender.sendBudgeted(channel, "irrelevant", 1024);

    assertThat(accepted).isFalse();
    verify(channel, never()).close();
  }

  /**
   * {@link WebSocketFrameSender#utf8Length}, moved here from {@code WebSocketEventBus} so both the event-bus
   * budget and this one charge non-ASCII payloads the same way, must still count the real wire cost rather
   * than {@code String.length()}'s char count.
   */
  @Test
  void utf8LengthCountsBytesNotCharacters() {
    assertThat(WebSocketFrameSender.utf8Length("abc")).isEqualTo(3);
    // One emoji: 1 Java char count (as a surrogate pair, 2 chars) but 4 UTF-8 bytes.
    assertThat(WebSocketFrameSender.utf8Length("😀")).isEqualTo(4);
  }

  /**
   * A channel with a real attribute map behind {@code getAttribute}/{@code setAttribute}, so the pending-bytes
   * counter {@link WebSocketFrameSender#sendBudgeted} installs on its first call is still there - and still
   * accumulating - on its second, exactly as it is on a real {@code WebSocketChannel} (whose attribute storage
   * this test does not otherwise exercise). A bare {@code mock(WebSocketChannel.class)} answers every
   * {@code getAttribute} with {@code null} and discards every {@code setAttribute}, which would silently install
   * a fresh, empty counter on every call and make the budget impossible to ever cross.
   */
  private static WebSocketChannel channelWithRealAttributes() {
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
}
