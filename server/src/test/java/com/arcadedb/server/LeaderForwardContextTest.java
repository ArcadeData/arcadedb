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
package com.arcadedb.server;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The marker that stops a request being redirected to the leader twice (issue #6191). Two properties carry
 * the whole safety of it, and both are about the pool of HTTP worker threads it lives on: a request that was
 * not forwarded must not inherit a previous request's marker, and one thread's marker must not be visible to
 * another.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LeaderForwardContextTest {

  @AfterEach
  void clearContext() {
    LeaderForwardContext.clear();
  }

  @Test
  void nothingIsMarkedByDefault() {
    assertThat(LeaderForwardContext.isAlreadyForwarded()).isFalse();
  }

  @Test
  void aMarkedRequestIsReported() {
    LeaderForwardContext.markAlreadyForwarded();

    assertThat(LeaderForwardContext.isAlreadyForwarded()).isTrue();
  }

  /** The finally block in the request loop: the next request served by this pooled thread starts clean. */
  @Test
  void clearingReleasesTheMarkerForTheNextRequestOnThisThread() {
    LeaderForwardContext.markAlreadyForwarded();
    LeaderForwardContext.clear();

    assertThat(LeaderForwardContext.isAlreadyForwarded()).isFalse();
  }

  /** A forwarded request on one worker thread must not make a concurrent, unrelated one refuse to forward. */
  @Test
  void theMarkerDoesNotEscapeToAnotherThread() throws Exception {
    LeaderForwardContext.markAlreadyForwarded();

    final AtomicBoolean seenElsewhere = new AtomicBoolean(true);
    final Thread other = new Thread(() -> seenElsewhere.set(LeaderForwardContext.isAlreadyForwarded()));
    other.start();
    other.join();

    assertThat(seenElsewhere).isFalse();
    assertThat(LeaderForwardContext.isAlreadyForwarded()).as("and this thread keeps its own marker").isTrue();
  }

  // Issue #7603: the refusal of a second hop has two causes, told apart by which node the forwarding peer meant.

  @Test
  void aPeerThatMeantThisNodeReportsALeadershipChange() {
    LeaderForwardContext.markAlreadyForwarded("peer-1");

    assertThat(LeaderForwardContext.intendedLeaderId()).isEqualTo("peer-1");
    assertThat(LeaderForwardContext.classifyRefusal("peer-1")).isEqualTo(LeaderForwardContext.Refusal.LEADERSHIP_MOVED);
  }

  @Test
  void aPeerThatMeantAnotherNodeReportsAnAddressThatDoesNotIdentifyTheLeader() {
    LeaderForwardContext.markAlreadyForwarded("peer-2");

    assertThat(LeaderForwardContext.classifyRefusal("peer-1"))
        .isEqualTo(LeaderForwardContext.Refusal.ADDRESS_DOES_NOT_IDENTIFY_LEADER);
  }

  @Test
  void withoutBothIdsTheCauseIsUndetermined() {
    LeaderForwardContext.markAlreadyForwarded();
    assertThat(LeaderForwardContext.classifyRefusal("peer-1")).isEqualTo(LeaderForwardContext.Refusal.UNDETERMINED);

    LeaderForwardContext.markAlreadyForwarded("  ");
    assertThat(LeaderForwardContext.classifyRefusal("peer-1")).isEqualTo(LeaderForwardContext.Refusal.UNDETERMINED);

    LeaderForwardContext.markAlreadyForwarded("peer-1");
    assertThat(LeaderForwardContext.classifyRefusal(null)).isEqualTo(LeaderForwardContext.Refusal.UNDETERMINED);
  }

  /** A pooled worker thread must not carry the previous request's intended leader into the next one. */
  @Test
  void clearingAlsoReleasesTheIntendedLeader() {
    LeaderForwardContext.markAlreadyForwarded("peer-1");
    LeaderForwardContext.clear();

    assertThat(LeaderForwardContext.intendedLeaderId()).isNull();
    LeaderForwardContext.markAlreadyForwarded();
    assertThat(LeaderForwardContext.intendedLeaderId()).as("a later marker without an id must not inherit one").isNull();
  }

  @Test
  void aLeaderIdIsSentOnlyWhenItDidNotChangeWhileTheAddressWasResolved() {
    assertThat(LeaderForwardContext.stableLeaderId("peer-1", "peer-1")).isEqualTo("peer-1");
    assertThat(LeaderForwardContext.stableLeaderId("peer-1", "peer-2")).isNull();
    assertThat(LeaderForwardContext.stableLeaderId("peer-1", null)).isNull();
    assertThat(LeaderForwardContext.stableLeaderId(null, null)).isNull();
  }
}
