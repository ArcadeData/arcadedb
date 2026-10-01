/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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

import com.arcadedb.server.ha.raft.SecurityCatchUp.Attempt;
import com.arcadedb.server.ha.raft.SecurityCatchUp.Outcome;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The once-per-start latch is released only by the request that took it (issue #8168).
 * <p>
 * After the #8034 fix {@code settle} released the latch for every attempt that asked nobody, whichever request that
 * attempt belonged to. A leader-change attempt finishing with "nobody to ask" therefore handed back the latch a
 * snapshot-install request had taken over in the meantime, and the next leader change submitted a second request
 * for the same node start alongside the snapshot-install's own.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8168SecurityCatchUpLatchOwnerTest {

  /** The interleaving from the report: A (leader change) is in flight, B (snapshot install) takes over, A settles. */
  @Test
  void aStaleAttemptThatAskedNobodyDoesNotReleaseALatchTakenOverByALaterRequest() {
    try (final SecurityCatchUp catchUp = new SecurityCatchUp()) {
      final long a = catchUp.tryTakeRequest();
      assertThat(a).as("the first leader observed takes the latch").isNotZero();

      final long b = catchUp.takeOverRequest();

      assertThat(catchUp.settle(a, Outcome.NOBODY_TO_ASK)).isTrue();
      assertThat(catchUp.hasRequestedSinceStart()).as("B has not run yet, so the latch is still B's").isTrue();
      assertThat(catchUp.tryTakeRequest()).as("no second once-per-start request may be taken beside B's").isZero();

      catchUp.settle(b, Outcome.NOBODY_TO_ASK);
      assertThat(catchUp.hasRequestedSinceStart()).as("B's own outcome still releases it").isFalse();
      assertThat(catchUp.tryTakeRequest()).isNotZero();
    }
  }

  @Test
  void theOwnerThatAskedNobodyStillReleasesItsOwnLatch() {
    try (final SecurityCatchUp catchUp = new SecurityCatchUp()) {
      final long token = catchUp.tryTakeRequest();

      catchUp.settle(token, Outcome.NOBODY_TO_ASK);

      assertThat(catchUp.hasRequestedSinceStart()).isFalse();
    }
  }

  /** A request that could not even be queued releases its own hold, and nobody else's. */
  @Test
  void aRejectedRequestReleasesOnlyItsOwnHold() {
    try (final SecurityCatchUp catchUp = new SecurityCatchUp()) {
      final long dropped = catchUp.tryTakeRequest();
      catchUp.onRejected(new Attempt(dropped, () -> {
      }));
      assertThat(catchUp.hasRequestedSinceStart()).as("the dropped request's own hold is released").isFalse();

      final long displaced = catchUp.tryTakeRequest();
      final long owner = catchUp.takeOverRequest();
      catchUp.onRejected(new Attempt(displaced, () -> {
      }));
      assertThat(catchUp.hasRequestedSinceStart()).as("the rejection of a displaced request leaves the owner's hold")
          .isTrue();

      catchUp.onRejected(new Attempt(owner, () -> {
      }));
      assertThat(catchUp.hasRequestedSinceStart()).isFalse();
    }
  }

  @Test
  void aTaskThatIsNotAnAttemptIsIgnoredByTheRejectionHandler() {
    try (final SecurityCatchUp catchUp = new SecurityCatchUp()) {
      catchUp.tryTakeRequest();

      catchUp.onRejected(() -> {
      });

      assertThat(catchUp.hasRequestedSinceStart()).isTrue();
    }
  }

  @Test
  void aSpentRequestStaysSpentWhenAStaleAttemptSettles() {
    try (final SecurityCatchUp catchUp = new SecurityCatchUp()) {
      final long stale = catchUp.tryTakeRequest();
      final long current = catchUp.takeOverRequest();

      catchUp.settle(current, Outcome.ASKED);
      catchUp.settle(stale, Outcome.NOBODY_TO_ASK);

      assertThat(catchUp.hasRequestedSinceStart()).as("an answered request stays spent").isTrue();
    }
  }
}
