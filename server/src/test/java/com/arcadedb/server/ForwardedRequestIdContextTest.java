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

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The request-scoped client {@code X-Request-Id} a follower relays on its SQL write forward to the leader (issue
 * #8323). It lives on the pooled HTTP worker threads, so a request must never inherit another request's id, and
 * two forwards taken by one request must never share one leader-side cache key.
 */
class ForwardedRequestIdContextTest {

  @AfterEach
  void clearContext() {
    ForwardedRequestIdContext.clear();
  }

  @Test
  void aRequestThatPublishedNoIdRelaysNone() {
    assertThat(ForwardedRequestIdContext.requestId()).isNull();
    assertThat(ForwardedRequestIdContext.nextForwardOrdinal()).isZero();
  }

  @Test
  void aBlankIdIsNotRelayed() {
    ForwardedRequestIdContext.set("   ");
    assertThat(ForwardedRequestIdContext.nextForwardOrdinal()).isZero();

    ForwardedRequestIdContext.set(null);
    assertThat(ForwardedRequestIdContext.nextForwardOrdinal()).isZero();
    assertThat(ForwardedRequestIdContext.requestId()).isNull();
  }

  @Test
  void theClientsIdIsRelayedUnchanged() {
    ForwardedRequestIdContext.set("req-8323");

    assertThat(ForwardedRequestIdContext.requestId()).isEqualTo("req-8323");
  }

  /**
   * Two forwards of the same statement in one request would share one key on the leader under the bare id, and the
   * second would be answered from the first one's cache entry instead of running.
   */
  @Test
  void everyForwardInTheSameRequestGetsItsOwnOrdinal() {
    ForwardedRequestIdContext.set("req-8323");

    assertThat(ForwardedRequestIdContext.nextForwardOrdinal()).isEqualTo(1);
    assertThat(ForwardedRequestIdContext.nextForwardOrdinal()).isEqualTo(2);
    assertThat(ForwardedRequestIdContext.nextForwardOrdinal()).isEqualTo(3);
    assertThat(ForwardedRequestIdContext.requestId()).isEqualTo("req-8323");
  }

  /** A retry of the same request, served on the same pooled thread, maps its forwards back to the same ordinals. */
  @Test
  void publishingAgainRestartsTheOrdinals() {
    ForwardedRequestIdContext.set("req-8323");
    ForwardedRequestIdContext.nextForwardOrdinal();
    ForwardedRequestIdContext.nextForwardOrdinal();

    ForwardedRequestIdContext.set("req-8323");

    assertThat(ForwardedRequestIdContext.nextForwardOrdinal()).isEqualTo(1);
  }

  @Test
  void restartingTheOrdinalsKeepsTheId() {
    ForwardedRequestIdContext.set("req-8323");
    ForwardedRequestIdContext.nextForwardOrdinal();
    ForwardedRequestIdContext.nextForwardOrdinal();

    ForwardedRequestIdContext.restartOrdinals();

    assertThat(ForwardedRequestIdContext.nextForwardOrdinal()).isEqualTo(1);
    assertThat(ForwardedRequestIdContext.requestId()).isEqualTo("req-8323");
  }

  /** The finally block in the request loop: the next request served by this pooled thread starts clean. */
  @Test
  void clearingReleasesTheIdForTheNextRequestOnThisThread() {
    ForwardedRequestIdContext.set("req-8323");
    ForwardedRequestIdContext.clear();

    assertThat(ForwardedRequestIdContext.requestId()).isNull();
    assertThat(ForwardedRequestIdContext.nextForwardOrdinal()).isZero();
  }

  @Test
  void anIdIsNotVisibleToAnotherThread() throws InterruptedException {
    ForwardedRequestIdContext.set("req-8323");

    final AtomicReference<String> seenElsewhere = new AtomicReference<>("unset");
    final Thread other = new Thread(() -> seenElsewhere.set(ForwardedRequestIdContext.requestId()));
    other.start();
    other.join();

    assertThat(seenElsewhere.get()).isNull();
    assertThat(ForwardedRequestIdContext.requestId()).isEqualTo("req-8323");
  }

  /** Only an ordinal a forward could have sent is honored; the first forward sends none. */
  @Test
  void onlyAnOrdinalAForwardCouldHaveSentIsParsed() {
    assertThat(ForwardedRequestIdContext.parseForwardOrdinal("2")).isEqualTo(2);
    assertThat(ForwardedRequestIdContext.parseForwardOrdinal("17")).isEqualTo(17);
    assertThat(ForwardedRequestIdContext.parseForwardOrdinal(null)).isZero();
    assertThat(ForwardedRequestIdContext.parseForwardOrdinal("")).isZero();
    assertThat(ForwardedRequestIdContext.parseForwardOrdinal("1")).isZero();
    assertThat(ForwardedRequestIdContext.parseForwardOrdinal("0")).isZero();
    assertThat(ForwardedRequestIdContext.parseForwardOrdinal("-2")).isZero();
    assertThat(ForwardedRequestIdContext.parseForwardOrdinal("2a")).isZero();
    assertThat(ForwardedRequestIdContext.parseForwardOrdinal("9999999999")).isZero();
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Issue #8347: the key of the client's own request, relayed by a forward that is the whole of it
  // ---------------------------------------------------------------------------------------------------------------

  private static final String KEY = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

  @Test
  void theFirstCommandForwardOfAOneCommandRouteRelaysTheClientKey() {
    ForwardedRequestIdContext.set("req-8347", KEY, true);

    assertThat(ForwardedRequestIdContext.clientKey()).isEqualTo(KEY);
    assertThat(ForwardedRequestIdContext.clientKeyForCommandForward(ForwardedRequestIdContext.nextForwardOrdinal()))
        .isEqualTo(KEY);
    assertThat(ForwardedRequestIdContext.clientKeyForCommandForward(ForwardedRequestIdContext.nextForwardOrdinal()))
        .as("a second forward is a part of the request, never the whole of it").isNull();
  }

  /** On any other route the key names more than one statement, so no command forward may settle it. */
  @Test
  void aRouteWhoseBodyIsNotOneCommandRelaysNoKeyOnACommandForward() {
    ForwardedRequestIdContext.set("req-8347", KEY, false);

    assertThat(ForwardedRequestIdContext.clientKeyForCommandForward(1)).isNull();
    assertThat(ForwardedRequestIdContext.clientKey()).as("the whole-request forwarder still relays it").isEqualTo(KEY);
  }

  /** A write issued by a command this node executes itself is a part of the request, whatever its ordinal. */
  @Test
  void aCommandExecutedLocallyStopsTheKeyFromTravellingOnACommandForward() {
    ForwardedRequestIdContext.set("req-8347", KEY, true);
    ForwardedRequestIdContext.markExecutedLocally();

    assertThat(ForwardedRequestIdContext.clientKeyForCommandForward(ForwardedRequestIdContext.nextForwardOrdinal())).isNull();

    // An auto-commit retry runs the same command locally again: restarting the ordinals does not bring the key back.
    ForwardedRequestIdContext.restartOrdinals();
    assertThat(ForwardedRequestIdContext.clientKeyForCommandForward(ForwardedRequestIdContext.nextForwardOrdinal())).isNull();
  }

  @Test
  void noIdMeansNoKeyAndClearingDropsIt() {
    ForwardedRequestIdContext.set("  ", KEY, true);
    assertThat(ForwardedRequestIdContext.clientKey()).isNull();
    assertThat(ForwardedRequestIdContext.clientKeyForCommandForward(1)).isNull();

    ForwardedRequestIdContext.set("req-8347", KEY, true);
    ForwardedRequestIdContext.clear();
    assertThat(ForwardedRequestIdContext.clientKey()).isNull();
    assertThat(ForwardedRequestIdContext.clientKeyForCommandForward(1)).isNull();
  }

  /** Only a value an idempotency key can be - a lower-case hex SHA-256 digest - is parsed or published. */
  @Test
  void onlyAValueAnIdempotencyKeyCanBeIsAccepted() {
    assertThat(ForwardedRequestIdContext.parseClientKey(KEY)).isEqualTo(KEY);
    assertThat(ForwardedRequestIdContext.parseClientKey(null)).isNull();
    assertThat(ForwardedRequestIdContext.parseClientKey("")).isNull();
    assertThat(ForwardedRequestIdContext.parseClientKey(KEY.substring(1))).isNull();
    assertThat(ForwardedRequestIdContext.parseClientKey(KEY + "0")).isNull();
    assertThat(ForwardedRequestIdContext.parseClientKey(KEY.toUpperCase())).isNull();
    assertThat(ForwardedRequestIdContext.parseClientKey("g" + KEY.substring(1))).isNull();

    ForwardedRequestIdContext.set("req-8347", "not-a-key", true);
    assertThat(ForwardedRequestIdContext.clientKey()).isNull();
    assertThat(ForwardedRequestIdContext.requestId()).isEqualTo("req-8347");
  }
}
