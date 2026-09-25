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
}
