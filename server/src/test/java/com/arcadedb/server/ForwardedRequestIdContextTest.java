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

  private static final String CMD  = "INSERT INTO V SET id = 1";
  private static final String BODY = "{ \"serializer\": \"studio\", \"language\": \"sql\", \"command\": \"" + CMD + "\" }";

  /** The key of the request, when the forward with this ordinal of the declared statement is the whole of it. */
  private static String keyFor(final int ordinal) {
    final ForwardedRequestIdContext.WholeRequest whole = ForwardedRequestIdContext.wholeRequestForward(ordinal, "sql", CMD);
    return whole != null ? whole.clientKey() : null;
  }

  @Test
  void theFirstCommandForwardOfAOneCommandRouteRelaysTheClientKey() {
    ForwardedRequestIdContext.set("req-8347", KEY, true);
    ForwardedRequestIdContext.declareWholeRequestCommand("sql", CMD, BODY);

    assertThat(ForwardedRequestIdContext.clientKey()).isEqualTo(KEY);
    assertThat(keyFor(ForwardedRequestIdContext.nextForwardOrdinal())).isEqualTo(KEY);
    assertThat(keyFor(ForwardedRequestIdContext.nextForwardOrdinal()))
        .as("a second forward is a part of the request, never the whole of it").isNull();
  }

  /** On any other route the key names more than one statement, so no command forward may settle it. */
  @Test
  void aRouteWhoseBodyIsNotOneCommandRelaysNoKeyOnACommandForward() {
    ForwardedRequestIdContext.set("req-8347", KEY, false);
    ForwardedRequestIdContext.declareWholeRequestCommand("sql", CMD, BODY);

    assertThat(keyFor(1)).isNull();
    assertThat(ForwardedRequestIdContext.clientKey()).as("the whole-request forwarder still relays it").isEqualTo(KEY);
  }

  /** A write issued by a command this node executes itself is a part of the request, whatever its ordinal. */
  @Test
  void aCommandExecutedLocallyStopsTheKeyFromTravellingOnACommandForward() {
    ForwardedRequestIdContext.set("req-8347", KEY, true);
    ForwardedRequestIdContext.declareWholeRequestCommand("sql", CMD, BODY);
    ForwardedRequestIdContext.markExecutedLocally();

    assertThat(keyFor(ForwardedRequestIdContext.nextForwardOrdinal())).isNull();

    // An auto-commit retry runs the same command locally again: restarting the ordinals does not bring the key back.
    ForwardedRequestIdContext.restartOrdinals();
    ForwardedRequestIdContext.declareWholeRequestCommand("sql", CMD, BODY);
    assertThat(keyFor(ForwardedRequestIdContext.nextForwardOrdinal())).isNull();
  }

  @Test
  void noIdMeansNoKeyAndClearingDropsIt() {
    ForwardedRequestIdContext.set("  ", KEY, true);
    ForwardedRequestIdContext.declareWholeRequestCommand("sql", CMD, BODY);
    assertThat(ForwardedRequestIdContext.clientKey()).isNull();
    assertThat(keyFor(1)).isNull();

    ForwardedRequestIdContext.set("req-8347", KEY, true);
    ForwardedRequestIdContext.declareWholeRequestCommand("sql", CMD, BODY);
    ForwardedRequestIdContext.clear();
    assertThat(ForwardedRequestIdContext.clientKey()).isNull();
    assertThat(keyFor(1)).isNull();
  }

  // Issue #8359 ------------------------------------------------------------------------------------------------------

  /** The whole-request forward carries the client's own body, so the leader answers it in the client's rendering. */
  @Test
  void theWholeRequestForwardCarriesTheClientsOwnBody() {
    ForwardedRequestIdContext.set("req-8359", KEY, true);
    ForwardedRequestIdContext.declareWholeRequestCommand("sql", CMD, BODY);

    final ForwardedRequestIdContext.WholeRequest whole = ForwardedRequestIdContext.wholeRequestForward(1, "sql", CMD);
    assertThat(whole).isNotNull();
    assertThat(whole.clientKey()).isEqualTo(KEY);
    assertThat(whole.clientBody()).isEqualTo(BODY);
  }

  /**
   * Nothing is the whole request unless the handler declared it: a route that runs its statement some other way - a
   * query, which a follower runs itself - declares nothing, and a write that statement issues from inside is a part.
   */
  @Test
  void nothingIsTheWholeRequestUntilTheHandlerDeclaresIt() {
    ForwardedRequestIdContext.set("req-8359", KEY, true);

    assertThat(ForwardedRequestIdContext.wholeRequestForward(1, "sql", CMD)).isNull();
  }

  /** A first forward of a statement other than the declared one - a write a function issues - is a part of it. */
  @Test
  void aForwardOfAnotherStatementIsNotTheWholeRequest() {
    ForwardedRequestIdContext.set("req-8359", KEY, true);
    ForwardedRequestIdContext.declareWholeRequestCommand("sql", "SELECT writes()", BODY);

    assertThat(ForwardedRequestIdContext.wholeRequestForward(1, "sql", CMD)).isNull();
    assertThat(ForwardedRequestIdContext.wholeRequestForward(1, "cypher", "SELECT writes()"))
        .as("the same text in another language is another statement").isNull();
    assertThat(ForwardedRequestIdContext.wholeRequestForward(1, "sql", "SELECT writes()")).isNotNull();
  }

  /** The leader's answer is read once, dropped by a new declaration (an auto-commit retry) and by clearing. */
  @Test
  void theLeadersAnswerIsConsumedOnceAndNeverOutlivesTheRequest() {
    ForwardedRequestIdContext.set("req-8359", KEY, true);
    ForwardedRequestIdContext.declareWholeRequestCommand("sql", CMD, BODY);

    assertThat(ForwardedRequestIdContext.takeWholeRequestAnswer()).isNull();

    ForwardedRequestIdContext.publishWholeRequestAnswer("{\"result\":{}}");
    assertThat(ForwardedRequestIdContext.takeWholeRequestAnswer()).isEqualTo("{\"result\":{}}");
    assertThat(ForwardedRequestIdContext.takeWholeRequestAnswer()).as("consumed").isNull();

    ForwardedRequestIdContext.publishWholeRequestAnswer("{\"result\":{}}");
    ForwardedRequestIdContext.declareWholeRequestCommand("sql", CMD, BODY);
    assertThat(ForwardedRequestIdContext.takeWholeRequestAnswer()).as("a retried attempt starts without one").isNull();

    ForwardedRequestIdContext.publishWholeRequestAnswer("{\"result\":{}}");
    ForwardedRequestIdContext.clear();
    assertThat(ForwardedRequestIdContext.takeWholeRequestAnswer()).isNull();
    assertThat(ForwardedRequestIdContext.wholeRequestForward(1, "sql", CMD)).isNull();
  }

  /** A request that cannot be forwarded whole holds no body: the declaration records nothing. */
  @Test
  void aDeclarationOnARequestThatCannotBeForwardedWholeRecordsNothing() {
    ForwardedRequestIdContext.set("req-8359", KEY, false);
    ForwardedRequestIdContext.declareWholeRequestCommand("sql", CMD, BODY);
    assertThat(ForwardedRequestIdContext.wholeRequestForward(1, "sql", CMD)).isNull();

    ForwardedRequestIdContext.clear();
    ForwardedRequestIdContext.declareWholeRequestCommand("sql", CMD, BODY);
    assertThat(ForwardedRequestIdContext.wholeRequestForward(1, "sql", CMD)).isNull();
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
