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

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TransactionException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pure decision rules of {@link RaftReplicatedDatabase} around schema changes and transactions: when the leader falls back to shipping the whole schema, when a transaction stays valid past a type creation, and how an unknown-function answer is recognized.
 */
class RaftReplicatedDatabaseSchemaTransactionTest {

  // ---- #6989 ----
  // every arm that makes the leader fall back to shipping the WHOLE schema document.
  // <p>
  // These are the branches whose failure mode is silent. Neither of the two defects the integration test caught -
  // the setting read off the per-database configuration instead of the server's, and a staleness guard keyed on
  // the schema version instead of the Raft term - broke replication or failed a test: they just stopped the delta
  // path from ever engaging. The term arm in particular is otherwise reachable only by forcing a re-election
  // mid-test, which is expensive and flaky, so the decision is pinned here instead.

  private static final long TERM = 7L;

  // Regression for #6989: every arm that makes the leader fall back to shipping the WHOLE schema document.
  @Test
  void theSettingBeingOffShipsTheWholeDocument() {
    assertThat(RaftReplicatedDatabase.baseIsUsable(false, true, TERM, TERM)).isFalse();
  }

  // Regression for #6989: every arm that makes the leader fall back to shipping the WHOLE schema document.
  @Test
  void noBaseYetShipsTheWholeDocument() {
    assertThat(RaftReplicatedDatabase.baseIsUsable(true, false, -1L, TERM)).isFalse();
  }

  // Regression for #6989: every arm that makes the leader fall back to shipping the WHOLE schema document.
  @Test
  void aTermThatMovedShipsTheWholeDocument() {
    // Another node was leader in between, so what the followers hold is whatever IT shipped.
    assertThat(RaftReplicatedDatabase.baseIsUsable(true, true, TERM, TERM + 1)).isFalse();
    // And the same node regaining leadership does not make the cache valid again: a new term is a new term.
    assertThat(RaftReplicatedDatabase.baseIsUsable(true, true, TERM, TERM + 2)).isFalse();
  }

  // Regression for #6989: every arm that makes the leader fall back to shipping the WHOLE schema document.
  @Test
  void anUnreadableTermShipsTheWholeDocument() {
    // A division restarting in place reports -1: unknown cannot show the cache is still valid.
    assertThat(RaftReplicatedDatabase.baseIsUsable(true, true, TERM, -1L)).isFalse();
    assertThat(RaftReplicatedDatabase.baseIsUsable(true, true, -1L, -1L))
        .as("-1 must not match -1 into a usable base")
        .isFalse();
  }

  // Regression for #6989: every arm that makes the leader fall back to shipping the WHOLE schema document.
  @Test
  void anUnchangedTermWithTheSettingOnUsesTheBase() {
    assertThat(RaftReplicatedDatabase.baseIsUsable(true, true, TERM, TERM)).isTrue();
  }

  // Regression for #6989: every arm that makes the leader fall back to shipping the WHOLE schema document.
  @Test
  void aDeltaWorthMostOfTheDocumentIsNotShipped() {
    assertThat(RaftReplicatedDatabase.deltaIsWorthShipping(600, 1000)).isFalse();
    assertThat(RaftReplicatedDatabase.deltaIsWorthShipping(500, 1000))
        .as("exactly half is not smaller enough to be worth the key sets")
        .isFalse();
    assertThat(RaftReplicatedDatabase.deltaIsWorthShipping(499, 1000)).isTrue();
  }

  // Regression for #6989: every arm that makes the leader fall back to shipping the WHOLE schema document.
  @Test
  void aDeltaIsShippedWhenNoWholeDocumentLengthIsKnownYet() {
    assertThat(RaftReplicatedDatabase.deltaIsWorthShipping(10_000, 0)).isTrue();
  }

  // Regression for #6989: every arm that makes the leader fall back to shipping the WHOLE schema document.
  @Test
  void theSizeBarDoesNotOverflow() {
    // The doubling is done in long arithmetic: an int-sized delta must not wrap into "worth shipping".
    assertThat(RaftReplicatedDatabase.deltaIsWorthShipping(Integer.MAX_VALUE, 1_000)).isFalse();
  }

  // ---- #10067 ----
  // a transaction that has already written is prepared from the position that includes a type the leader created for
  // it only when that type is KNOWN to be the one schema change since it was prepared. The rule is {@link
  // RaftReplicatedDatabase#transactionStaysValidPastTheType}, one condition per way another change could hide behind the count of one.
  //

  /** A transaction that wrote nothing has nothing built against any schema: always. */
  // Regression for #10067: a transaction that has already written is prepared from the position that includes a type the leader created for it only when that type is KNOWN to...
  @Test
  void aTransactionThatWroteNothingIsAlwaysPreparedFromHere() {
    for (final boolean unchanged : new boolean[] { true, false })
      for (final boolean created : new boolean[] { true, false })
        for (final int changes : new int[] { 0, 1, 2, Integer.MAX_VALUE })
          assertThat(RaftReplicatedDatabase.transactionStaysValidPastTheType(false, unchanged, created, changes))
              .as("unchanged=%s created=%s changes=%d", unchanged, created, changes).isTrue();
  }

  // Regression for #10067: a transaction that has already written is prepared from the position that includes a type the leader created for it only when that type is KNOWN to...
  @Test
  void aTransactionThatWroteStaysValidWhenTheTypeItsRequestMadeIsTheOnlyChange() {
    assertThat(RaftReplicatedDatabase.transactionStaysValidPastTheType(true, true, true, 1)).isTrue();
  }

  /** Somebody changed the schema after the transaction was prepared and before the request. */
  // Regression for #10067: a transaction that has already written is prepared from the position that includes a type the leader created for it only when that type is KNOWN to...
  @Test
  void aSchemaChangedSinceItWasPreparedKeepsTheLeadersVerdict() {
    assertThat(RaftReplicatedDatabase.transactionStaysValidPastTheType(true, false, true, 1)).isFalse();
  }

  /**
   * The type was there already (created=false): the one entry applied since is the one that made it, some other client's, and may
   * have changed more than the type. Not ours to vouch for.
   */
  // Regression for #10067: a transaction that has already written is prepared from the position that includes a type the leader created for it only when that type is KNOWN to...
  @Test
  void aTypeThatWasThereAlreadyIsNotOurEntry() {
    assertThat(RaftReplicatedDatabase.transactionStaysValidPastTheType(true, true, false, 1)).isFalse();
    assertThat(RaftReplicatedDatabase.transactionStaysValidPastTheType(true, true, false, 0)).isFalse();
  }

  /** None is a count that cannot be right for a request that made a type; more than one is somebody else's change as well. */
  // Regression for #10067: a transaction that has already written is prepared from the position that includes a type the leader created for it only when that type is KNOWN to...
  @Test
  void onlyExactlyOneSchemaEntryInTheStretchIsOurs() {
    assertThat(RaftReplicatedDatabase.transactionStaysValidPastTheType(true, true, true, 0)).isFalse();
    assertThat(RaftReplicatedDatabase.transactionStaysValidPastTheType(true, true, true, 2)).isFalse();
    assertThat(RaftReplicatedDatabase.transactionStaysValidPastTheType(true, true, true, 50)).isFalse();
  }

  /** The retained history is bounded: a stretch it does not reach back to fails closed, never open. */
  // Regression for #10067: a transaction that has already written is prepared from the position that includes a type the leader created for it only when that type is KNOWN to...
  @Test
  void aStretchTheHistoryCannotVouchForFailsClosed() {
    assertThat(RaftReplicatedDatabase.transactionStaysValidPastTheType(true, true, true, Integer.MAX_VALUE)).isFalse();
  }

  // ---- #11064 ----
  // how a follower recognizes the answer of a leader that predates the {@code ensureLabelTypes} function.

  // Regression for #11064: how a follower recognizes the answer of a leader that predates the ensureLabelTypes function.
  @Test
  void theAnswerOfALeaderWithoutTheFunctionIsRecognized() {
    final RuntimeException error = RaftReplicatedDatabase.reconstructLeaderException(500,
        "{\"error\":\"Error on command execution\",\"detail\":\"Function not found: ensureLabelTypes\","
            + "\"exception\":\"com.arcadedb.exception.CommandExecutionException\"}");
    assertThat(RaftReplicatedDatabase.isUnknownFunctionAnswer(error, "ensureLabelTypes")).isTrue();
  }

  // Regression for #11064: how a follower recognizes the answer of a leader that predates the ensureLabelTypes function.
  @Test
  void theLookupWordingIsRecognizedToo() {
    final RuntimeException error = RaftReplicatedDatabase.reconstructLeaderException(500,
        "{\"error\":\"Error on command execution\",\"detail\":\"Unknown function name 'ensureLabelTypes'\","
            + "\"exception\":\"com.arcadedb.exception.CommandExecutionException\"}");
    assertThat(RaftReplicatedDatabase.isUnknownFunctionAnswer(error, "ensureLabelTypes")).isTrue();
  }

  // Regression for #11064: how a follower recognizes the answer of a leader that predates the ensureLabelTypes function.
  @Test
  void theNameIsMatchedAnyCaseAndThroughTheCauseChain() {
    final RuntimeException error = new TransactionException("wrapped", new CommandExecutionException("Function not found: ENSURELABELTYPES"));
    assertThat(RaftReplicatedDatabase.isUnknownFunctionAnswer(error, "ensureLabelTypes")).isTrue();
  }

  // Regression for #11064: how a follower recognizes the answer of a leader that predates the ensureLabelTypes function.
  @Test
  void anyOtherErrorIsNotAnUnknownFunction() {
    assertThat(RaftReplicatedDatabase.isUnknownFunctionAnswer(new TransactionException("boom"), "ensureLabelTypes")).isFalse();
    assertThat(RaftReplicatedDatabase.isUnknownFunctionAnswer(new CommandExecutionException("Function not found: other"),
        "ensureLabelTypes")).isFalse();
    assertThat(RaftReplicatedDatabase.isUnknownFunctionAnswer(new TransactionException((String) null), "ensureLabelTypes")).isFalse();
  }
}
