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
package com.arcadedb.server.ha.raft;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10067: a transaction that has already written is prepared from the position that includes a type the leader created for
 * it only when that type is KNOWN to be the one schema change since it was prepared. The rule is {@link
 * RaftReplicatedDatabase#transactionStaysValidPastTheType}, one condition per way another change could hide behind the count of one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10067TransactionStaysValidPastTheTypeTest {

  /** A transaction that wrote nothing has nothing built against any schema: always. */
  @Test
  void aTransactionThatWroteNothingIsAlwaysPreparedFromHere() {
    for (final boolean unchanged : new boolean[] { true, false })
      for (final boolean created : new boolean[] { true, false })
        for (final int changes : new int[] { 0, 1, 2, Integer.MAX_VALUE })
          assertThat(RaftReplicatedDatabase.transactionStaysValidPastTheType(false, unchanged, created, changes))
              .as("unchanged=%s created=%s changes=%d", unchanged, created, changes).isTrue();
  }

  @Test
  void aTransactionThatWroteStaysValidWhenTheTypeItsRequestMadeIsTheOnlyChange() {
    assertThat(RaftReplicatedDatabase.transactionStaysValidPastTheType(true, true, true, 1)).isTrue();
  }

  /** Somebody changed the schema after the transaction was prepared and before the request. */
  @Test
  void aSchemaChangedSinceItWasPreparedKeepsTheLeadersVerdict() {
    assertThat(RaftReplicatedDatabase.transactionStaysValidPastTheType(true, false, true, 1)).isFalse();
  }

  /**
   * The type was there already (created=false): the one entry applied since is the one that made it, some other client's, and may
   * have changed more than the type. Not ours to vouch for.
   */
  @Test
  void aTypeThatWasThereAlreadyIsNotOurEntry() {
    assertThat(RaftReplicatedDatabase.transactionStaysValidPastTheType(true, true, false, 1)).isFalse();
    assertThat(RaftReplicatedDatabase.transactionStaysValidPastTheType(true, true, false, 0)).isFalse();
  }

  /** None is a count that cannot be right for a request that made a type; more than one is somebody else's change as well. */
  @Test
  void onlyExactlyOneSchemaEntryInTheStretchIsOurs() {
    assertThat(RaftReplicatedDatabase.transactionStaysValidPastTheType(true, true, true, 0)).isFalse();
    assertThat(RaftReplicatedDatabase.transactionStaysValidPastTheType(true, true, true, 2)).isFalse();
    assertThat(RaftReplicatedDatabase.transactionStaysValidPastTheType(true, true, true, 50)).isFalse();
  }

  /** The retained history is bounded: a stretch it does not reach back to fails closed, never open. */
  @Test
  void aStretchTheHistoryCannotVouchForFailsClosed() {
    assertThat(RaftReplicatedDatabase.transactionStaysValidPastTheType(true, true, true, Integer.MAX_VALUE)).isFalse();
  }
}
