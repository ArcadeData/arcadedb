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

import com.arcadedb.server.ha.raft.RaftReplicatedDatabase.InstalmentRetirement;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6143: what the compensation does when it CANNOT run, which is the branch nothing covered.
 * <p>
 * A schema session that ships instalments and then fails retires what those instalments announced. That works while
 * this node still leads. It cannot once leadership has moved - failover, a brief partition, a manual step-down, all
 * ordinary Raft events - because a node that lost the term can no longer submit anything, so the removal fails and
 * the files stay on the other nodes with nothing referencing them.
 * <p>
 * {@code RaftSchemaWalInstalment3NodesIT} covers the compensation SUCCEEDING; this branch needs a step-down to land
 * between an instalment and the unwind, which is why it was previously reasoned about rather than tested. Making the
 * submitter injectable is what makes it constructible: a submitter that throws IS a node that lost leadership, as
 * far as this code can tell.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6143AbandonedInstalmentRetirementTest {
  private static final String DB = "instalments";

  /**
   * THE BRANCH THIS TEST EXISTS FOR. The compensation must not throw - it runs from a {@code finally} that is
   * usually unwinding the build's own exception, and masking that would replace a diagnosable failure with a
   * confusing one - and it must report that the files are still out there.
   */
  @Test
  void aCompensationThatCannotBeReplicatedIsReportedAndNeverThrows() {
    final AtomicReference<Map<Integer, String>> attempted = new AtomicReference<>();

    final InstalmentRetirement outcome = RaftReplicatedDatabase.retireAbandonedInstalments(this, DB, 3,
        files(7, "idx_7.ptree"), fileId -> false, filesToRemove -> {
          attempted.set(filesToRemove);
          throw new IllegalStateException("this node is no longer the leader");
        }, () -> false);

    assertThat(outcome)
        .as("the files the instalments created are on the other nodes and nothing will reclaim them; the caller's "
            + "own exception must still be the one that reaches the application")
        .isEqualTo(InstalmentRetirement.NOT_REPLICATED);
    assertThat(attempted.get()).as("and it must have named them, since the SEVERE line is all an operator gets")
        .containsExactly(Map.entry(7, "idx_7.ptree"));
  }

  /** The ordinary failure: this node still leads, so it undoes what its instalments announced. */
  @Test
  void aCompensationThatCanBeReplicatedRetiresWhatThisNodeNoLongerHas() {
    final AtomicReference<Map<Integer, String>> retired = new AtomicReference<>();

    final InstalmentRetirement outcome = RaftReplicatedDatabase.retireAbandonedInstalments(this, DB, 2,
        files(7, "idx_7.ptree"), fileId -> false, retired::set, () -> true);

    assertThat(outcome).isEqualTo(InstalmentRetirement.RETIRED);
    assertThat(retired.get()).containsExactly(Map.entry(7, "idx_7.ptree"));
  }

  /**
   * A file this node STILL HAS is left alone, and the submitter is never called. Both sides holding an unpublished
   * file is a state they agree on; retiring it would swap one divergence for another.
   */
  @Test
  void aFileThisNodeStillHoldsIsLeftAloneAndNothingIsSubmitted() {
    final AtomicReference<Map<Integer, String>> submitted = new AtomicReference<>();

    final InstalmentRetirement outcome = RaftReplicatedDatabase.retireAbandonedInstalments(this, DB, 2,
        files(8, "kept_8.ptree"), fileId -> true, submitted::set, () -> true);

    assertThat(outcome).isEqualTo(InstalmentRetirement.KEPT_BY_THIS_NODE);
    assertThat(submitted.get()).as("a removal nobody needs is still a replicated entry, so it must not be sent")
        .isNull();
  }

  /** Instalments that carried WAL only: their pages went to files that already existed everywhere. */
  @Test
  void walOnlyInstalmentsHaveNothingToRetire() {
    final InstalmentRetirement outcome = RaftReplicatedDatabase.retireAbandonedInstalments(this, DB, 5,
        new LinkedHashMap<>(), fileId -> false, files -> {
          throw new AssertionError("nothing was announced, so nothing may be submitted");
        }, () -> true);

    assertThat(outcome).isEqualTo(InstalmentRetirement.NOTHING_TO_RETIRE);
  }

  /** The overwhelmingly common case: the session was small enough to ship in one entry, so nothing needs undoing. */
  @Test
  void aSessionThatShippedNoInstalmentDoesNothing() {
    final InstalmentRetirement outcome = RaftReplicatedDatabase.retireAbandonedInstalments(this, DB, 0,
        files(7, "idx_7.ptree"), fileId -> false, files -> {
          throw new AssertionError("no instalment went out, so no follower is holding anything");
        }, () -> true);

    assertThat(outcome).isEqualTo(InstalmentRetirement.NOTHING_SHIPPED);
  }

  /** A mixed session retires only the half this node let go of. */
  @Test
  void onlyTheHalfThisNodeLetGoOfIsRetired() {
    final Map<Integer, String> shipped = new LinkedHashMap<>();
    shipped.put(7, "dropped_7.ptree");
    shipped.put(8, "kept_8.ptree");
    final Set<Integer> stillHere = Set.of(8);
    final AtomicReference<Map<Integer, String>> retired = new AtomicReference<>();

    final InstalmentRetirement outcome = RaftReplicatedDatabase.retireAbandonedInstalments(this, DB, 4, shipped,
        stillHere::contains, retired::set, () -> true);

    assertThat(outcome).isEqualTo(InstalmentRetirement.RETIRED);
    assertThat(retired.get()).containsExactly(Map.entry(7, "dropped_7.ptree"));
  }

  private static Map<Integer, String> files(final int fileId, final String fileName) {
    final Map<Integer, String> files = new LinkedHashMap<>();
    files.put(fileId, fileName);
    return files;
  }
}
