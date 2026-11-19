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
 * Bookkeeping and retirement of files announced by instalments of a snapshot session.
 */
class SnapshotInstalmentFileRetirementTest {

  // ------------------------------------------------------------------------------------------
  // Issue #6136
  // ------------------------------------------------------------------------------------------
  /*
   * Issue #6136: the file bookkeeping a schema session carries across its instalments, pinned WITHOUT a cluster.
   * <p>
   * Both rules below are map arithmetic spread over the life of a session, which is exactly the kind of logic that
   * regresses silently - and both decide whether a follower ends up holding a file no schema references. They are
   * covered end to end by {@code RaftSchemaWalInstalment3NodesIT}, but that is a {@code @Tag("slow")} three-node test
   * whose failure says "the nodes disagree" rather than which rule got it wrong. These run in the fast lane and name
   * the rule.
   *
   */

  // Regression for #6136: a file an instalment announced is not announced again
  // -------------------------------------------------------------------------------------------------------------
  // reconcileInstalmentFiles: what the session's FINAL entry has to say about what its instalments announced
  // -------------------------------------------------------------------------------------------------------------

  /** A file an instalment announced exists on the followers already, so the final entry must not repeat it. */
  @Test
  void aFileAnInstalmentAnnouncedIsNotAnnouncedAgain() {
    final Map<Integer, String> shipped = files(7, "idx_7.ptree");
    final Map<Integer, String> addFiles = files(7, "idx_7.ptree");
    final Map<Integer, String> removeFiles = new LinkedHashMap<>();

    RaftReplicatedDatabase.reconcileInstalmentFiles(shipped, addFiles, removeFiles);

    assertThat(addFiles).as("re-announcing it would run createNewFiles over a file being written into").isEmpty();
    assertThat(removeFiles).as("the session still wants the file, so nothing is retired").isEmpty();
  }

  // Regression for #6136: a file the session created and then dropped is retired
  /**
   * The case that makes this method necessary. {@code FileManager.dropFile} CANCELS the recorded create when a
   * session creates and then drops the same file, so the final entry's own maps say nothing at all about it - and
   * the followers were told to create it. Without the compensating removal it would survive there and nowhere else.
   */
  @Test
  void aFileTheSessionCreatedAndThenDroppedIsRetired() {
    final Map<Integer, String> shipped = files(7, "idx_7.ptree");
    // Neither map mentions file 7: the create was cancelled by the drop, which is what FileManager records.
    final Map<Integer, String> addFiles = files(9, "other_9.ptree");
    final Map<Integer, String> removeFiles = new LinkedHashMap<>();

    RaftReplicatedDatabase.reconcileInstalmentFiles(shipped, addFiles, removeFiles);

    assertThat(removeFiles).as("the followers hold it and this node does not, so the final entry must retire it")
        .containsExactly(Map.entry(7, "idx_7.ptree"));
    assertThat(addFiles).as("a file no instalment announced is untouched").containsExactly(Map.entry(9, "other_9.ptree"));
  }

  // Regression for #6136: an explicit removal is not overwritten
  /** An explicit removal the session already recorded wins: the announced name must not overwrite it. */
  @Test
  void anExplicitRemovalIsNotOverwritten() {
    final Map<Integer, String> shipped = files(7, "stale_name.ptree");
    final Map<Integer, String> addFiles = new LinkedHashMap<>();
    final Map<Integer, String> removeFiles = files(7, "current_name.ptree");

    RaftReplicatedDatabase.reconcileInstalmentFiles(shipped, addFiles, removeFiles);

    assertThat(removeFiles).containsExactly(Map.entry(7, "current_name.ptree"));
  }

  // Regression for #6136: a session that shipped nothing is untouched
  /** A session that shipped no instalment is left exactly as it was - the overwhelmingly common case. */
  @Test
  void aSessionThatShippedNothingIsUntouched() {
    final Map<Integer, String> addFiles = files(9, "other_9.ptree");
    final Map<Integer, String> removeFiles = new LinkedHashMap<>();

    RaftReplicatedDatabase.reconcileInstalmentFiles(new LinkedHashMap<>(), addFiles, removeFiles);

    assertThat(addFiles).containsExactly(Map.entry(9, "other_9.ptree"));
    assertThat(removeFiles).isEmpty();
  }

  // Regression for #6136: only the files this node no longer has are retired
  // -------------------------------------------------------------------------------------------------------------
  // partitionAbandonedFiles: what a session that never published has to undo
  // -------------------------------------------------------------------------------------------------------------

  /**
   * THIS NODE OWNS THE TRUTH, and both halves matter. A file it let go of is retired - the case
   * {@code BucketIndexBuilder.create()} produces, since it drops the half-built index from its own error handler,
   * leaving only the followers holding it. A file it still has is LEFT: both sides holding an unpublished file is a
   * state they agree on, and retiring it would end that agreement, swapping one divergence for another.
   */
  @Test
  void onlyTheFilesThisNodeNoLongerHasAreRetired() {
    final Map<Integer, String> shipped = new LinkedHashMap<>();
    shipped.put(7, "dropped_7.ptree");
    shipped.put(8, "kept_8.ptree");

    final Set<Integer> stillHere = Set.of(8);
    final Map<Integer, String> toRetire = new LinkedHashMap<>();
    final Map<Integer, String> keptLocally = new LinkedHashMap<>();

    RaftReplicatedDatabase.partitionAbandonedFiles(shipped, stillHere::contains, toRetire, keptLocally);

    assertThat(toRetire).as("this node let it go, so the followers must too")
        .containsExactly(Map.entry(7, "dropped_7.ptree"));
    assertThat(keptLocally).as("this node still has it, so retiring it on the followers would DIVERGE them")
        .containsExactly(Map.entry(8, "kept_8.ptree"));
  }

  // Regression for #6136: an instalment that created no file has nothing to retire
  /** Nothing announced, nothing to decide - the case where the instalments carried WAL only. */
  @Test
  void anInstalmentThatCreatedNoFileHasNothingToRetire() {
    final Map<Integer, String> toRetire = new LinkedHashMap<>();
    final Map<Integer, String> keptLocally = new LinkedHashMap<>();

    RaftReplicatedDatabase.partitionAbandonedFiles(new LinkedHashMap<>(), id -> true, toRetire, keptLocally);

    assertThat(toRetire).isEmpty();
    assertThat(keptLocally).isEmpty();
  }

  private static Map<Integer, String> files(final int fileId, final String fileName) {
    final Map<Integer, String> map = new LinkedHashMap<>();
    map.put(fileId, fileName);
    return map;
  }

  // ------------------------------------------------------------------------------------------
  // Issue #6143
  // ------------------------------------------------------------------------------------------
  /*
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
   */

  private static final String DB = "instalments";

  // Regression for #6143: a compensation that cannot be replicated is reported and never throws
  /**
   * THE BRANCH THIS TEST EXISTS FOR. The compensation must not throw - it runs from a {@code finally} that is
   * usually unwinding the build's own exception, and masking that would replace a diagnosable failure with a
   * confusing one - and it must report that the filesRetirement are still out there.
   */
  @Test
  void aCompensationThatCannotBeReplicatedIsReportedAndNeverThrows() {
    final AtomicReference<Map<Integer, String>> attempted = new AtomicReference<>();

    final InstalmentRetirement outcome = RaftReplicatedDatabase.retireAbandonedInstalments(this, DB, 3,
        filesRetirement(7, "idx_7.ptree"), fileId -> false, filesToRemove -> {
          attempted.set(filesToRemove);
          throw new IllegalStateException("this node is no longer the leader");
        }, () -> false);

    assertThat(outcome)
        .as("the files6143 the instalments created are on the other nodes and nothing will reclaim them; the caller's "
            + "own exception must still be the one that reaches the application")
        .isEqualTo(InstalmentRetirement.NOT_REPLICATED);
    assertThat(attempted.get()).as("and it must have named them, since the SEVERE line is all an operator gets")
        .containsExactly(Map.entry(7, "idx_7.ptree"));
  }

  // Regression for #6143: a compensation that can be replicated retires what this node no longer has
  /** The ordinary failure: this node still leads, so it undoes what its instalments announced. */
  @Test
  void aCompensationThatCanBeReplicatedRetiresWhatThisNodeNoLongerHas() {
    final AtomicReference<Map<Integer, String>> retired = new AtomicReference<>();

    final InstalmentRetirement outcome = RaftReplicatedDatabase.retireAbandonedInstalments(this, DB, 2,
        filesRetirement(7, "idx_7.ptree"), fileId -> false, retired::set, () -> true);

    assertThat(outcome).isEqualTo(InstalmentRetirement.RETIRED);
    assertThat(retired.get()).containsExactly(Map.entry(7, "idx_7.ptree"));
  }

  // Regression for #6143: a file this node still holds is left alone and nothing is submitted
  /**
   * A file this node STILL HAS is left alone, and the submitter is never called. Both sides holding an unpublished
   * file is a state they agree on; retiring it would swap one divergence for another.
   */
  @Test
  void aFileThisNodeStillHoldsIsLeftAloneAndNothingIsSubmitted() {
    final AtomicReference<Map<Integer, String>> submitted = new AtomicReference<>();

    final InstalmentRetirement outcome = RaftReplicatedDatabase.retireAbandonedInstalments(this, DB, 2,
        filesRetirement(8, "kept_8.ptree"), fileId -> true, submitted::set, () -> true);

    assertThat(outcome).isEqualTo(InstalmentRetirement.KEPT_BY_THIS_NODE);
    assertThat(submitted.get()).as("a removal nobody needs is still a replicated entry, so it must not be sent")
        .isNull();
  }

  // Regression for #6143: wal only instalments have nothing to retire
  /** Instalments that carried WAL only: their pages went to filesRetirement that already existed everywhere. */
  @Test
  void walOnlyInstalmentsHaveNothingToRetire() {
    final InstalmentRetirement outcome = RaftReplicatedDatabase.retireAbandonedInstalments(this, DB, 5,
        new LinkedHashMap<>(), fileId -> false, filesRetirement -> {
          throw new AssertionError("nothing was announced, so nothing may be submitted");
        }, () -> true);

    assertThat(outcome).isEqualTo(InstalmentRetirement.NOTHING_TO_RETIRE);
  }

  // Regression for #6143: a session that shipped no instalment does nothing
  /** The overwhelmingly common case: the session was small enough to ship in one entry, so nothing needs undoing. */
  @Test
  void aSessionThatShippedNoInstalmentDoesNothing() {
    final InstalmentRetirement outcome = RaftReplicatedDatabase.retireAbandonedInstalments(this, DB, 0,
        filesRetirement(7, "idx_7.ptree"), fileId -> false, filesRetirement -> {
          throw new AssertionError("no instalment went out, so no follower is holding anything");
        }, () -> true);

    assertThat(outcome).isEqualTo(InstalmentRetirement.NOTHING_SHIPPED);
  }

  // Regression for #6143: only the half this node let go of is retired
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

  private static Map<Integer, String> filesRetirement(final int fileId, final String fileName) {
    final Map<Integer, String> filesRetirement = new LinkedHashMap<>();
    filesRetirement.put(fileId, fileName);
    return filesRetirement;
  }
}
