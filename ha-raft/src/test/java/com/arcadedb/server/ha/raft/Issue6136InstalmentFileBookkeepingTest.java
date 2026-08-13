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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6136: the file bookkeeping a schema session carries across its instalments, pinned WITHOUT a cluster.
 * <p>
 * Both rules below are map arithmetic spread over the life of a session, which is exactly the kind of logic that
 * regresses silently - and both decide whether a follower ends up holding a file no schema references. They are
 * covered end to end by {@code RaftSchemaWalInstalment3NodesIT}, but that is a {@code @Tag("slow")} three-node test
 * whose failure says "the nodes disagree" rather than which rule got it wrong. These run in the fast lane and name
 * the rule.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6136InstalmentFileBookkeepingTest {

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

  /** An explicit removal the session already recorded wins: the announced name must not overwrite it. */
  @Test
  void anExplicitRemovalIsNotOverwritten() {
    final Map<Integer, String> shipped = files(7, "stale_name.ptree");
    final Map<Integer, String> addFiles = new LinkedHashMap<>();
    final Map<Integer, String> removeFiles = files(7, "current_name.ptree");

    RaftReplicatedDatabase.reconcileInstalmentFiles(shipped, addFiles, removeFiles);

    assertThat(removeFiles).containsExactly(Map.entry(7, "current_name.ptree"));
  }

  /** A session that shipped no instalment is left exactly as it was - the overwhelmingly common case. */
  @Test
  void aSessionThatShippedNothingIsUntouched() {
    final Map<Integer, String> addFiles = files(9, "other_9.ptree");
    final Map<Integer, String> removeFiles = new LinkedHashMap<>();

    RaftReplicatedDatabase.reconcileInstalmentFiles(new LinkedHashMap<>(), addFiles, removeFiles);

    assertThat(addFiles).containsExactly(Map.entry(9, "other_9.ptree"));
    assertThat(removeFiles).isEmpty();
  }

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
}
