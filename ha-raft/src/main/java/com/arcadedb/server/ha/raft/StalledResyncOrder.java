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

import com.arcadedb.serializer.json.JSONObject;

/**
 * The leader's view of a stalled follower at the moment it decided to force that follower to drop its databases and
 * re-acquire them (issue #8490). Sent in the body of {@code POST /api/v1/cluster/resync/{database}} so the follower
 * can check, when the order ARRIVES, that the state it was based on still holds.
 * <p>
 * The order is carried out long after it was decided whenever the HTTP call is queued or delivered late: in the
 * reported incident a follower that had been stopped received it after it had restarted and caught up, and dropped a
 * current copy for nothing. The checks are clock-independent on purpose - two nodes' wall clocks cannot be compared,
 * but the Raft term and the log indexes can:
 * <ul>
 *   <li>a follower already in a LATER term than the order was issued in refuses it: the leader that decided is gone.
 *       A follower in an EARLIER term does not: a follower whose replication path is dead has never heard from the
 *       new leader at all, which is exactly the case this recovery exists for;</li>
 *   <li>a follower whose applied index has reached the leader's commit index the order was based on is not behind;</li>
 *   <li>a follower whose applied index has moved past the {@code matchIndex} the leader saw has progressed since the
 *       decision. Skipped when the leader saw the never-appended sentinel ({@code -1}), which says nothing about the
 *       follower's own log.</li>
 * </ul>
 * A request without these fields (an operator's manual resync) carries no order and is not checked.
 */
record StalledResyncOrder(long leaderTerm, long observedMatchIndex, long leaderCommitIndex) {

  static final String LEADER_TERM          = "leaderTerm";
  static final String OBSERVED_MATCH_INDEX = "observedMatchIndex";
  static final String LEADER_COMMIT_INDEX  = "leaderCommitIndex";

  JSONObject toJSON() {
    return new JSONObject().put(LEADER_TERM, leaderTerm).put(OBSERVED_MATCH_INDEX, observedMatchIndex)
        .put(LEADER_COMMIT_INDEX, leaderCommitIndex);
  }

  /** The order carried by a resync request body, or {@code null} for a request that carries none (manual resync). */
  static StalledResyncOrder fromPayload(final JSONObject payload) {
    if (payload == null || !payload.has(LEADER_TERM))
      return null;
    return new StalledResyncOrder(payload.getLong(LEADER_TERM, -1), payload.getLong(OBSERVED_MATCH_INDEX, -1),
        payload.getLong(LEADER_COMMIT_INDEX, -1));
  }

  /**
   * Why a follower in {@code followerTerm} with {@code followerAppliedIndex} applied must refuse this order, or
   * {@code null} when the leader's view still holds and the drop may go ahead.
   */
  String refusal(final long followerTerm, final long followerAppliedIndex) {
    if (followerTerm > leaderTerm)
      return "the order was issued in term " + leaderTerm + " and this node is already in term " + followerTerm;
    if (leaderCommitIndex >= 0 && followerAppliedIndex >= leaderCommitIndex)
      return "this node is not behind: it has applied up to " + followerAppliedIndex
          + ", at or past the leader commit index " + leaderCommitIndex + " the order was based on";
    if (observedMatchIndex >= 0 && followerAppliedIndex > observedMatchIndex)
      return "this node has progressed since the order was decided: it has applied up to " + followerAppliedIndex
          + ", past the matchIndex " + observedMatchIndex + " the leader saw";
    return null;
  }
}
