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
package com.arcadedb.server.ha.raft.ratis;

import com.arcadedb.log.LogManager;
import org.apache.ratis.grpc.server.GrpcLogAppender;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.leader.FollowerInfo;
import org.apache.ratis.server.leader.LeaderState;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.RaftLogIndex;

import java.lang.reflect.Field;
import java.util.function.LongUnaryOperator;
import java.util.logging.Level;

/**
 * Drop-in replacement for Apache Ratis 3.2.2's {@link GrpcLogAppender} that fixes
 * <a href="https://issues.apache.org/jira/browse/RATIS-2523">RATIS-2523</a>: on an idle cluster,
 * when a follower restarts with empty Raft storage, the leader's appender keeps emitting
 * {@code received INCONSISTENCY reply with nextIndex 0 ... entriesCount=0} every ~5 s and never
 * advances. The cluster is functionally broken until any user transaction generates a real
 * AppendEntries.
 * <p>
 * Root cause is in {@code LogAppenderBase.getNextIndexForInconsistency}: for a heartbeat
 * ({@code requestFirstIndex == RaftLog.INVALID_LOG_INDEX}) the existing logic prefers the
 * leader's stale {@code matchIndex + 1} over the follower's truthful "I have nothing,
 * nextIndex=0" hint, leaving the appender stuck.
 * <p>
 * This subclass overrides {@code getNextIndexForInconsistency} narrowly: when the request is a
 * heartbeat and the follower's reply hint is at or below the leader's tracked
 * {@code matchIndex}, trust the follower's hint instead. That signals "I lost state since you
 * last heard from me" and the leader's {@code matchIndex} for that follower is therefore stale.
 * For real AppendEntries requests we delegate to the upstream method unchanged.
 * <p>
 * It also overrides {@link #shouldInstallSnapshot(boolean)} so a follower that already installed a snapshot ending
 * right before the leader's log start receives entries instead of an endless stream of install-snapshot
 * notifications (issue #8459, see that method). Apache Ratis 3.3.1 ships an exemption of its own for the same state,
 * but a looser one that leaves this class's fallback for a follower that no longer holds its snapshot unreachable, so
 * the override still answers that question itself (issue #8548).
 * <p>
 * Once RATIS-2523 is fixed and Apache Ratis offers the #8459 fallback, drop this class, drop
 * {@link FixedGrpcRpcType} / {@link FixedGrpcFactory}, and revert
 * {@code RaftPropertiesBuilder} to the stock {@code SupportedRpcType.GRPC}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class FixedGrpcLogAppender extends GrpcLogAppender {

  /**
   * Cached, validated reflective handle on {@code FollowerInfoImpl.matchIndex}. Resolved once
   * (fail-loud) at the first appender construction and reused for every rewind. {@code volatile}
   * for safe publication across the appender threads.
   */
  private static volatile Field matchIndexField;

  /**
   * Follower snapshot index the last {@link #shouldInstallSnapshot(boolean)} anchored on, or
   * {@link RaftLog#INVALID_LOG_INDEX}. Written by the appender's {@code run()} loop, read and cleared in
   * {@link #getNextIndexForError(long)}, which Ratis calls from the gRPC reply callback
   * ({@code AppendLogResponseHandler.onError}), hence {@code volatile}.
   * <p>
   * Deliberately not cleared when the anchored append succeeds, so it can outlive the anchor. That is harmless:
   * {@link #getNextIndexForError(long)} acts on it only while the follower's {@code snapshotIndex} and
   * {@code matchIndex} are both still equal to it, which stops being true as soon as the follower acknowledges an
   * entry past it; a stale value is then simply discarded.
   */
  private volatile long anchoredSnapshotIndex = RaftLog.INVALID_LOG_INDEX;

  /**
   * Anchor last logged at INFO and at WARNING, so each line is written once per anchor rather than once per appender
   * iteration or per failed append. Log-throttling only; a lost update costs at most one duplicate line.
   */
  private volatile long lastLoggedAnchorIndex   = RaftLog.INVALID_LOG_INDEX;
  private volatile long lastLoggedFallbackIndex = RaftLog.INVALID_LOG_INDEX;

  public FixedGrpcLogAppender(final RaftServer.Division server, final LeaderState leaderState, final FollowerInfo f) {
    super(server, leaderState, f);
    // Fail-loud startup self-check: verify the Ratis-internal matchIndex field is still present
    // and of the expected type. If a Ratis upgrade renamed/retyped the field, fail now (loudly)
    // instead of silently no-op'ing the RATIS-2523 workaround later and re-exposing the bug.
    resolveMatchIndexField(f.getClass());
  }

  @Override
  protected long getNextIndexForInconsistency(final long requestFirstIndex, final long replyNextIndex) {
    final long currentMatchIndex = getFollower().getMatchIndex();
    if (requestFirstIndex == RaftLog.INVALID_LOG_INDEX
        && replyNextIndex <= currentMatchIndex) {
      // RATIS-2523: heartbeat path with a follower whose hint is at or below our recorded
      // matchIndex. Our matchIndex is stale (the follower restarted with empty storage and we
      // never observed a SUCCESS that would have rolled it back). Setting only nextIndex back
      // to replyNextIndex is not enough: the SUCCESS handler uses updateMatchIndex which
      // monotonically takes the max, so the stale matchIndex never moves down and SUCCESS
      // skips the corresponding updateNextIndex. The appender then loops at the SUCCESS layer
      // instead of the INCONSISTENCY layer.
      //
      // Rewind matchIndex back to (replyNextIndex - 1) via reflection on FollowerInfoImpl. The
      // public FollowerInfo API only offers setSnapshotIndex which also rewrites snapshotIndex
      // and risks confusing Ratis's snapshot-install detection; we want the surgical reset.
      // The rewind is an atomic compare-and-set against the value we just observed, so a
      // concurrent SUCCESS reply that legitimately raised matchIndex wins and we never push it
      // backwards. Once RATIS-2523 ships, drop this whole subclass.
      forceMatchIndex(getFollower(), currentMatchIndex, replyNextIndex - 1);
      return replyNextIndex;
    }
    return super.getNextIndexForInconsistency(requestFirstIndex, replyNextIndex);
  }

  /**
   * Breaks the leader install-snapshot notify loop (issues #8449, #8457, #8459).
   * <p>
   * Stock Ratis asks for a snapshot when {@code nextIndex == log start} and {@code getPrevious(nextIndex)} is null,
   * without looking at the snapshot the follower already reported. After a follower installs up to exactly the
   * leader's log start - 1 (or the leader later purges to exactly the follower's snapshot index + 1) while the
   * leader's own marker is elsewhere, that previous entry never exists on the leader, so the leader re-notifies the
   * same boundary and the follower answers {@code ALREADY_INSTALLED} - without calling its state machine - forever,
   * about 180 times a second on both nodes, while its lag only grows.
   * <p>
   * The rest of Ratis already treats that state as appendable: {@code LogAppenderBase.newAppendEntriesRequest} and
   * {@code assertProtos} skip the previous entry when {@code nextIndex == follower snapshotIndex + 1}, and so does the
   * follower's {@code ServerImplUtils.assertEntries} against its own snapshot index. Only this decision disagreed. When
   * {@link #isAnchoredOnInstalledSnapshot} holds, this answers {@code false}, the appender sends the entries from the
   * log start on, and the follower accepts them. Every other case, including a follower bootstrapping for the first
   * time, keeps the stock answer. Covers both {@code shouldInstallSnapshot()} and
   * {@code shouldNotifyToInstallSnapshot()}, which both delegate here.
   * <p>
   * If the follower's own state no longer matches what it reported (for instance its storage was wiped since), its
   * {@code assertEntries} rejects the append. {@link #getNextIndexForError(long)} then withdraws the anchor, so the next
   * iteration notifies again and the follower either re-confirms its snapshot or installs a new one.
   * <p>
   * Apache Ratis 3.3.1 added an exemption of its own to the stock answer: no snapshot when
   * {@code nextIndex == follower snapshotIndex + 1}, without the {@code matchIndex} and install-reply conditions of
   * {@link #isAnchoredOnInstalledSnapshot}. Taken as is, it answered {@code false} before this method could record the
   * anchor, so the fallback above never ran; and after a fallback it went on answering {@code false}, because a
   * rewound {@code matchIndex} does not move {@code snapshotIndex}. A follower whose append was rejected was then sent
   * the same append forever (issue #8548). The stock answer is therefore read without that exemption: a {@code false}
   * from Ratis in exactly the state {@link #leaderLacksPreviousEntry} describes is treated as the {@code true} it was
   * in Ratis 3.3.0, and {@link #isAnchoredOnInstalledSnapshot} alone decides whether to skip the snapshot.
   * <p>
   * Verified against Apache Ratis 3.3.1: that delegation, the three previous-entry carve-outs above and the stock
   * exemption are what this override relies on, and nothing checks them at startup. Re-read them on a Ratis upgrade.
   * {@code Issue8459InstallSnapshotNotifyLoopTest.rejectedAnchoredAppendFallsBackToANewNotification} runs in
   * notification mode and fails if the stock answer again hides the anchor from this method; it is the test that
   * caught the 3.3.1 change.
   */
  @Override
  public boolean shouldInstallSnapshot(final boolean hasSnapshot) {
    final FollowerInfo follower = getFollower();
    final long nextIndex = follower.getNextIndex();
    if (!super.shouldInstallSnapshot(hasSnapshot)) {
      // The appender asks on every iteration, so the log-start comparison comes first: getPrevious() is only looked
      // up in the one position where the stock exemption can have hidden a missing previous entry. It repeats a
      // condition of leaderLacksPreviousEntry on purpose, as a guard for that lookup; the helper stays complete on its
      // own so its unit tests describe the whole clause.
      final long leaderStartIndex = getRaftLog().getStartIndex();
      if (nextIndex != leaderStartIndex
          || !leaderLacksPreviousEntry(nextIndex, getRaftLog().getNextIndex(), leaderStartIndex, getPrevious(nextIndex) == null))
        return false;
    }

    final long snapshotIndex = follower.getSnapshotIndex();
    if (!isAnchoredOnInstalledSnapshot(nextIndex, getRaftLog().getStartIndex(), snapshotIndex, follower.getMatchIndex(),
        follower.hasAttemptedToInstallSnapshot()))
      return true;

    anchoredSnapshotIndex = snapshotIndex;
    // One line per anchor, not per heartbeat: the appender thread consults this on every iteration.
    if (lastLoggedAnchorIndex != snapshotIndex) {
      lastLoggedAnchorIndex = snapshotIndex;
      LogManager.instance().log(this, Level.INFO,
          "Follower %s already installed a snapshot up to index %d, right before this leader's log start: sending log "
              + "entries from %d instead of re-notifying the snapshot (issue #8459)",
          follower.getName(), snapshotIndex, nextIndex);
    }
    return false;
  }

  /**
   * Withdraws the anchor of {@link #shouldInstallSnapshot(boolean)} when an append sent under it fails (issue #8459).
   * <p>
   * Ratis calls this for a failed non-heartbeat AppendEntries. Without it, an anchored append that the follower rejects
   * would be retried forever: the stock operator never moves {@code nextIndex} below {@code matchIndex + 1}, which is the
   * anchor itself, so the follower would stay anchored and no notification would ever reach it. That is what happens
   * to a follower whose Raft storage was wiped after it reported the snapshot: its {@code assertEntries} expects index 0
   * once there is no previous entry. Rewinding {@code matchIndex} one entry below the snapshot index breaks
   * {@code matchIndex == snapshotIndex}, so the next iteration takes the stock path and notifies; the follower answers
   * {@code ALREADY_INSTALLED} (which re-confirms the anchor) or installs a new snapshot. A transient error pays one extra
   * notification round trip. Lowering one follower's {@code matchIndex} cannot lower the commit index, which Ratis
   * only ever raises.
   */
  @Override
  protected LongUnaryOperator getNextIndexForError(final long newNextIndex) {
    final long anchor = anchoredSnapshotIndex;
    if (anchor > RaftLog.LEAST_VALID_LOG_INDEX) {
      anchoredSnapshotIndex = RaftLog.INVALID_LOG_INDEX;
      final FollowerInfo follower = getFollower();
      // Only while the follower has acknowledged nothing past the anchor: once it has, the failure is not about it.
      if (follower.getSnapshotIndex() == anchor && follower.getMatchIndex() == anchor) {
        forceMatchIndex(follower, anchor, anchor - 1);
        if (lastLoggedFallbackIndex != anchor) {
          lastLoggedFallbackIndex = anchor;
          LogManager.instance().log(this, Level.WARNING,
              "Follower %s failed an append starting right after its snapshot at index %d; notifying it to install a "
                  + "snapshot again instead (issue #8459)", follower.getName(), anchor);
        }
      }
    }
    return super.getNextIndexForError(newNextIndex);
  }

  /**
   * The last clause of Apache Ratis 3.3.0's {@code LogAppender.shouldInstallSnapshot}, which 3.3.1 narrowed with its
   * follower-snapshot exemption (issue #8548): the follower still needs entries, its {@code nextIndex} is this
   * leader's log start, and the entry before it is found neither in the log nor as this leader's snapshot marker
   * ({@code previousMissing}, the caller's {@code getPrevious(nextIndex) == null}). Without a previous entry an
   * append is only accepted by a follower that holds that entry as its snapshot, which is what
   * {@link #isAnchoredOnInstalledSnapshot} checks.
   * <p>
   * Package-private for direct unit testing.
   */
  static boolean leaderLacksPreviousEntry(final long followerNextIndex, final long leaderNextIndex,
      final long leaderStartIndex, final boolean previousMissing) {
    return followerNextIndex < leaderNextIndex
        && followerNextIndex == leaderStartIndex
        && followerNextIndex > RaftLog.LEAST_VALID_LOG_INDEX
        && previousMissing;
  }

  /**
   * Whether the follower is anchored on the snapshot it installed: its {@code nextIndex} is this leader's log start
   * and the entry just before it is the snapshot the follower itself reported (issue #8459).
   * <p>
   * Every argument is read from the leader's {@link FollowerInfo} and {@link RaftLog}:
   * <ul>
   *   <li>{@code followerNextIndex == leaderStartIndex}: the only shape in which {@code shouldInstallSnapshot} asks for
   *       a snapshot although the leader holds every entry the follower still needs - it does so because
   *       {@code getPrevious(nextIndex)} finds the previous entry neither in the purged log nor in this leader's own
   *       snapshot marker.</li>
   *   <li>{@code followerSnapshotIndex == followerNextIndex - 1}: the follower told us it holds that entry as a
   *       snapshot. {@code FollowerInfoImpl.snapshotIndex} is written only once the follower acknowledged a snapshot:
   *       an ALREADY_INSTALLED or SNAPSHOT_INSTALLED reply, or a chunked install whose every chunk was answered (not
   *       used here: ArcadeDB runs notification mode). A snapshot covers applied, hence committed, entries, which
   *       every later leader holds identically (leader completeness), so an append that starts right after it needs
   *       no previous-entry check.</li>
   *   <li>{@code followerMatchIndex == followerSnapshotIndex}: nothing was acknowledged since that reply, so the
   *       snapshot index is still the follower's latest confirmed position (both replies set matchIndex to it).</li>
   *   <li>{@code attemptedToInstallSnapshot}: set by those replies, among others, so a {@code FollowerInfo} that never
   *       saw one does not count; {@code followerSnapshotIndex > 0} excludes the initial {@code snapshotIndex} value
   *       outright.</li>
   * </ul>
   * Package-private for direct unit testing.
   */
  static boolean isAnchoredOnInstalledSnapshot(final long followerNextIndex, final long leaderStartIndex,
      final long followerSnapshotIndex, final long followerMatchIndex, final boolean attemptedToInstallSnapshot) {
    return attemptedToInstallSnapshot
        && leaderStartIndex > RaftLog.LEAST_VALID_LOG_INDEX
        && followerNextIndex == leaderStartIndex
        && followerSnapshotIndex > RaftLog.LEAST_VALID_LOG_INDEX
        && followerSnapshotIndex == followerNextIndex - 1
        && followerMatchIndex == followerSnapshotIndex;
  }

  /**
   * Surgically rewinds {@code FollowerInfoImpl.matchIndex} backwards. The public
   * {@link FollowerInfo#updateMatchIndex} method only takes the max, so to undo a stale value we
   * read the private {@code matchIndex} {@link RaftLogIndex} field and rewrite it atomically. A
   * Ratis layout change throws (the field was already validated at construction time, so this
   * only fires on a genuinely unexpected runtime state) rather than silently degrading.
   *
   * @param observedMatchIndex the matchIndex value observed by the caller; the rewind only
   *                           applies if matchIndex is still that value (compare-and-set)
   */
  private static void forceMatchIndex(final FollowerInfo follower, final long observedMatchIndex, final long newMatchIndex) {
    try {
      final RaftLogIndex idx = (RaftLogIndex) resolveMatchIndexField(follower.getClass()).get(follower);
      rewindMatchIndex(idx, observedMatchIndex, newMatchIndex);
    } catch (final ReflectiveOperationException | ClassCastException e) {
      throw new IllegalStateException("RATIS-2523 workaround: cannot rewind matchIndex for follower " + follower.getName()
          + "; the Ratis internal layout changed. Update or remove FixedGrpcLogAppender.", e);
    }
  }

  /**
   * Atomically rewinds {@code matchIndex} to {@code newMatchIndex}, but only if it is still the
   * {@code observedMatchIndex} the caller saw. Folding the guard into the atomic read-modify-write
   * keeps the operation race-free against Ratis's concurrent {@code updateMatchIndex} (which takes
   * the max): a SUCCESS reply that raised matchIndex above the observed value wins and this call
   * no-ops, preserving Ratis's monotonic-matchIndex invariant.
   * <p>
   * Package-private for direct unit testing of the concurrency-sensitive logic.
   */
  static void rewindMatchIndex(final RaftLogIndex matchIndex, final long observedMatchIndex, final long newMatchIndex) {
    if (newMatchIndex < RaftLog.INVALID_LOG_INDEX)
      throw new IllegalArgumentException(
          "RATIS-2523 workaround: refusing to set matchIndex below " + RaftLog.INVALID_LOG_INDEX + " (got " + newMatchIndex + ")");

    matchIndex.updateUnconditionally(old -> old == observedMatchIndex ? newMatchIndex : old, msg -> {
    });
  }

  /**
   * Resolves and validates the reflective handle on {@code FollowerInfoImpl.matchIndex}, caching
   * the result. Throws {@link IllegalStateException} (fail-loud) if the field is missing or is not
   * a {@link RaftLogIndex}, which can only happen if the Ratis internal layout changes on an
   * upgrade. Package-private so the self-check can be exercised by unit tests.
   */
  static Field resolveMatchIndexField(final Class<?> followerClass) {
    final Field cached = matchIndexField;
    if (cached != null && cached.getDeclaringClass() == followerClass)
      return cached;

    final Field found;
    try {
      found = followerClass.getDeclaredField("matchIndex");
    } catch (final NoSuchFieldException e) {
      throw new IllegalStateException("RATIS-2523 workaround self-check failed: " + followerClass.getName()
          + " has no 'matchIndex' field. The Ratis internal layout changed; update or remove FixedGrpcLogAppender.", e);
    }
    if (!RaftLogIndex.class.isAssignableFrom(found.getType()))
      throw new IllegalStateException("RATIS-2523 workaround self-check failed: " + followerClass.getName()
          + ".matchIndex has unexpected type " + found.getType().getName() + " (expected " + RaftLogIndex.class.getName()
          + "). The Ratis internal layout changed; update or remove FixedGrpcLogAppender.");

    found.setAccessible(true);
    matchIndexField = found;
    return found;
  }
}
