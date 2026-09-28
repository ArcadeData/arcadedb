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
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StartLeaderElectionRequestProto;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.util.ProtoUtils;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * Makes the target of a leadership transfer catch up with the commit it was sent to campaign on before it campaigns
 * (issue #8533).
 * <p>
 * Apache Ratis 3.3.x races its own targeted transfer. When the target's AppendEntries acknowledgement is the one that
 * completes the majority for the leader's last data entry D, {@code LeaderStateImpl.onFollowerSuccessAppendEntries}
 * queues the commit update and then, synchronously, has {@code TransferLeadership} send {@code StartLeaderElection}
 * with {@code lastEntry = D}. The commit that follows appends a commit-index metadata entry D+1
 * ({@code raft.server.log.metadata.enabled}, on by default). If D+1 reaches the other follower before the target's
 * vote request does, both voters hold a longer log than the candidate, reject it, and the ex-leader has already
 * stepped down on the higher term: nobody campaigns until an election timer fires, 5-10 s with the default
 * {@code arcadedb.ha.electionTimeoutMin/Max}.
 * <p>
 * The fix is on the receiving side, because that is the one side ArcadeDB can hook without patching Ratis: the
 * {@link RaftServer} handed to the gRPC services is wrapped so an incoming {@code StartLeaderElection} first waits,
 * bounded by {@link #MAX_WAIT_MS}, until this node has learned that D is committed and has received what the leader
 * appended after it. Only then does the request reach Ratis, and the vote request carries a log at least as long as
 * either voter's. The wait applies only when it can help: the local entry at D must be a data entry of the term the
 * leader named that this node does not yet know to be committed - the exact shape of the race. A transfer whose last
 * entry is already a metadata entry (the quiescent case, where committing it appends nothing further), is already
 * committed, or is the log's first entry (whose commit Ratis never records in a metadata entry) goes straight through,
 * and so does anything this gate fails to read.
 * <p>
 * Commit-index metadata stays enabled. Disabling it would remove the entry the race needs, but it would also change
 * what a restarted node applies before it hears from a leader, and {@code BootstrapElection}'s first-formation gate
 * reads that very commit index.
 * <p>
 * Drop this class, and its use in {@link FixedGrpcFactory#newRaftServerRpc(RaftServer)}, once an Apache Ratis release
 * samples the transfer's {@code lastEntry} after the commit update.
 */
public final class TransfereeCatchUp {

  /**
   * Longest a {@code StartLeaderElection} is held back. In the race the commit and the metadata entry reach this node
   * in the leader's next AppendEntries, within milliseconds; the bound only caps the cost when they do not come. Far
   * below the 10 s Ratis request timeout the leader's RPC runs under, and further capped by {@link #maxWaitMs}.
   */
  static final long MAX_WAIT_MS = 1_000;

  /**
   * After the commit is known, how long to wait for the entry the leader appended after D when it has not arrived in
   * the same AppendEntries as the commit index (the leader can read its commit index for a request before it appends
   * the metadata entry). Bounded by what is left of {@link #MAX_WAIT_MS}.
   */
  static final long APPENDED_AFTER_COMMIT_GRACE_MS = 200;

  private static final long POLL_MS = 2;

  /** The three reads the gate needs from the local Raft log, so the waiting logic is testable without a server. */
  interface LocalLog {
    long commitIndex();

    long lastIndex();

    /**
     * Whether the local entry at {@code index} has term {@code term} and is not a commit-index metadata entry. False
     * when the entry is absent or unreadable.
     */
    boolean isDataEntry(long index, long term);
  }

  private TransfereeCatchUp() {
  }

  /**
   * Returns a {@link RaftServer} that behaves exactly like {@code server}, except that
   * {@link RaftServer#startLeaderElection(StartLeaderElectionRequestProto)} first runs
   * {@link #beforeStartLeaderElection(RaftServer, StartLeaderElectionRequestProto)}.
   */
  public static RaftServer wrap(final RaftServer server) {
    final InvocationHandler handler = new InvocationHandler() {
      @Override
      public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
        if (args != null && args.length == 1 && args[0] instanceof StartLeaderElectionRequestProto request
            && "startLeaderElection".equals(method.getName()))
          beforeStartLeaderElection(server, request);
        try {
          return method.invoke(server, args);
        } catch (final InvocationTargetException e) {
          throw e.getCause();
        }
      }
    };
    return (RaftServer) Proxy.newProxyInstance(RaftServer.class.getClassLoader(), new Class<?>[] { RaftServer.class },
        handler);
  }

  /**
   * Holds an incoming {@code StartLeaderElection} until this node has caught up with the commit of the leader's last
   * entry (see the class comment). Never throws: whatever it cannot read, it lets the request through unchanged.
   */
  static void beforeStartLeaderElection(final RaftServer server, final StartLeaderElectionRequestProto request) {
    if (!request.hasLeaderLastEntry())
      return;
    try {
      final TermIndex leaderLastEntry = TermIndex.valueOf(request.getLeaderLastEntry());
      final RaftGroupId groupId = ProtoUtils.toRaftGroupId(request.getServerRequest().getRaftGroupId());
      final RaftLog log = server.getDivision(groupId).getRaftLog();
      final long start = System.nanoTime();
      if (awaitCatchUp(localLog(log), leaderLastEntry.getIndex(), leaderLastEntry.getTerm(), maxWaitMs(server),
          APPENDED_AFTER_COMMIT_GRACE_MS))
        LogManager.instance().log(TransfereeCatchUp.class, Level.INFO,
            "Leadership transfer: waited %d ms for the commit of %s before campaigning (commit=%d, last=%d)",
            (System.nanoTime() - start) / 1_000_000, leaderLastEntry, log.getLastCommittedIndex(),
            log.getLastEntryTermIndex() != null ? log.getLastEntryTermIndex().getIndex() : -1);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (final Exception e) {
      LogManager.instance().log(TransfereeCatchUp.class, Level.FINE,
          "Leadership transfer: could not check the local log before campaigning, campaigning as asked: %s", e.toString());
    }
  }

  /**
   * {@link #MAX_WAIT_MS}, but never more than half the minimum election timeout. A transfer Ratis starts on its own
   * (yielding to a higher-priority peer) gives up after that minimum, and a target still waiting past it would then
   * campaign against a leader that has resumed: a disruption rather than a handoff.
   */
  static long maxWaitMs(final RaftServer server) {
    final long electionMinMs = RaftServerConfigKeys.Rpc.timeoutMin(server.getProperties()).toLong(TimeUnit.MILLISECONDS);
    return Math.max(0, Math.min(MAX_WAIT_MS, electionMinMs / 2));
  }

  /**
   * Waits until {@code log} knows the entry at {@code index} to be committed and holds something past it, within
   * {@code maxWaitMs}, and only when that entry is an uncommitted data entry of {@code term}.
   *
   * @return whether it waited at all
   */
  static boolean awaitCatchUp(final LocalLog log, final long index, final long term, final long maxWaitMs,
      final long appendedAfterCommitGraceMs) throws InterruptedException {
    // Ratis never appends a metadata entry for a commit index of 0 or less (RaftLogBase.shouldAppendMetadata: "do not
    // log the first conf entry"), so a transfer whose last entry is the very first one has no race to wait out. It is
    // exactly the transfer BootstrapElection makes on a fresh cluster, where waiting would only cost the full bound.
    if (index <= 0 || log.commitIndex() >= index || !log.isDataEntry(index, term))
      return false;

    final long deadline = System.nanoTime() + maxWaitMs * 1_000_000L;
    while (log.commitIndex() < index) {
      if (System.nanoTime() - deadline >= 0)
        return true;
      Thread.sleep(POLL_MS);
    }
    final long graceDeadline = Math.min(deadline, System.nanoTime() + appendedAfterCommitGraceMs * 1_000_000L);
    while (log.lastIndex() <= index && System.nanoTime() - graceDeadline < 0)
      Thread.sleep(POLL_MS);
    return true;
  }

  private static LocalLog localLog(final RaftLog log) {
    return new LocalLog() {
      @Override
      public long commitIndex() {
        return log.getLastCommittedIndex();
      }

      @Override
      public long lastIndex() {
        final TermIndex last = log.getLastEntryTermIndex();
        return last != null ? last.getIndex() : RaftLog.INVALID_LOG_INDEX;
      }

      @Override
      public boolean isDataEntry(final long index, final long term) {
        try {
          final LogEntryProto entry = log.get(index);
          return entry != null && entry.getTerm() == term && !entry.hasMetadataEntry();
        } catch (final Exception e) {
          return false;
        }
      }
    };
  }
}
