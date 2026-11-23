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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ha.raft.RaftHAServer.InPlaceRestartBaseline;
import com.arcadedb.server.monitor.HAReplicationStatsProvider;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.logging.Level;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.util.LifeCycle;

/**
 * Restarts the Ratis server of this node in place, without restarting the JVM: closes the old server (and remembers one
 * that would not close), optionally reformats the Raft storage, starts a new server on the same group and records the
 * replication position it restarted from, so the health monitor can tell a replication path that works again from one
 * that is still dead.
 * <p>
 * Split out of {@link RaftHAServer}, whose health monitor and divergence recovery ask for the restart.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class RatisInPlaceRestart {
  private final RaftHAServer ha;

  // Completed in-place restarts, by kind (issue #8900): a RECOVER restart keeps the Raft log, a FORMAT restart (the
  // divergence reformat) throws it away. Read by tests that must tell a recovery from a reformat.
  private final    AtomicInteger             recoverRestartCount   = new AtomicInteger();

  private final    AtomicInteger             formatRestartCount    = new AtomicInteger();

  /** Baseline whose values could not be read right after the restart; resolved by the next health tick. */
  static final InPlaceRestartBaseline PENDING_BASELINE = new InPlaceRestartBaseline(null, Long.MIN_VALUE, false);

  RatisInPlaceRestart(final RaftHAServer ha) {
    this.ha = ha;
  }

  /**
   * Records the baseline right after {@link #restartRatis(boolean)} started the new server, or leaves it pending when
   * the division cannot be read yet. A late read errs on the safe side: an entry that landed before it does not count
   * as proof, so the hold lasts until the next entry or leader change.
   */
  private void captureInPlaceRestartBaseline() {
    final InPlaceRestartBaseline baseline = readReplicationPosition();
    ha.inPlaceRestartBaseline.set(baseline != null ? baseline : PENDING_BASELINE);
  }

  /** The division's last log entry, current term and leader presence, or {@code null} when they cannot be read. */
  InPlaceRestartBaseline readReplicationPosition() {
    final RaftServer server = ha.raftServer;
    if (server == null)
      return null;
    try {
      final RaftServer.Division division = server.getDivision(ha.raftGroup.getGroupId());
      final DivisionInfo info = division.getInfo();
      return new InPlaceRestartBaseline(division.getRaftLog().getLastEntryTermIndex(), info.getCurrentTerm(),
          info.getLeaderId() != null);
    } catch (final Exception e) {
      // Same Ratis IllegalStateException window as isReadyForTraffic() (issue #5271).
      LogManager.instance().log(ha, Level.FINE, "Cannot read the Raft log position of the restarted division", e);
      return null;
    }
  }

  /**
   * Restarts the local Ratis server. When {@code formatStorage} is {@code true} the Raft storage
   * directory is deleted first and the server starts with {@link RaftStorage.StartupOption#FORMAT},
   * so this peer rejoins the group with an empty log and is reconciled by the leader via the
   * snapshot-install path. This is the in-process equivalent of an operator restarting a node whose
   * Raft log diverged (issue #4741). When {@code false} the existing storage is preserved and the
   * server starts with {@link RaftStorage.StartupOption#RECOVER} (the CLOSED/EXCEPTION recovery path).
   */
  void restartRatis(final boolean formatStorage) {
    synchronized (ha.recoveryLock) {
      if (ha.shutdownRequested) {
        HALog.log(ha, HALog.BASIC, "Recovery skipped: shutdown requested");
        return;
      }
      // Issue #8900: the waits below give up at once on an interrupted thread. Leave before closing anything, so the
      // client, broker and server stay as they are and the next health tick retries the whole restart.
      if (Thread.currentThread().isInterrupted()) {
        LogManager.instance().log(ha, Level.WARNING,
            "In-place Ratis restart called on an interrupted thread without a shutdown; skipped, the next health tick "
                + "retries it");
        return;
      }

      final int maxRetries = ha.configuration.getValueAsInteger(GlobalConfiguration.HA_RATIS_RESTART_MAX_RETRIES);
      if (ha.restartFailureCount >= maxRetries) {
        LogManager.instance().log(ha, Level.SEVERE,
            "Ratis restart failed %d consecutive times (max=%d). Stopping server for cluster-level recovery",
            ha.restartFailureCount, maxRetries);
        final Thread stopThread = new Thread(() -> {
          try {
            ha.arcadeServer.stop();
          } catch (final Exception ignored) {
          }
        }, "arcadedb-restart-failure-stop");
        stopThread.setDaemon(true);
        stopThread.start();
        return;
      }

      // Issue #9561: a close of the old server that outlived its bound on an earlier attempt may still hold the storage
      // lock and the gRPC ports. Starting a second server beside it is not an option, so this attempt fails as well and
      // counts toward the budget above, which stops the node once it is spent.
      final Thread stillClosing = ha.stuckRatisClose;
      if (stillClosing != null) {
        if (stillClosing.isAlive()) {
          ha.restartFailureCount++;
          LogManager.instance().log(ha, Level.SEVERE,
              "HealthMonitor recovery failed (attempt %d/%d): the close of the old Ratis server is still running on thread "
                  + "%s (its stack was logged when it timed out); not starting a new server beside it", ha.restartFailureCount,
              maxRetries, stillClosing.getName());
          return;
        }
        // The late close finished. this.raftServer may still be that old, now CLOSED server: closing it again below is
        // a no-op: LifeCycle.checkStateAndClose runs a Ratis server close at most once.
        ha.stuckRatisClose = null;
      }

      final RaftClient oldClient = ha.raftClient;
      final RaftServer oldServer = ha.raftServer;
      final RaftTransactionBroker oldBroker = ha.transactionBroker;

      // Issue #8900: the waits below end early on a shutdown (stop() does not take recoveryLock) or an interrupt (the
      // health monitor being stopped), and such an ending is not a failed restart.
      final BooleanSupplier abandoned = () -> ha.shutdownRequested || Thread.currentThread().isInterrupted();
      // Issue #8900: read before the close, which empties the server's group map.
      RaftServer.Division oldDivision = null;
      if (oldServer != null)
        try {
          oldDivision = oldServer.getDivision(ha.raftGroup.getGroupId());
        } catch (final Throwable t) {
          // No readable division (never started, or already removed): nothing to wait for below.
        }

      // Issue #8900: Ratis may already be closing this server on another thread - the JVM-pause monitor does it on its
      // own thread. close() below would then be a no-op that still INTERRUPTS that thread (pauseMonitor.stop()), cutting
      // its gRPC shutdown short, so let that close finish first. This reads the PROXY state: an #8651 zombie (proxy
      // RUNNING, division stuck CLOSING) does not wait here. The wait runs before anything is closed, so a restart
      // abandoned here leaves the client and broker usable for the next tick.
      LifeCycle.State beforeClose = null;
      if (oldServer != null) {
        try {
          beforeClose = OldRatisServerTermination.awaitCloseInProgress(oldServer::getLifeCycleState,
              OldRatisServerTermination.CLOSE_IN_PROGRESS_WAIT_MS, abandoned);
        } catch (final Throwable t) {
          LogManager.instance().log(ha, Level.FINE, "Cannot read the old Ratis server state: %s", t, t.getMessage());
        }
        if (abandoned.getAsBoolean()) {
          // stop() closes this server itself; closing it here too would only interrupt that close.
          HALog.log(ha, HALog.BASIC, "Recovery abandoned: shutdown or interrupt while the old Ratis server was closing");
          return;
        }
      }

      try {
        if (oldBroker != null)
          oldBroker.stop();
      } catch (final Throwable t) {
        LogManager.instance().log(ha, Level.FINE, "Error closing old broker: %s", t, t.getMessage());
      }
      try {
        if (oldClient != null)
          oldClient.close();
      } catch (final Throwable t) {
        LogManager.instance().log(ha, Level.FINE, "Error closing old client: %s", t, t.getMessage());
      }
      Thread closeStillRunning = null;
      try {
        if (oldServer != null) {
          if (beforeClose == LifeCycle.State.CLOSING)
            LogManager.instance().log(ha, Level.WARNING,
                "Old Ratis server is still CLOSING after %dms; closing it from the restart, which interrupts that close",
                OldRatisServerTermination.CLOSE_IN_PROGRESS_WAIT_MS);
          // Issue #9556 (the cache-eviction stop runs inside) and #9561 (the close is bounded): a close that never
          // returns would hold recoveryLock for good, and the health monitor could never retry.
          closeStillRunning = RatisServerCloser.close(oldServer, ratisCloseTimeoutMs());
          // Issue #8898: a close that returns with the server not CLOSED means a second server is about to start
          // beside one that may still answer the leader. A close Ratis already performed itself, or a gRPC shutdown
          // close() swallowed, reports CLOSED here: the gRPC check in the try block below covers those.
          if (closeStillRunning == null) {
            final LifeCycle.State afterClose = oldServer.getLifeCycleState();
            if (afterClose != LifeCycle.State.CLOSED)
              LogManager.instance().log(ha, Level.WARNING,
                  "Old Ratis server is %s after close(); restart proceeds anyway",
                  afterClose);
          }
        }
      } catch (final Throwable t) {
        LogManager.instance().log(ha, Level.WARNING, "Error closing old Ratis server before restart: %s", t, t.getMessage());
      }

      try {
        if (closeStillRunning != null) {
          // Issue #9561: remembered so the next attempts fail too while it runs, instead of closing it a second time. The
          // closer also returns early when this thread was interrupted: the catch below then sees the restart as
          // abandoned and does not count it, and the interrupt flag stays set for the health monitor to see.
          rememberStuckRatisClose(closeStillRunning);
          throw new IOException("The old Ratis server did not close within " + ratisCloseTimeoutMs()
              + "ms (or the wait was interrupted); not starting a new server beside one whose storage lock and gRPC ports "
              + "may still be held");
        }
        if (oldServer != null)
          verifyOldServerTerminated(oldServer, oldDivision, abandoned);
        // The waits above can take seconds, and stop() does not take recoveryLock: do not start a server it will not stop.
        if (abandoned.getAsBoolean()) {
          HALog.log(ha, HALog.BASIC, "Recovery abandoned: shutdown requested while the old Ratis server was closing");
          return;
        }

        // Issue #11921: not under a commit published outside the log, which holds the validation lock of the old ledger. An
        // interrupt at the gate leaves the flag set, so the catch below reads the restart as abandoned, not failed.
        ha.replaceStateMachineBetweenPublications(ratisCloseTimeoutMs());

        final RaftProperties properties = RaftPropertiesBuilder.build(ha.configuration);
        final File storageDir = ha.storageFiles.getRaftStorageDir();
        RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));

        // For a divergence reformat (#4741) discard the local Raft log so this peer rejoins as a fresh
        // bootstrapping member; otherwise recover the existing log in place (CLOSED/EXCEPTION path).
        final RaftStorage.StartupOption startupOption;
        if (formatStorage) {
          if (storageDir.exists()) {
            RaftStorageFiles.deleteRecursive(storageDir);
            // deleteRecursive swallows per-file failures (e.g. a locked file or a permission issue on
            // Windows). FORMAT on a non-empty directory is undefined behaviour, so abort instead of
            // proceeding: the throw is caught below, increments restartFailureCount, and the divergence
            // reformat budget already counted this as an attempt - so the failure escalates through the
            // existing retry/backoff machinery rather than starting Ratis on a half-deleted directory.
            if (storageDir.exists())
              throw new IOException("Could not fully delete Raft storage directory " + storageDir.getAbsolutePath()
                  + " before reformat; aborting to avoid FORMAT on a non-empty directory");
          }
          startupOption = RaftStorage.StartupOption.FORMAT;
        } else
          startupOption = RaftStorage.StartupOption.RECOVER;

        final Parameters recoveryParameters = ha.transport.buildParameters(ha.configuration);

        // Before the new server can take anything: a tick in between holds, and so does a start() that throws.
        ha.inPlaceRestartBaseline.set(PENDING_BASELINE);
        ha.raftServer = RaftServer.newBuilder()
            .setServerId(ha.localPeerId)
            .setGroup(ha.raftGroup)
            .setStateMachine(ha.stateMachine)
            .setProperties(properties)
            .setParameters(recoveryParameters)
            .setOption(startupOption)
            .build();
        ha.raftServer.start();
        if (ha.shutdownRequested) {
          // stop() may have read the old server before this one was published: close the new one here, a second close
          // from stop() is a no-op.
          // Issues #9556 and #9561. A close that outlives its bound is logged at SEVERE by the closer, and remembered so a
          // later restart attempt does not start a server beside it (stop() may already have read the server field).
          final Thread newServerStillClosing = RatisServerCloser.close(ha.raftServer, ratisCloseTimeoutMs());
          if (newServerStillClosing != null)
            rememberStuckRatisClose(newServerStillClosing);
          HALog.log(ha, HALog.BASIC, "Recovery abandoned: shutdown requested while the new Ratis server was starting");
          return;
        }
        captureInPlaceRestartBaseline();
        ha.raftProperties = properties;
        ha.raftClient = RaftHAServer.buildRaftClient(ha.raftGroup, properties, recoveryParameters);

        final int batchSize = ha.configuration.getValueAsInteger(GlobalConfiguration.HA_GROUP_COMMIT_BATCH_SIZE);
        final int queueSize = ha.configuration.getValueAsInteger(GlobalConfiguration.HA_GROUP_COMMIT_QUEUE_SIZE);
        final int offerTimeout = ha.configuration.getValueAsInteger(GlobalConfiguration.HA_GROUP_COMMIT_OFFER_TIMEOUT);
        // #4743: the effective per-entry ceiling is the SMALLER of the gRPC frame cap and the Ratis
        // appender byte limit - an entry above the latter makes the leader step down.
        final long grpcMessageSizeMax = RaftPropertiesBuilder.maxReplicatedEntrySize(ha.configuration);
        final long maxQueuedBytes = ha.configuration.getValueAsLong(GlobalConfiguration.HA_GROUP_COMMIT_MAX_QUEUED_BYTES);
        ha.transactionBroker = new RaftTransactionBroker(ha.raftClient, ha.quorum, ha.quorumTimeout, batchSize, queueSize,
            offerTimeout, grpcMessageSizeMax, maxQueuedBytes, ha::refreshRaftClient);

        ha.restartFailureCount = 0;
        (formatStorage ? formatRestartCount : recoverRestartCount).incrementAndGet();
        // The counters above, not this line, are the contract: GET /api/v1/cluster publishes them as localInPlaceRestarts
        // and the metrics as arcadedb.ha.in_place_restarts.* (issue #9429), and that is what the HA chaos harness reads
        LogManager.instance().log(ha, Level.INFO, "Ratis restarted in place (%s storage)", formatStorage ? "reformatted" : "recovered");
      } catch (final Throwable t) {
        if (abandoned.getAsBoolean()) {
          // A shutdown or an interrupt cut the verification short (issue #8900): not a failure of the restart, and not
          // one that may count toward stopping the node.
          HALog.log(ha, HALog.BASIC, "Recovery abandoned during shutdown: %s", t.getMessage());
          return;
        }
        ha.restartFailureCount++;
        LogManager.instance().log(ha, Level.SEVERE,
            "HealthMonitor recovery failed (attempt %d/%d): %s",
            t, ha.restartFailureCount,
            ha.configuration.getValueAsInteger(GlobalConfiguration.HA_RATIS_RESTART_MAX_RETRIES),
            t.getMessage());
      }
    }
  }

  /**
   * The Ratis server close of {@link RaftHAServer#stop()}, bounded (issue #9561). A server already CLOSING is being closed by another
   * thread: an in-place restart (stop() does not take recoveryLock), a restart close that timed out earlier, or Ratis's
   * own JVM-pause monitor. Calling close() again would be a no-op that still interrupts that close
   * ({@code pauseMonitor.stop()}, issue #8900), so stop() waits for it instead, with the same bound. Either way a close
   * that does not finish in time is logged at SEVERE and left running on its thread: the shutdown goes on rather than hang
   * the JVM shutdown hook behind it.
   */
  void closeRatisServerOnStop(final RaftServer server) throws IOException {
    final long timeoutMs = ratisCloseTimeoutMs();
    LifeCycle.State state = null;
    try {
      state = server.getLifeCycleState();
    } catch (final Throwable t) {
      // unreadable: close it below, which is what stop() always did
    }
    if (state == LifeCycle.State.CLOSING) {
      // 0 or negative means no bound; a year stands in for it without overflowing the deadline arithmetic
      final long waitMs = timeoutMs > 0 ? timeoutMs : TimeUnit.DAYS.toMillis(365);
      if (OldRatisServerTermination.awaitCloseInProgress(server::getLifeCycleState, waitMs, () -> false)
          == LifeCycle.State.CLOSING) {
        final Thread stuck = ha.stuckRatisClose;
        LogManager.instance().log(ha, Level.SEVERE,
            "Ratis HA service stopped while another thread%s was still closing its Ratis server after %dms; the storage "
                + "lock and gRPC ports may stay held until that close finishes (issue #9561)",
            stuck != null && stuck.isAlive() ? " (" + stuck.getName() + ")" : "", timeoutMs);
      }
      return;
    }
    final Thread stillClosing = RatisServerCloser.close(server, timeoutMs);
    if (stillClosing != null) {
      rememberStuckRatisClose(stillClosing);
      LogManager.instance().log(ha, Level.SEVERE,
          "Ratis HA service stopped without its Ratis server closing (thread %s); the storage lock and gRPC ports may "
              + "stay held until that close finishes", stillClosing.getName());
    }
  }

  /**
   * Records a Ratis close that outlived its bound (issue #9561), keeping an earlier one that is still running: the gate in
   * {@link #restartRatis} must wait for every close still in flight, and the oldest is the one it already reports. The
   * read-then-write is not atomic against stop(), which does not take recoveryLock; losing that race only drops a
   * reference on a node that is shutting down, where no restart reads it again.
   */
  private void rememberStuckRatisClose(final Thread closer) {
    final Thread previous = ha.stuckRatisClose;
    if (previous == null || !previous.isAlive())
      ha.stuckRatisClose = closer;
  }

  /** How long a Ratis server close may take before the caller stops waiting for it (issue #9561). */
  private long ratisCloseTimeoutMs() {
    return ha.configuration.getValueAsLong(GlobalConfiguration.HA_RATIS_CLOSE_TIMEOUT_MS);
  }

  /**
   * Refuses to start a new Ratis server while the old one can still answer the leader (issue #8900). A gRPC server of
   * the old instance that is still running after a bounded second shutdown fails the restart: the throw is counted
   * like any other failed restart, the health monitor retries on its next tick, and the existing retry budget stops
   * the node once it is spent. Starting anyway is what #8898 suspected: the leader keeps appending through the old
   * server's connection to a division that is CLOSED, while the new server reports RUNNING.
   * <p>
   * The old division's close runs asynchronously inside the proxy close and can still be running when an interrupted
   * close returns. That is only logged: the division does not own the gRPC listener, and refusing the restart for it
   * would also refuse the recovery of a division whose close never finishes (issue #8651).
   */
  private void verifyOldServerTerminated(final RaftServer oldServer, final RaftServer.Division oldDivision,
      final BooleanSupplier abandoned) throws IOException {
    final List<String> running = OldRatisServerTermination.terminateGrpcServers(
        OldRatisServerTermination.serversOf(oldServer.getServerRpc()), OldRatisServerTermination.GRPC_TERMINATION_WAIT_MS);
    if (!running.isEmpty())
      throw new IOException("The old Ratis server's gRPC services " + running + " did not terminate within "
          + OldRatisServerTermination.GRPC_TERMINATION_WAIT_MS + "ms; not starting a new server beside one that can still "
          + "answer the leader");

    if (oldDivision != null) {
      final LifeCycle.State divisionState = OldRatisServerTermination.awaitClosed(
          () -> oldDivision.getInfo().getLifeCycleState(), OldRatisServerTermination.DIVISION_CLOSE_WAIT_MS, abandoned);
      if (divisionState != LifeCycle.State.CLOSED && !abandoned.getAsBoolean())
        LogManager.instance().log(ha, Level.WARNING,
            "Old Ratis division is %s after its server closed; starting the new server anyway", divisionState);
    }
  }

  /**
   * Completed in-place restarts of this node's Raft layer since the process started, by storage outcome (issue #9429):
   * published by {@code GET /api/v1/cluster} as {@code localInPlaceRestarts} and by the metrics as
   * {@code arcadedb.ha.in_place_restarts.*}. The two counters are read one after the other, not as one snapshot; each
   * only grows, so a reader sees at worst a restart that completed between the two reads in one counter only.
   */
  public HAReplicationStatsProvider.InPlaceRestartStats getInPlaceRestartStats() {
    return new HAReplicationStatsProvider.InPlaceRestartStats(recoverRestartCount.get(), formatRestartCount.get());
  }

  /** Completed in-place restarts that kept the Raft log ({@code RECOVER}). Package-private: tests (issue #8900). */
  int getRecoverRestartCount() {
    return recoverRestartCount.get();
  }

  /** Completed in-place restarts that reformatted the Raft storage. Package-private: tests (issue #8900). */
  int getFormatRestartCount() {
    return formatRestartCount.get();
  }
}
