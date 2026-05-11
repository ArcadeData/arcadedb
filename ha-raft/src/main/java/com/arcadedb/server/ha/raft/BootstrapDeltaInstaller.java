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
import com.arcadedb.server.ArcadeDBServer;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.logging.Level;

/**
 * Tries the transaction-delta endpoint first; falls back to the existing full-snapshot path on
 * {@code 412 Precondition Failed} (Ratis log doesn't cover the gap), {@code 5xx}, or any I/O failure.
 * <p>
 * Issue #4147 phase 6. Used by the bootstrap mismatch path
 * ({@code ArcadeStateMachine.applyBootstrapFingerprintEntry}) and, in time, the runtime catch-up
 * path ({@code SnapshotInstaller}'s callers). The wrapper deliberately does not change the
 * full-snapshot install machinery in any way - all behaviour preserved on the fallback - so this
 * code can ship before Ratis-log delta serving lands.
 * <p>
 * <b>Status:</b> the delta endpoint is currently a 412-only stub (phase 6a scaffolding). Real
 * Ratis-log delta serving is the follow-up. When that ships, the same wrapper picks up delta
 * success automatically; no caller-side change.
 */
final class BootstrapDeltaInstaller {

  private static final HttpClient HTTP = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(5))
      .build();

  private BootstrapDeltaInstaller() {
  }

  /**
   * Bring the local database up to the source's state.
   *
   * @param dbName            database name on this peer.
   * @param databasePath      where the database files live (existing dir we may need to overwrite).
   * @param sourceHttpAddr    HTTP address of the bootstrap source peer (host:port). Often the
   *                          current Raft leader, but not required to be.
   * @param clusterToken      cluster token for inter-peer auth.
   * @param server            the local server, needed by SnapshotInstaller for the fallback path.
   * @param localLastTxId     this peer's persisted lastTxId; passed to the delta endpoint as
   *                          {@code fromTxId}.
   * @param sourceLastTxId    the source's lastTxId from the bootstrap entry; used for gap-vs-threshold check.
   */
  static void installDeltaOrSnapshot(final String dbName, final String databasePath,
      final String sourceHttpAddr, final String clusterToken, final ArcadeDBServer server,
      final long localLastTxId, final long sourceLastTxId) throws IOException {
    final long deltaThreshold = server.getConfiguration()
        .getValueAsLong(GlobalConfiguration.HA_BOOTSTRAP_DELTA_THRESHOLD);

    final long gap = sourceLastTxId - localLastTxId;
    if (deltaThreshold > 0 && gap > 0 && gap <= deltaThreshold && sourceHttpAddr != null) {
      // Gap is within the threshold; ask the source to ship just the delta. On failure (most
      // commonly 412 because Ratis-log delta serving isn't implemented yet) fall through to the
      // full snapshot path.
      if (tryDelta(dbName, sourceHttpAddr, clusterToken, localLastTxId, gap))
        return;
    } else if (gap > deltaThreshold) {
      LogManager.instance().log(BootstrapDeltaInstaller.class, Level.INFO,
          "Bootstrap mismatch for '%s': gap=%d > threshold=%d, going straight to full snapshot",
          dbName, gap, deltaThreshold);
    }

    // Fallback: full leader-shipped snapshot, identical to the legacy path. This is the
    // pre-#4147 behaviour and stays the only safe choice until Ratis-log delta serving lands.
    SnapshotInstaller.install(dbName, databasePath, sourceHttpAddr, clusterToken, server);
  }

  /**
   * Attempt the delta endpoint. Returns {@code true} if the local database was successfully
   * brought up to date via transaction-delta replay; {@code false} on 412 / 5xx / I/O failure
   * (caller falls back to full snapshot).
   */
  private static boolean tryDelta(final String dbName, final String sourceHttpAddr, final String clusterToken,
      final long fromTxId, final long expectedGap) {
    final String url = "http://" + sourceHttpAddr + "/api/v1/ha/delta/" + dbName + "?fromTxId=" + fromTxId;
    final HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .timeout(Duration.ofSeconds(60))
        .GET();
    if (clusterToken != null && !clusterToken.isBlank())
      builder.header("X-ArcadeDB-Cluster-Token", clusterToken);

    try {
      final HttpResponse<String> resp = HTTP.send(builder.build(), HttpResponse.BodyHandlers.ofString());
      switch (resp.statusCode()) {
      case 200:
        // Phase 6b will land here. For now the endpoint never returns 200; treating as
        // "delta applied" is correct only when the response body has been consumed and replayed.
        LogManager.instance().log(BootstrapDeltaInstaller.class, Level.WARNING,
            "Delta endpoint for '%s' returned 200 but phase 6a does not yet apply the body; falling back to full snapshot",
            dbName);
        return false;
      case 204:
        LogManager.instance().log(BootstrapDeltaInstaller.class, Level.INFO,
            "Delta endpoint for '%s' returned 204 No Content; nothing to apply", dbName);
        return true;
      case 412:
        LogManager.instance().log(BootstrapDeltaInstaller.class, Level.INFO,
            "Delta unavailable for '%s' (gap=%d): %s; falling back to full snapshot",
            dbName, expectedGap, summarize(resp.body()));
        return false;
      default:
        LogManager.instance().log(BootstrapDeltaInstaller.class, Level.WARNING,
            "Delta endpoint for '%s' returned HTTP %d; falling back to full snapshot",
            dbName, resp.statusCode());
        return false;
      }
    } catch (final Exception e) {
      LogManager.instance().log(BootstrapDeltaInstaller.class, Level.WARNING,
          "Delta endpoint for '%s' failed (%s); falling back to full snapshot", dbName, e.getMessage());
      return false;
    }
  }

  private static String summarize(final String body) {
    if (body == null)
      return "<empty body>";
    final String trimmed = body.trim();
    return trimmed.length() <= 200 ? trimmed : trimmed.substring(0, 197) + "...";
  }
}
