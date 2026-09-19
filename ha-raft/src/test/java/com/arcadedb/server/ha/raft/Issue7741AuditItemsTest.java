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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.FileManager;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PageManager;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ha.raft.ArcadeStateMachine.LocalResyncState;
import com.arcadedb.server.http.handler.LeaderDial;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #7741: five low-severity audit items. Four of them are covered here; the fifth is a javadoc whose stated
 * reason was not true, pinned in {@code Issue7741LineProtocolWriteStaysInThreadTest} in the engine module, where
 * the fact it asserts lives.
 * <ol>
 * <li>{@code arcadedb.ha.proxyConnectTimeout} had no effect on an SSL cluster: the HTTPS peer client used a
 * hardcoded 5s, while the setting's own description named this path among the ones it governs.</li>
 * <li>{@code HA_PROXY_BATCH_READ_TIMEOUT}'s description named HTTP 503; the code answers 504 on expiry, so a
 * support runbook written from the description looked for the wrong code.</li>
 * <li>{@code applyTxEntry}'s {@code finally} could throw and REPLACE the failure being reported - restoring, for
 * the exact case #7495 exists to remove, the silent skip that issue removed.</li>
 * <li>The quarantine alert described every quarantine as "after a WAL version gap", so an operator whose node
 * quarantined a database on an entry it could not decode - a corrupt LOCAL log segment - was pointed at the
 * leader.</li>
 * </ol>
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7741">issue #7741</a>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7741AuditItemsTest {

  // ---- 1. the HTTPS connect timeout ----

  @Test
  void theHttpsPeerClientReadsTheConfiguredConnectTimeout() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT, 1_234L);

    assertThat(TrustedHttpClientCache.connectTimeoutOf(configuration))
        .as("the setting governs the TLS dial too, not only the plain-HTTP one")
        .isEqualTo(Duration.ofMillis(1_234L));
  }

  @Test
  void aNonPositiveConnectTimeoutIsClampedRatherThanUnbounded() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT, 0L);

    assertThat(TrustedHttpClientCache.connectTimeoutOf(configuration))
        .isEqualTo(Duration.ofMillis(LeaderDial.MIN_FORWARD_TIMEOUT_MS));
  }

  @Test
  void theDefaultIsUnchanged() {
    assertThat(TrustedHttpClientCache.connectTimeoutOf(new ContextConfiguration()))
        .as("an operator who never touched the setting sees what they saw before")
        .isEqualTo(Duration.ofMillis(
            (Long) GlobalConfiguration.HA_PROXY_CONNECT_TIMEOUT.getDefValue()));
  }

  // ---- 2. the batch-timeout description ----

  @Test
  void theBatchReadTimeoutDescriptionNamesTheStatusCodeTheCodeAnswers() {
    final String description = GlobalConfiguration.HA_PROXY_BATCH_READ_TIMEOUT.getDescription();

    assertThat(description).as("PostBatchHandler answers 504 on expiry").contains("504");
    assertThat(description).as("and not the 503 it used to promise").doesNotContain("503");
  }

  // ---- 3. the release that could replace the apply failure ----

  /**
   * A leader applying an entry it did not append holds no {@code Pages}, so the release parses them out of the WAL
   * payload - the very payload the apply has just failed to read. The parse throws, and in a {@code finally} that
   * throw replaces the {@link RaftLogEntryDecodeException} that would have quarantined the database.
   */
  @Test
  void aReleaseThatCannotParseTheWalDoesNotSwallowTheDecodeFailure() {
    // databaseFor() returning null gets the apply past the database lookup without a server, so what fails is the
    // WAL decode INSIDE the try - which is the only arm the finally can steal the failure from.
    final ArcadeStateMachine sm = new ArcadeStateMachine() {
      @Override
      DatabaseInternal databaseFor(final String databaseName) {
        return null;
      }
    };
    reserveOnePage(sm, "db-A");

    // Readable transaction id, corrupt beyond it: past the read that happens BEFORE the try, and unparseable for
    // the release in the finally, which has no Pages of its own here and has to parse them out of this payload.
    final ByteBuffer corrupt = ByteBuffer.allocate(2 * Long.BYTES + 2 * Integer.BYTES);
    corrupt.putLong(1L);  // txId
    corrupt.putLong(0L);  // timestamp
    corrupt.putInt(-7);   // pageCount: the corruption of issue #4420
    corrupt.putInt(0);    // segmentSize

    final CompletableFuture<Message> future = sm.applyTransaction(txEntry(sm, "db-A", corrupt.array(), 5L));

    final Throwable failure = catchThrowable(future::join);
    assertThat(failure.getCause())
        .as("the apply's own failure is what the caller is told about")
        .isInstanceOf(ReplicationException.class);
    assertThat(failure.getCause().getCause())
        .as("and it still carries the decode failure #7495 added")
        .isInstanceOf(RaftLogEntryDecodeException.class);
    assertThat(sm.isDatabaseDiverged("db-A"))
        .as("so the database is quarantined instead of the entry being skipped in silence")
        .isTrue();
    assertThat(sm.isHaltedAfterCriticalError()).isFalse();
  }

  // ---- 4. the quarantine cause in the alert ----

  @Test
  void theAlertNamesAnUndecodableEntryAsSuchRatherThanAsAWalGap() {
    final JSONArray alerts = new JSONArray();
    ClusterAlerts.addLocalResyncAlert(new LocalResyncState(false, false, -1, Map.of(), Map.of("db-A", DivergenceCause.UNDECODABLE_LOG_ENTRY)), null, alerts);

    final JSONObject alert = alerts.getJSONObject(0);
    assertThat(alert.getString("severity")).isEqualTo(ClusterAlerts.SEVERITY_CRITICAL);
    assertThat(alert.getString("message"))
        .as("the operator is told to look at this node's log segment, not at the leader")
        .contains("cannot decode")
        .doesNotContain("WAL version gap");
    assertThat(alert.getJSONObject("details").getJSONObject("divergenceCauses").getString("db-A"))
        .isEqualTo(DivergenceCause.UNDECODABLE_LOG_ENTRY.name());
  }

  @Test
  void aWalGapIsStillDescribedAsAWalGap() {
    final JSONArray alerts = new JSONArray();
    ClusterAlerts.addLocalResyncAlert(new LocalResyncState(false, false, -1, Map.of(), Map.of("db-A", DivergenceCause.WAL_VERSION_GAP)), null, alerts);

    assertThat(alerts.getJSONObject(0).getString("message")).contains("WAL version gap");
  }

  /** Two databases quarantined for two different reasons say both, once each. */
  @Test
  void twoCausesAreBothNamedAndNeitherIsRepeated() {
    final JSONArray alerts = new JSONArray();
    ClusterAlerts.addLocalResyncAlert(new LocalResyncState(false, false, -1, Map.of(),
        Map.of("db-A", DivergenceCause.WAL_VERSION_GAP, "db-B", DivergenceCause.UNDECODABLE_LOG_ENTRY,
            "db-C", DivergenceCause.WAL_VERSION_GAP)), null, alerts);

    final String message = alerts.getJSONObject(0).getString("message");
    assertThat(message).contains("WAL version gap").contains("cannot decode");
    assertThat(message.split("WAL version gap", -1).length - 1).as("said once, not once per database").isEqualTo(1);
  }

  /** A node held back only by a read floor has no quarantine to describe, and says so without inventing one. */
  @Test
  void aReadFloorWithNoQuarantineNamesNoCause() {
    final JSONArray alerts = new JSONArray();
    ClusterAlerts.addLocalResyncAlert(
        new LocalResyncState(false, false, 100L, Map.of(), Map.of()), null, alerts);

    assertThat(alerts.getJSONObject(0).getString("message"))
        .contains("clamped at a read floor")
        .doesNotContain("quarantined");
  }

  /**
   * And a node that is BOTH says each thing once. The message used to name the quarantine's cause and then offer
   * the read floor as an alternative - "A, or clamped at a read floor because a snapshot install did not bring it
   * up to date" - which for a quarantine caused BY an incomplete install described one event as two
   * (code review on PR #7747).
   */
  @Test
  void aQuarantineAndAReadFloorAreEachSaidOnce() {
    final JSONArray alerts = new JSONArray();
    ClusterAlerts.addLocalResyncAlert(new LocalResyncState(false, false, -1, Map.of("db-A", 7L),
        Map.of("db-A", DivergenceCause.SNAPSHOT_INSTALL_INCOMPLETE)), null, alerts);

    final String message = alerts.getJSONObject(0).getString("message");
    assertThat(message).contains("quarantined after a snapshot install").contains("clamped at a read floor");
    assertThat(message.split("snapshot install", -1).length - 1)
        .as("the install is the cause AND the reason for the floor: said once")
        .isEqualTo(1);
  }

  /** The first cause a quarantine is recorded with is the one it keeps, and a resync clears the slate. */
  @Test
  void theFirstCauseWinsUntilTheResyncClearsIt() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();

    sm.markStateDiverged("db-A", DivergenceCause.WAL_VERSION_GAP);
    sm.markStateDiverged("db-A", DivergenceCause.APPLY_ERROR);

    assertThat(sm.getLocalResyncState().divergenceCauses())
        .as("a quarantined database goes on failing; the first cause is the one that describes why")
        .containsExactly(entry("db-A", DivergenceCause.WAL_VERSION_GAP));

    sm.clearDivergedDatabase("db-A");
    sm.markStateDiverged("db-A", DivergenceCause.APPLY_ERROR);

    assertThat(sm.getLocalResyncState().divergenceCauses())
        .as("and the next quarantine records afresh")
        .containsExactly(entry("db-A", DivergenceCause.APPLY_ERROR));
  }

  // ---- helpers ----

  private static TransactionContext txEntry(final ArcadeStateMachine sm, final String databaseName,
      final byte[] walData, final long index) {
    final ByteString payload = RaftLogEntryCodec.encodeTxEntry(databaseName, walData, Collections.emptyMap());
    final LogEntryProto logEntry = LogEntryProto.newBuilder()
        .setTerm(1L)
        .setIndex(index)
        .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(payload).build())
        .build();
    return TransactionContext.newBuilder().setStateMachine(sm).setLogEntry(logEntry).build();
  }

  /**
   * Leaves one page reserved for {@code databaseName}, which is what makes the release on the apply path do any
   * work at all: with an empty ledger - every follower - it returns before parsing anything.
   */
  private static void reserveOnePage(final ArcadeStateMachine sm, final String databaseName) {
    final DatabaseInternal db = mock(DatabaseInternal.class);
    final FileManager fileManager = mock(FileManager.class);
    final PageManager pageManager = mock(PageManager.class);
    final PaginatedComponentFile file = mock(PaginatedComponentFile.class);

    when(db.getName()).thenReturn(databaseName);
    when(db.getFileManager()).thenReturn(fileManager);
    when(db.getPageManager()).thenReturn(pageManager);
    when(fileManager.existsFile(anyInt())).thenReturn(true);
    when(fileManager.getFile(anyInt())).thenReturn(file);
    when(file.getPageSize()).thenReturn(1024);
    try {
      when(pageManager.getMostRecentVersionOfPage(any(PageId.class), anyInt())).thenReturn(0);
    } catch (final IOException e) {
      throw new IllegalStateException(e);
    }

    sm.validateBeforeAppend(db, walWithOnePage(), new PageVersionLedger.EntryId("client", 1L));
    assertThat(sm.reservedPageVersions(databaseName)).isEqualTo(1);
  }

  /** The minimal well-formed WAL payload {@code PageVersionLedger.parse} accepts: one page, one 4-byte delta. */
  private static byte[] walWithOnePage() {
    final int deltaSize = 4;
    final ByteBuffer buf = ByteBuffer.allocate(2 * Long.BYTES + 2 * Integer.BYTES + 6 * Integer.BYTES + deltaSize);
    buf.putLong(1L);                       // txId
    buf.putLong(System.currentTimeMillis());
    buf.putInt(1);                         // pageCount
    buf.putInt(6 * Integer.BYTES + deltaSize);
    buf.putInt(3);                         // fileId
    buf.putInt(0);                         // pageNumber
    buf.putInt(64);                        // changesFrom
    buf.putInt(64 + deltaSize - 1);        // changesTo
    buf.putInt(1);                         // target version
    buf.putInt(1024);                      // currentPageSize
    buf.put(new byte[deltaSize]);
    return buf.array();
  }
}
