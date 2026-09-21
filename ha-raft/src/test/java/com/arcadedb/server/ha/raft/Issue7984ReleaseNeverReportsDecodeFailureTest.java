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

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #7984: the release of an entry's page-version reservations must never be the thing that reports a WAL
 * payload this node cannot read.
 * <p>
 * {@code applyTxEntry} releases the reservations in its {@code finally}, and with no {@code Pages} in hand
 * {@link PageVersionLedger#release} used to decode them out of the payload a third time. A payload that cannot be
 * decoded is exactly the state the apply has just failed on, so the release raised a second
 * {@link ReplicationException} from inside a {@code finally} - replacing, and later merely riding along on, the
 * {@link RaftLogEntryDecodeException} whose whole purpose is to quarantine the database for a targeted snapshot
 * resync (issues #7495, #7678). The release is bookkeeping; the apply result is not.
 * <p>
 * There is nothing for the release to report, either: every reservation enters the ledger through
 * {@link PageVersionLedger#validateAndReserve}, whose only two callers
 * ({@code ArcadeStateMachine.startTransaction} and {@code validateBeforeAppend(DatabaseInternal, byte[], EntryId)})
 * parse the very same bytes before they call it, so a payload {@code parse()} rejects is one this node never
 * reserved anything from.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7984ReleaseNeverReportsDecodeFailureTest {
  private static final String TYPE = "Counter";

  /**
   * Long enough for {@code peekWalTransactionId} to read the entry's transaction id (8 bytes), so the apply gets
   * past the claim and into the {@code try}/{@code finally}, and short of the 24-byte transaction header both
   * {@code deserializeWalTransaction} and {@link PageVersionLedger#parse} need. The apply therefore fails on the
   * decode it is supposed to fail on, and the release meets the same unreadable bytes.
   */
  private static final byte[] PEEKABLE_BUT_UNPARSEABLE = new byte[16];

  /** Too short even for the transaction id: what {@code releaseReservedVersions} can be handed directly. */
  private static final byte[] TRUNCATED = new byte[3];

  @TempDir
  Path tempDir;

  private LocalDatabase      db;
  private ArcadeStateMachine stateMachine;
  private RID                counter;

  @BeforeEach
  void setUp() {
    db = (LocalDatabase) new DatabaseFactory(tempDir.resolve("issue7984").toString()).create();
    db.getSchema().createDocumentType(TYPE, 1);
    db.transaction(() -> counter = db.newDocument(TYPE).set("value", 0L).save().getIdentity());
    stateMachine = new DatabaseBoundStateMachine(db);
  }

  @AfterEach
  void tearDown() {
    if (db != null && db.isOpen())
      db.drop();
  }

  /**
   * Entry point 1: {@code ArcadeStateMachine.releaseReservedVersions(String, byte[])}, the seam the apply's
   * {@code finally} and the #6965 unit tests both go through. With reservations in the ledger it used to decode the
   * payload and throw.
   */
  @Test
  void theReleaseSeamDoesNotReportAPayloadItCannotParse() {
    stateMachine.validateBeforeAppend(db, prepareIncrement(), entry(1));
    final int reserved = stateMachine.reservedPageVersions(db.getName());
    assertThat(reserved).isGreaterThan(0);

    assertThatCode(() -> stateMachine.releaseReservedVersions(db.getName(), TRUNCATED)).doesNotThrowAnyException();

    assertThat(stateMachine.reservedPageVersions(db.getName()))
        .as("a payload no reservation was taken from releases nothing, and must take nothing away")
        .isEqualTo(reserved);
  }

  /**
   * Entry point 2: the apply path itself. A committed entry this node cannot decode must be reported as the
   * {@link RaftLogEntryDecodeException} that routes it to the per-database quarantine, with nothing from the release
   * bookkeeping attached to it - not as the failure, and not as a suppressed rider on it.
   */
  @Test
  void aCorruptCommittedEntryReportsTheApplyFailureAlone() {
    stateMachine.validateBeforeAppend(db, prepareIncrement(), entry(1));
    final int reserved = stateMachine.reservedPageVersions(db.getName());
    assertThat(reserved).isGreaterThan(0);

    final CompletableFuture<Message> future = stateMachine.applyTransaction(
        txEntry(stateMachine, db.getName(), PEEKABLE_BUT_UNPARSEABLE, 1L, 5L));

    assertThat(future.isCompletedExceptionally()).isTrue();
    final Throwable applyFailure = rootApplyFailure(catchThrowable(future::join));
    assertThat(applyFailure)
        .as("the entry is undecodable, which is what the per-database quarantine exists for")
        .isInstanceOf(RaftLogEntryDecodeException.class);
    assertThat(applyFailure.getSuppressed())
        .as("the release bookkeeping must not report the decode failure, not even alongside the apply's own")
        .isEmpty();

    assertThat(stateMachine.isDatabaseDiverged(db.getName())).isTrue();
    assertThat(stateMachine.isHaltedAfterCriticalError()).isFalse();
    assertThat(stateMachine.reservedPageVersions(db.getName()))
        .as("the reservations of the OTHER in-flight entry are not this entry's to discard")
        .isEqualTo(reserved);
  }

  /**
   * Entry point 3: {@link PageVersionLedger#release} on its own. The ledger has to hold reservations for the
   * database before the decode is reached at all - {@code release} returns early on an empty ledger, which is why a
   * follower never met this - so the unit pins the contract rather than the caller's guard.
   */
  @Test
  void theLedgerReleaseIsTotalOverTheWalPayload() throws IOException {
    final PageVersionLedger ledger = new PageVersionLedger();
    ledger.validateAndReserve("ledger", PageVersionLedger.parse(wal(1L, 3, 0, 1)), entry(1), (fileId, pageNumber) -> 0);
    assertThat(ledger.reservedPages("ledger")).isEqualTo(1);

    assertThatCode(() -> ledger.release("ledger", null, PEEKABLE_BUT_UNPARSEABLE)).doesNotThrowAnyException();
    assertThatCode(() -> ledger.release("ledger", null, TRUNCATED)).doesNotThrowAnyException();
    assertThatCode(() -> ledger.release("ledger", null, null)).doesNotThrowAnyException();

    assertThat(ledger.reservedPages("ledger")).isEqualTo(1);

    // A payload it CAN parse still releases, so the quiet return is scoped to the bytes it cannot read.
    ledger.release("ledger", null, wal(1L, 3, 0, 1));
    assertThat(ledger.reservedPages("ledger")).isZero();
  }

  /**
   * The release counter must be bumped before the decode is attempted, not after. It is what tells a validation
   * whether the local page versions it read outside the ledger lock may have moved under it
   * ({@code validateAndReserve}'s early-read optimisation); an apply moves the local copy whether or not this ledger
   * still holds the entry's reservation, so skipping the bump on an unreadable payload would let a validation trust
   * a stale read. Reading the counter needs reflection, the same way {@code PageVersionLedgerTest} ages a
   * reservation.
   */
  @Test
  void aReleaseItCannotParseIsStillCounted() throws Exception {
    final PageVersionLedger ledger = new PageVersionLedger();
    ledger.validateAndReserve("ledger", PageVersionLedger.parse(wal(1L, 3, 0, 1)), entry(1), (fileId, pageNumber) -> 0);
    final long before = releaseCount(ledger, "ledger");

    ledger.release("ledger", null, PEEKABLE_BUT_UNPARSEABLE);

    assertThat(releaseCount(ledger, "ledger")).isEqualTo(before + 1);
  }

  /**
   * The claim the quiet return rests on, from the other side: giving up on a payload is safe only while
   * {@link PageVersionLedger#parse} is no stricter than the decode the apply itself runs. If it ever became
   * stricter, a payload that applied cleanly could fail the release, and the reservations of an entry that really
   * did move the local copies would be dropped silently rather than reported. The two read the same layout with the
   * same bounds checks; {@code PageVersionLedgerTest.theHeaderWalkAgreesWithTheFullDeserialization} pins that they
   * agree page for page on a well-formed entry, and this pins that they agree on a malformed one.
   */
  @Test
  void theHeaderWalkIsNoStricterThanTheDecodeTheApplyRuns() {
    final byte[] tooManyPages = wal(1L, 3, 0, 1);
    ByteBuffer.wrap(tooManyPages).putInt(2 * Long.BYTES, 1_000_000);
    final byte[] negativePageCount = wal(1L, 3, 0, 1);
    ByteBuffer.wrap(negativePageCount).putInt(2 * Long.BYTES, -7);
    final byte[] invalidDeltaRange = wal(1L, 3, 0, 1);
    ByteBuffer.wrap(invalidDeltaRange).putInt(2 * Long.BYTES + 2 * Integer.BYTES + 3 * Integer.BYTES, -1);

    for (final byte[] payload : new byte[][] { PEEKABLE_BUT_UNPARSEABLE, TRUNCATED, tooManyPages, negativePageCount,
        invalidDeltaRange, wal(1L, 3, 0, 1) }) {
      final boolean headerWalkRejects = catchThrowable(() -> PageVersionLedger.parse(payload)) != null;
      final boolean applyDecodeRejects = catchThrowable(() -> ArcadeStateMachine.deserializeWalTransaction(payload)) != null;
      assertThat(headerWalkRejects)
          .as("parse() and deserializeWalTransaction() must agree on whether these %d bytes are readable", payload.length)
          .isEqualTo(applyDecodeRejects);
    }
  }

  private static Throwable rootApplyFailure(final Throwable thrown) {
    // join() wraps in a CompletionException, and handleUnexpectedApplyError wraps the apply's own failure in the
    // ReplicationException that tells the caller a per-database resync is under way.
    Throwable t = thrown;
    while (t.getCause() != null && !(t instanceof RaftLogEntryDecodeException))
      t = t.getCause();
    return t;
  }

  private static long releaseCount(final PageVersionLedger ledger, final String database) throws Exception {
    final Field byDatabase = PageVersionLedger.class.getDeclaredField("byDatabase");
    byDatabase.setAccessible(true);
    final Object databaseLedger = ((Map<?, ?>) byDatabase.get(ledger)).get(database);
    final Field releases = databaseLedger.getClass().getDeclaredField("releases");
    releases.setAccessible(true);
    return ((AtomicLong) releases.get(databaseLedger)).get();
  }

  private static PageVersionLedger.EntryId entry(final long callId) {
    return new PageVersionLedger.EntryId("client", callId);
  }

  private static org.apache.ratis.statemachine.TransactionContext txEntry(final ArcadeStateMachine sm,
      final String databaseName, final byte[] walData, final long term, final long index) {
    final ByteString payload = RaftLogEntryCodec.encodeTxEntry(databaseName, walData, Collections.emptyMap());
    final LogEntryProto logEntry = LogEntryProto.newBuilder()
        .setTerm(term)
        .setIndex(index)
        .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(payload).build())
        .build();
    return org.apache.ratis.statemachine.TransactionContext.newBuilder()
        .setStateMachine(sm)
        .setLogEntry(logEntry)
        .build();
  }

  /** The wire layout {@code deserializeWalTransaction} reads, with a single page and a 4-byte delta. */
  private static byte[] wal(final long txId, final int fileId, final int pageNumber, final int targetVersion) {
    final int deltaSize = 4;
    final ByteBuffer buf = ByteBuffer.allocate(2 * Long.BYTES + 2 * Integer.BYTES + 6 * Integer.BYTES + deltaSize);
    buf.putLong(txId);
    buf.putLong(System.currentTimeMillis());
    buf.putInt(1);
    buf.putInt(6 * Integer.BYTES + deltaSize);
    buf.putInt(fileId);
    buf.putInt(pageNumber);
    buf.putInt(0);              // changesFrom
    buf.putInt(deltaSize - 1);  // changesTo
    buf.putInt(targetVersion);
    buf.putInt(4096);           // currentPageSize
    buf.put(new byte[deltaSize]);
    return buf.array();
  }

  private byte[] prepareIncrement() {
    db.begin();
    final Document doc = db.lookupByRID(counter, true).asDocument();
    doc.modify().set("value", doc.getLong("value") + 1).save();
    final TransactionContext tx = db.getTransaction();
    final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(true);
    final byte[] walData = phase1.result.toByteArray();
    tx.rollback();
    return walData;
  }

  /** A state machine with no server attached, whose only database is the one this test opened. */
  private static class DatabaseBoundStateMachine extends ArcadeStateMachine {
    private final DatabaseInternal database;

    DatabaseBoundStateMachine(final DatabaseInternal database) {
      this.database = database;
    }

    @Override
    DatabaseInternal databaseFor(final String databaseName) {
      return database;
    }
  }
}
