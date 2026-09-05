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

import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #7138: Raft log entries carry no version field - the type byte is the whole
 * envelope - and only {@code SCHEMA_ENTRY} tolerated trailing bytes. For the other five types an unknown
 * trailing field was fatal, and fatal in the worst arm: {@code RaftLogEntryCodec.decode} runs OUTSIDE
 * {@code applyWithRetry}, so the throw was neither a {@code NeedRetryException} nor a
 * {@code ReplicationException} and could not be quarantined - it reached {@code catch (Throwable)} and halted
 * the node, with the applied index deliberately not advanced so the halt repeated on every restart.
 * <p>
 * A newer leader adding one field to {@code DROP_DATABASE_ENTRY} would therefore have permanently halted every
 * not-yet-upgraded peer: two nodes down during a rolling upgrade of a three-node cluster.
 * <p>
 * The fix gives every type the extension mechanism {@code SCHEMA_ENTRY} demonstrated the need for, framed so
 * the corruption signal the old check existed for is kept rather than traded away: a deliberate extension
 * carries {@code RaftLogEntryCodec.EXTENSION_MAGIC}, garbage does not.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7138EntryExtensionSectionTest {

  private static final String DB = "mydb";

  /** What a future release adds: one more field on an entry type that has no extension mechanism today. */
  private static ByteString dropDatabaseEntryFromANewerNode() {
    return RaftLogEntryCodec.appendExtensionSection(RaftLogEntryCodec.encodeDropDatabaseEntry(DB),
        "a field this version has never heard of".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void aFieldAppendedByANewerNodeIsSkippedInsteadOfHaltingTheDecoder() {
    final RaftLogEntryCodec.DecodedEntry decoded = RaftLogEntryCodec.decode(dropDatabaseEntryFromANewerNode());

    assertThat(decoded.type()).isEqualTo(RaftLogEntryType.DROP_DATABASE_ENTRY);
    assertThat(decoded.databaseName()).isEqualTo(DB);
  }

  /** Every type, not just the one: the point of #7138 is that the constraint stops being per-type folklore. */
  @Test
  void everyEntryTypeToleratesATrailingExtensionSection() {
    final byte[] future = new byte[] { 1, 2, 3, 4 };

    assertThat(RaftLogEntryCodec.decode(RaftLogEntryCodec.appendExtensionSection(
        RaftLogEntryCodec.encodeTxEntry(DB, new byte[0], Map.of()), future)).type())
        .isEqualTo(RaftLogEntryType.TX_ENTRY);

    assertThat(RaftLogEntryCodec.decode(RaftLogEntryCodec.appendExtensionSection(
        RaftLogEntryCodec.encodeDropDatabaseEntry(DB), future)).type())
        .isEqualTo(RaftLogEntryType.DROP_DATABASE_ENTRY);

    assertThat(RaftLogEntryCodec.decode(RaftLogEntryCodec.appendExtensionSection(
        RaftLogEntryCodec.encodeSecurityUsersEntry("[]"), future)).type())
        .isEqualTo(RaftLogEntryType.SECURITY_USERS_ENTRY);

    assertThat(RaftLogEntryCodec.decode(RaftLogEntryCodec.appendExtensionSection(
        RaftLogEntryCodec.encodeBootstrapFingerprintEntry(DB, "fp", 42L), future)).type())
        .isEqualTo(RaftLogEntryType.BOOTSTRAP_FINGERPRINT_ENTRY);
  }

  /** Several fields added over several releases stack up; a decoder skips all of them. */
  @Test
  void repeatedExtensionSectionsAreAllSkipped() {
    ByteString entry = RaftLogEntryCodec.encodeDropDatabaseEntry(DB);
    entry = RaftLogEntryCodec.appendExtensionSection(entry, new byte[] { 9 });
    entry = RaftLogEntryCodec.appendExtensionSection(entry, new byte[0]);
    entry = RaftLogEntryCodec.appendExtensionSection(entry, new byte[] { 7, 7, 7 });

    assertThat(RaftLogEntryCodec.decode(entry).databaseName()).isEqualTo(DB);
  }

  /**
   * The half that must NOT be traded away: tolerating anything trailing (what {@code SCHEMA_ENTRY} does) would
   * make every type extensible but would stop reporting a truncated or corrupt entry at all.
   */
  @Test
  void unframedTrailingBytesAreStillReportedAsCorruption() throws Exception {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    baos.write(RaftLogEntryCodec.encodeDropDatabaseEntry(DB).toByteArray());
    baos.write(new byte[] { 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42 });

    assertThatThrownBy(() -> RaftLogEntryCodec.decode(ByteString.copyFrom(baos.toByteArray())))
        .isInstanceOf(RaftLogEntryDecodeException.class)
        .hasMessageContaining("DROP_DATABASE_ENTRY")
        .rootCause()
        .hasMessageContaining("not an extension section");
  }

  /** A tail too short to even be a frame header is corruption, not an empty extension. */
  @Test
  void aTailTooShortForAFrameHeaderIsCorruption() throws Exception {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    baos.write(RaftLogEntryCodec.encodeDropDatabaseEntry(DB).toByteArray());
    baos.write(new byte[] { 0x41, 0x44 });

    assertThatThrownBy(() -> RaftLogEntryCodec.decode(ByteString.copyFrom(baos.toByteArray())))
        .isInstanceOf(RaftLogEntryDecodeException.class)
        .rootCause()
        .hasMessageContaining("too few for an extension section header");
  }

  /** A frame whose length runs past the end of the entry is a truncation, and must not be skipped over. */
  @Test
  void anExtensionSectionTruncatedInFlightIsCorruption() throws Exception {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final DataOutputStream dos = new DataOutputStream(baos);
    RaftLogEntryCodec.encodeDropDatabaseEntry(DB).writeTo(dos);
    dos.writeInt(RaftLogEntryCodec.EXTENSION_MAGIC);
    dos.writeInt(64);            // claims 64 bytes...
    dos.write(new byte[] { 1 }); // ...and carries 1
    dos.flush();

    assertThatThrownBy(() -> RaftLogEntryCodec.decode(ByteString.copyFrom(baos.toByteArray())))
        .isInstanceOf(RaftLogEntryDecodeException.class)
        .rootCause()
        .hasMessageContaining("extension section");
  }

  /**
   * The second half of #7138: a decode failure on ONE database must not be a whole-node halt. It is isolable
   * exactly like an apply error on that database, so the node quarantines it, resyncs it from the leader and
   * keeps serving its other databases - and the entry is still never silently skipped.
   */
  @Test
  void aCorruptEntryQuarantinesItsDatabaseInsteadOfHaltingTheNode() throws Exception {
    final ArcadeStateMachine sm = new ArcadeStateMachine();

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    baos.write(RaftLogEntryCodec.encodeDropDatabaseEntry("db-A").toByteArray());
    baos.write(new byte[] { 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42 });

    final CompletableFuture<Message> future = sm.applyTransaction(entryAt(sm, ByteString.copyFrom(baos.toByteArray()), 5L));

    assertThat(future.isCompletedExceptionally()).isTrue();
    assertThat(sm.isHaltedAfterCriticalError())
        .as("one unreadable entry must not stop every co-located database (issue #7138)")
        .isFalse();
    assertThat(sm.isDatabaseDiverged("db-A")).isTrue();
    assertThat(sm.isDatabaseDiverged("db-B")).isFalse();
  }

  /**
   * Control for the arm above: an unknown TYPE is a different failure and still halts. #4798 is right that
   * skipping a committed mutation nobody can read is a silent divergence, and nothing here loosens that.
   */
  @Test
  void anUnknownEntryTypeStillHaltsTheNode() {
    final ArcadeStateMachine sm = new ArcadeStateMachine();

    final CompletableFuture<Message> future = sm.applyTransaction(
        entryAt(sm, ByteString.copyFrom(new byte[] { (byte) 99, 0, 0 }), 7L));

    assertThat(future.isCompletedExceptionally()).isTrue();
    assertThat(sm.isHaltedAfterCriticalError()).isTrue();
  }

  private static TransactionContext entryAt(final ArcadeStateMachine sm, final ByteString payload, final long index) {
    final LogEntryProto logEntry = LogEntryProto.newBuilder()
        .setTerm(1L)
        .setIndex(index)
        .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(payload).build())
        .build();
    return TransactionContext.newBuilder().setStateMachine(sm).setLogEntry(logEntry).build();
  }
}
