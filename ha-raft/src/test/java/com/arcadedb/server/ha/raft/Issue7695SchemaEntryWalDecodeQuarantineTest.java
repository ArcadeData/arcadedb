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

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.schema.Type;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7695: {@code ArcadeStateMachine.applySchemaEntry} decoded the WAL entries a {@code SCHEMA_ENTRY} carries
 * with a bare {@code deserializeWalTransaction} call, which is the same defect class PR #7687 closed for
 * {@code TX_ENTRY} on the call site its sweep missed.
 * <p>
 * {@code deserializeWalTransaction} rejects a misaligned page count or delta range with a bare
 * {@code ReplicationException} (issue #4420) and raises a bare {@code BufferUnderflowException} for a payload
 * shorter than the 24-byte header it reads. {@code applyWithRetry} rethrows a {@code ReplicationException}
 * UNCHANGED - it is the resync signal {@code applyTxEntry} raises on a WAL gap - so
 * {@code handleUnexpectedApplyError} never ran: the database was not quarantined, no targeted snapshot resync was
 * triggered, and the failed future is one Ratis swallows while advancing its own applied index. <b>The corrupt
 * schema entry was skipped on this node and nothing said so.</b>
 * <p>
 * Narrower than the {@code TX_ENTRY} path - it only fires for DDL that carries buffered WAL, the
 * {@code recordFileChanges()} path through {@code RaftReplicatedDatabase}'s schema WAL buffer - but live code.
 * <p>
 * The assertion is the exception TYPE and what it carries, because that is what routes the failure: a
 * {@link RaftLogEntryDecodeException} naming the database reaches the per-database quarantine of issue #7138,
 * and anything else does not.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7695SchemaEntryWalDecodeQuarantineTest {

  private static final String DB = "issue7695";

  @TempDir
  private Path         serverDir;
  private LocalDatabase database;

  @BeforeEach
  void setUp() {
    database = (LocalDatabase) new DatabaseFactory(serverDir.resolve(DB).toString()).create();
    database.transaction(() -> database.getSchema().createVertexType("Seed").createProperty("id", Type.STRING));
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  /**
   * A payload of eight bytes: long enough to hold the transaction id the decoder reads first, shorter than the
   * 24-byte header it goes on to read, so the decode runs out of buffer and raises a
   * {@code BufferUnderflowException} - the shape that used to reach {@code applyTransaction}'s fatal
   * {@code catch (Throwable)} rather than the quarantine.
   */
  @Test
  void aTruncatedBufferedWalEntryIsADecodeFailureAgainstItsOwnDatabase() {
    assertThatThrownBy(() -> applySchemaEntryWith(List.of(new byte[8])))
        .isInstanceOf(RaftLogEntryDecodeException.class)
        .satisfies(thrown -> {
          final RaftLogEntryDecodeException decode = (RaftLogEntryDecodeException) thrown;
          assertThat(decode.getType())
              .as("the entry that could not be read is a schema entry, not a transaction entry")
              .isEqualTo(RaftLogEntryType.SCHEMA_ENTRY);
          assertThat(decode.getDatabaseName())
              .as("without the database name there is nothing to quarantine and the node halts instead")
              .isEqualTo(DB);
        })
        .hasMessageContaining("schema entry")
        .hasMessageContaining(DB);
  }

  /**
   * The other shape, and the one the bare {@code catch (IOException)} in {@code applySchemaEntry} let through
   * untouched: a payload whose header parses but whose page count is impossible, which
   * {@code deserializeWalTransaction} refuses with a {@code ReplicationException} (issue #4420). That type is
   * the resync signal, so {@code applyWithRetry} rethrew it unchanged and the quarantine was bypassed - the node
   * skipped the entry silently.
   */
  @Test
  void aMisalignedBufferedWalEntryIsADecodeFailureRatherThanABareResyncSignal() {
    assertThatThrownBy(() -> applySchemaEntryWith(List.of(walHeaderWithPageCount(Integer.MAX_VALUE))))
        .isInstanceOf(RaftLogEntryDecodeException.class)
        .hasMessageContaining(DB);
  }

  /**
   * Which buffered entry of the batch failed has to be in the message: a schema entry carries a whole sequence of
   * them, and "the entry could not be read" leaves an operator no way to tell one corrupt WAL record from an
   * entry that is corrupt throughout.
   */
  @Test
  void theFailingBufferedEntryIsNamedWithinItsBatch() {
    assertThatThrownBy(() -> applySchemaEntryWith(List.of(walHeaderWithPageCount(0), new byte[8])))
        .isInstanceOf(RaftLogEntryDecodeException.class)
        .hasMessageContaining("buffered WAL entry 2 of 2");
  }

  /**
   * The counter-case: a well-formed buffered WAL entry still applies. Without it the three tests above would pass
   * against an {@code applySchemaEntry} that refused every entry it was handed.
   */
  @Test
  void aWellFormedBufferedWalEntryStillApplies() {
    applySchemaEntryWith(List.of(walHeaderWithPageCount(0)));

    assertThat(database.getSchema().existsType("Seed"))
        .as("the apply completed rather than throwing").isTrue();
  }

  /**
   * Drives the production applier over a real encode/decode round trip, against the real database opened above -
   * which is what this defect needs and what the null-server harness covering the {@code TX_ENTRY} cases cannot
   * reach: {@code applySchemaEntry} resolves its database through {@code databaseFor}, so a state machine with
   * no server attached can answer it here.
   */
  private void applySchemaEntryWith(final List<byte[]> walEntries) {
    final ByteString entry = RaftLogEntryCodec.encodeSchemaEntry(DB, "", Collections.emptyMap(),
        Collections.emptyMap(), walEntries, Collections.emptyList());

    new DatabaseBoundStateMachine(database).applySchemaEntry(RaftLogEntryCodec.decode(entry), 42L, false);
  }

  /**
   * A WAL payload whose 24-byte header parses: transaction id, timestamp, then the page count. A count of zero
   * is a well-formed empty transaction; anything the remaining bytes cannot account for is the misalignment
   * {@code deserializeWalTransaction} refuses.
   */
  private static byte[] walHeaderWithPageCount(final int pages) {
    final ByteBuffer buffer = ByteBuffer.allocate(24);
    buffer.putLong(1L);   // txId
    buffer.putLong(System.currentTimeMillis());
    buffer.putInt(pages);
    buffer.putInt(24);    // segment size
    return buffer.array();
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
