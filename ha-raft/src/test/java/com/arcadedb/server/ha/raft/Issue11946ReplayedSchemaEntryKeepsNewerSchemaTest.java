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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.ImmutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestServerHelper;
import org.apache.ratis.protocol.ClientId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11946: a node that restarts replays the Raft log from its snapshot marker, and the schema entries in that stretch carry
 * the schema documents the leader published THEN. The copy on disk already holds a newer one. Applying an older document over it
 * took the indexes created since out of the schema until a later entry put them back, and while it lasted the node served
 * clients: a transaction committed through it carried no index changes, the leader accepted it (its page versions were right),
 * and every node kept the index entries of a deleted record. In the run that found it the stretch replayed the entries of the
 * database's previous incarnation (dropped and created again since), whose index entry then failed on a file id and left the
 * regressed schema in place for a whole quarantine.
 */
class Issue11946ReplayedSchemaEntryKeepsNewerSchemaTest {
  private static final String DB    = "replayed-schema";
  private static final String INDEX = "T[k]";

  @TempDir
  Path serverDir;

  private ArcadeDBServer server;
  private LocalDatabase  db;
  private String         documentBeforeTheIndex;

  @BeforeEach
  void setUp() {
    server = TestServerHelper.unstartedServer(serverDir, new ContextConfiguration());
    final Path databases = Path.of(server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY));
    db = (LocalDatabase) new DatabaseFactory(databases.resolve(DB).toString()).create();

    db.getSchema().buildDocumentType().withName("T").withTotalBuckets(1).create().createProperty("k", Type.STRING);
    // The document the CREATE PROPERTY entry carried: the type, and no index yet
    documentBeforeTheIndex = db.getSchema().getEmbedded().toJSON().toString();

    db.getSchema().buildTypeIndex("T", new String[] { "k" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).create();
    db.transaction(() -> {
      for (int i = 0; i < 10; i++)
        db.newDocument("T").set("k", "k" + i).save();
    });

    // The previous session of this node applied every entry of the database up to index 2000, the index entry included
    stateMachine().writePersistedAppliedIndex(2_000L, DB);
  }

  @AfterEach
  void tearDown() {
    if (db != null && db.isOpen())
      db.drop();
  }

  @Test
  void aReplayedOlderDocumentLeavesTheIndexInTheSchemaAndADeleteStillRemovesItsEntry() {
    // This session: a new state machine, which replays the stretch after its snapshot marker
    stateMachine().applySchemaEntry(schemaEntry(documentBeforeTheIndex, Collections.emptyMap()), 1_000L, false);

    assertThat(db.getSchema().existsIndex(INDEX)).as("the index the copy holds, after the replay of an older document").isTrue();
    assertThat(db.getSchema().getType("T").getIndexesByProperties("k")).as("the indexes of the type on k").isNotEmpty();

    // What a client of the restarting node commits meanwhile
    db.transaction(() -> db.command("sql", "DELETE FROM T WHERE k = 'k3'"));

    final Index index = db.getSchema().getIndexByName(INDEX);
    assertThat(index.get(new Object[] { "k3" }).hasNext()).as("the entry of the deleted record").isFalse();
    assertThat(index.countEntries()).as("entries of the index").isEqualTo(9);
  }

  /**
   * The entry of a previous incarnation of the database (dropped and created again since) names one of its own file ids, which
   * the current copy uses for another file, and carries a page of it whose version lines up with the current file's. Taken for an
   * entry the previous session applied to this copy, its pages would be written into that other file. It is refused as a
   * divergence instead, before any page or document is applied, which quarantines the database and resyncs it.
   */
  @Test
  void aReplayedEntryOfAPreviousIncarnationWritesNoPageIntoTheFileThatReusesItsId() throws Exception {
    final int indexFileId = ((TypeIndex) db.getSchema().getIndexByName(INDEX)).getIndexesOnBuckets()[0].getFileId();
    final PaginatedComponentFile file = (PaginatedComponentFile) db.getFileManager().getFile(indexFileId);
    final PageId pageId = new PageId(db, indexFileId, 0);
    final ImmutablePage before = db.getPageManager().getImmutablePage(pageId, file.getPageSize(), false, false);
    final long version = before.getVersion();

    final byte[] page = walPage(indexFileId, (int) version + 1, before.getContentSize());
    final RaftLogEntryCodec.DecodedEntry entry = RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeSchemaEntry(DB,
        documentBeforeTheIndex, Map.of(indexFileId, "T_0_1.2.262144.v1.umtidx"), Collections.emptyMap(), List.of(page),
        List.of(Collections.emptyMap())));

    assertThatThrownBy(() -> stateMachine().applySchemaEntry(entry, 1_000L, false))
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("diverged");

    assertThat(db.getPageManager().getImmutablePage(pageId, file.getPageSize(), false, false).getVersion())
        .as("version of the page of the file that reuses the id").isEqualTo(version);
    assertThat(db.getSchema().existsIndex(INDEX)).as("the index the copy holds").isTrue();
  }

  /** An entry the previous session did not reach is applied as before. */
  @Test
  void anEntryAfterThePositionThePreviousSessionReachedIsApplied() {
    final JSONObject newer = db.getSchema().getEmbedded().toJSON();
    newer.getJSONObject("settings").put("dateFormat", "yyyy/MM/dd");

    stateMachine().applySchemaEntry(schemaEntry(newer.toString(), Collections.emptyMap()), 2_001L, false);

    assertThat(db.getSchema().getDateFormat()).isEqualTo("yyyy/MM/dd");
  }

  /** A database dropped and created again in this session is a new copy: the position the previous session reached is not its. */
  @Test
  void aCopyCreatedInThisSessionTakesEveryEntry() {
    final ArcadeStateMachine stateMachine = stateMachine();
    stateMachine.appliedIndexes.forgetPositionOfPreviousSession(DB);

    final JSONObject newer = db.getSchema().getEmbedded().toJSON();
    newer.getJSONObject("settings").put("dateFormat", "yyyy/MM/dd");
    stateMachine.applySchemaEntry(schemaEntry(newer.toString(), Collections.emptyMap()), 1_500L, false);

    assertThat(db.getSchema().getDateFormat()).isEqualTo("yyyy/MM/dd");
  }

  private RaftLogEntryCodec.DecodedEntry schemaEntry(final String document, final Map<Integer, String> filesToAdd) {
    return RaftLogEntryCodec.decode(RaftLogEntryCodec.encodeSchemaEntry(DB, document, filesToAdd, Collections.emptyMap(),
        Collections.emptyList(), Collections.emptyList()));
  }

  /** The wire layout {@code deserializeWalTransaction} reads: one 4-byte segment of page 0 of {@code fileId}. */
  private static byte[] walPage(final int fileId, final int targetVersion, final int contentSize) {
    final int deltaSize = 4;
    final ByteBuffer buf = ByteBuffer.allocate(2 * Long.BYTES + 2 * Integer.BYTES + 6 * Integer.BYTES + deltaSize);
    buf.putLong(1L);
    buf.putLong(System.currentTimeMillis());
    buf.putInt(1);
    buf.putInt(6 * Integer.BYTES + deltaSize);
    buf.putInt(fileId);
    buf.putInt(0);                                         // pageNumber
    buf.putInt(BasePage.PAGE_HEADER_SIZE);                 // changesFrom
    buf.putInt(BasePage.PAGE_HEADER_SIZE + deltaSize - 1); // changesTo
    buf.putInt(targetVersion);
    buf.putInt(contentSize);
    buf.put(new byte[] { 0x7F, 0x7F, 0x7F, 0x7F });
    return buf.array();
  }

  private ArcadeStateMachine stateMachine() {
    final ArcadeStateMachine stateMachine = new ArcadeStateMachine() {
      @Override
      DatabaseInternal databaseFor(final String databaseName) {
        return db;
      }

      @Override
      ClientId localClientId() {
        return null;
      }
    };
    stateMachine.setServer(server);
    return stateMachine;
  }
}
