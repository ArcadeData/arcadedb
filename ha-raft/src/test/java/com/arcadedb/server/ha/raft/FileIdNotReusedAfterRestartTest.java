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
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.utility.FileUtils;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11240: {@code FileManager.newFileId()} answers the next free slot, and the slots are rebuilt at open from the files on
 * disk, so the id of the highest file dropped before a restart used to be handed out again after it. A node still holding a
 * file under that id - one that diverged, or that has not applied the drop yet - then refused the new file under it and was
 * quarantined. The schema now carries the first id never handed out whenever the highest ids were dropped.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class FileIdNotReusedAfterRestartTest {
  private static final String DB = "fileids";

  @TempDir
  Path tempDir;

  private LocalDatabase db;
  private LocalDatabase follower;

  @AfterEach
  void tearDown() {
    if (db != null && db.isOpen())
      db.drop();
    if (follower != null && follower.isOpen())
      follower.drop();
  }

  @Test
  void theIdOfTheDroppedHighestFileIsNotReusedInTheSameSession() {
    db = (LocalDatabase) new DatabaseFactory(tempDir.resolve(DB).toString()).create();
    final int droppedId = createTypeWithOneBucket("Last");
    db.getSchema().dropType("Last");
    assertThat(db.getFileManager().existsFile(droppedId)).isFalse();

    assertThat(createTypeWithOneBucket("Again")).as("id allocated after the drop, same session").isEqualTo(droppedId + 1);
  }

  @Test
  void theIdOfTheDroppedHighestFileIsNotReusedAfterARestart() {
    db = (LocalDatabase) new DatabaseFactory(tempDir.resolve(DB).toString()).create();
    final int droppedId = createTypeWithOneBucket("Last");
    db.getSchema().dropType("Last");
    db.close();

    db = (LocalDatabase) new DatabaseFactory(tempDir.resolve(DB).toString()).open();
    assertThat(db.getFileManager().getFiles().size()).as("file slots after the reopen").isEqualTo(droppedId + 1);
    assertThat(createTypeWithOneBucket("Again")).as("id allocated after the drop and a restart").isEqualTo(droppedId + 1);

    // Once a new file holds the highest id again, the schema stops carrying the mark: the files say it all
    db.close();
    db = (LocalDatabase) new DatabaseFactory(tempDir.resolve(DB).toString()).open();
    assertThat(db.getSchema().getEmbedded().toJSON().has("nextFileId")).isFalse();
    assertThat(createTypeWithOneBucket("Third")).isEqualTo(droppedId + 2);
  }

  /**
   * The collision half: a node still holding id X (the file the leader dropped on its own) is handed the next file the
   * restarted leader allocates. That file no longer reuses X, so the node creates it and is not quarantined.
   */
  @Test
  void aFollowerStillHoldingTheDroppedIdAcceptsTheLeadersNextFile() throws IOException {
    final Path leaderPath = tempDir.resolve("leader").resolve(DB);
    final Path followerPath = tempDir.resolve("follower").resolve(DB);
    db = (LocalDatabase) new DatabaseFactory(leaderPath.toString()).create();
    final int droppedId = createTypeWithOneBucket("Last");
    db.close();
    FileUtils.copyDirectory(leaderPath.toFile(), followerPath.toFile());

    // A drop on the leader alone, then the leader restarts and allocates a new file
    db = (LocalDatabase) new DatabaseFactory(leaderPath.toString()).open();
    db.getSchema().dropType("Last");
    db.close();
    db = (LocalDatabase) new DatabaseFactory(leaderPath.toString()).open();
    final int newId = createTypeWithOneBucket("Again");
    assertThat(newId).isEqualTo(droppedId + 1);
    final String leaderFileName = db.getFileManager().getFile(newId).getFileName();

    follower = (LocalDatabase) new DatabaseFactory(followerPath.toString()).open();
    assertThat(follower.getFileManager().existsFile(droppedId)).as("the follower still holds id %d", droppedId).isTrue();
    assertThat(follower.getFileManager().existsFile(newId)).isFalse();

    final ArcadeStateMachine followerStateMachine = new ArcadeStateMachine() {
      @Override
      DatabaseInternal databaseFor(final String databaseName) {
        return follower;
      }

      @Override
      ClientId localClientId() {
        return null;
      }

      @Override
      long currentRaftTerm() {
        return 7L;
      }
    };
    final ByteString payload = RaftLogEntryCodec.encodeSchemaEntry(DB, db.getSchema().getEmbedded().toJSON().toString(),
        Map.of(newId, leaderFileName), Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
        Collections.emptyList());
    final LogEntryProto logEntry = LogEntryProto.newBuilder().setTerm(1L).setIndex(100L)
        .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(payload).build()).build();
    final CompletableFuture<Message> applied = followerStateMachine.applyTransaction(
        TransactionContext.newBuilder().setStateMachine(followerStateMachine).setLogEntry(logEntry).build());

    assertThat(applied.isCompletedExceptionally()).as("the follower takes the leader's new file").isFalse();
    assertThat(followerStateMachine.isDatabaseDiverged(DB)).as("the follower is not quarantined").isFalse();
    assertThat(follower.getFileManager().getFile(newId).getFileName()).isEqualTo(leaderFileName);
  }

  private int createTypeWithOneBucket(final String name) {
    final DocumentType type = db.getSchema().buildDocumentType().withName(name).withTotalBuckets(1).create();
    final int fileId = type.getBuckets(false).getFirst().getFileId();
    int highest = -1;
    for (final var file : db.getFileManager().getFiles())
      if (file != null)
        highest = Math.max(highest, file.getFileId());
    assertThat(fileId).as("the bucket of '%s' is the highest file id", name).isEqualTo(highest);
    return fileId;
  }
}
