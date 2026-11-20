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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexCompacted;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12348: a replica whose unique index could not attach its compacted sub-index ("Invalid sub-index ... File with id N was
 * not found") logged a warning on every restart and went on serving, and leading with, an index that held the keys of its mutable
 * pages only - a UNIQUE constraint enforced on part of the keys. Such a copy is now local-copy damage like a unique index with no
 * header page (#11258): the node quarantines the database and re-installs it from the cluster.
 * <p>
 * The replica's state is built the way the reporter's node held it: the compacted file of its index is gone from its disk when it
 * starts.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class PartialUniqueIndexQuarantineIT extends BaseRaftHATest {
  private static final String TYPE_NAME = "Event";
  private static final int    KEYS      = 3_200;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected boolean persistentRaftStorage() {
    return true;
  }

  // Regression for #12348: a replica starting without its compacted sub index is reinstalled from the cluster
  @Test
  void aReplicaStartingWithoutItsCompactedSubIndexIsReinstalledFromTheCluster() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).isGreaterThanOrEqualTo(0);
    final DatabaseInternal db = wrapped(leader);

    final DocumentType type = db.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
    type.createProperty("eid", Type.INTEGER);
    db.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "eid" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true)
        .withPageSize(4_096).create();

    insert(db, 0, 3_000);
    final LSMTreeIndex leaderIndex = bucketIndex(db);
    assertThat(leaderIndex.scheduleCompaction()).isTrue();
    assertThat(leaderIndex.compact()).isTrue();
    insert(db, 3_000, KEYS - 3_000);
    waitForAllServers();

    final int replica = firstFollowerOf(leader);
    final DatabaseInternal replicaDb = (DatabaseInternal) getServerDatabase(replica, getDatabaseName());
    final LSMTreeIndexCompacted compacted = bucketIndex(replicaDb).getMutableIndex().getSubIndex();
    assertThat(compacted).as("the compaction must have reached the replica").isNotNull();
    final String compactedName = new File(replicaDb.getFileManager().getFile(compacted.getFileId()).getFilePath()).getName();
    final String compactedBase = compactedName.substring(0, compactedName.lastIndexOf('.'));
    final String databasePath = replicaDb.getDatabasePath();

    getServer(replica).stop();
    final File[] files = new File(databasePath).listFiles((dir, name) -> name.startsWith(compactedBase + "."));
    assertThat(files).isNotEmpty();
    for (final File f : files)
      assertThat(f.delete()).isTrue();

    startServer(replica);

    // the replica must not keep the partial index: re-installed from the cluster, it finds every key again
    assertThat(awaitValue(KEYS, () -> withResyncRetry(replica, d -> keysFoundByIndex(d))))
        .as("the replica must be re-installed with the whole index").isEqualTo(KEYS);
    final String detached = withResyncRetry(replica, d -> bucketIndex((DatabaseInternal) d).getMutableIndex().getDetachedSubIndex());
    assertThat(detached).isNull();
    assertThat(awaitValue(0, () -> isDatabaseQuarantined(replica, getDatabaseName()) ? 1 : 0)).isZero();
  }

  private DatabaseInternal wrapped(final int serverIndex) {
    return ((DatabaseInternal) getServerDatabase(serverIndex, getDatabaseName())).getWrappedDatabaseInstance();
  }

  private static void insert(final DatabaseInternal db, final int from, final int count) {
    db.transaction(() -> {
      for (int i = from; i < from + count; i++)
        db.newDocument(TYPE_NAME).set("eid", i).save();
    });
  }

  private static LSMTreeIndex bucketIndex(final DatabaseInternal db) {
    final TypeIndex typeIndex = db.getSchema().getType(TYPE_NAME).getIndexesByProperties("eid").getFirst();
    return (LSMTreeIndex) typeIndex.getIndexesOnBuckets()[0];
  }

  private static long keysFoundByIndex(final Database db) {
    final TypeIndex typeIndex = db.getSchema().getType(TYPE_NAME).getIndexesByProperties("eid").getFirst();
    long found = 0;
    for (int i = 0; i < KEYS; i++)
      if (typeIndex.get(new Object[] { i }).hasNext())
        ++found;
    return found;
  }
}
