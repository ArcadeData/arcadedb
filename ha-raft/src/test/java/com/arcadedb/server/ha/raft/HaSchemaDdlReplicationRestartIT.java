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

import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9762: under Raft HA, {@code ALTER TYPE ... NAME} renamed the type and its bucket and index files on the leader
 * only. The rename ended in a bare schema save, so no {@code SCHEMA_ENTRY} was proposed, and a schema entry could not
 * have carried it anyway: it lists files to create and to remove, not renames. Here a follower is restarted after the
 * rename, and every node is checked three ways before and after: its in-memory schema, its own {@code schema.json}, and
 * the files on its disk - the new names present, the old ones gone.
 * <p>
 * Sibling of {@link HaSchemaDdlReplicationIT}, merged from {@code Issue9762TypeRenameReplicationIT} (its restart test;
 * the other five tests of that class are in the shared sibling). It keeps a fresh cluster per test
 * ({@link BaseRaftHATest}) with the source's fixture unchanged (two servers, in-memory Raft storage, no configuration
 * override): the test restarts a follower, which comes back with fresh in-memory state (Raft storage, capability
 * registry) and has to catch up again, so the cluster it leaves is not the one it found.
 */
class HaSchemaDdlReplicationRestartIT extends BaseRaftHATest {

  private static final int RECORDS = 50;

  /**
   * Issue #9762, from {@code Issue9762TypeRenameReplicationIT}.
   * <p>
   * A follower survives a restart after the rename: it reopens with the renamed files and a schema naming them, rather
   * than dropping the type's buckets because the schema and the disk disagree.
   */
  @Test
  void aFollowerReopensTheRenamedTypeAfterARestart() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int followerIndex = (leaderIndex + 1) % getServerCount();
    final Database leader = leader();
    leader.command("sql", "CREATE DOCUMENT TYPE S");
    leader.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        leader.newDocument("S").set("id", i).save();
    });
    assertClusterConsistency();

    leader.command("sql", "ALTER TYPE S NAME S2");
    assertRenamedOnEveryNode("S", "S2", RECORDS);

    restartServer(followerIndex);

    assertRenamedOnEveryNode("S", "S2", RECORDS);
  }

  private void assertRenamedOnEveryNode(final String oldName, final String newName, final long records)
      throws IOException {
    assertClusterConsistency();
    for (int i = 0; i < getServerCount(); i++) {
      final Database database = getServerDatabase(i, getDatabaseName());
      final Schema schema = database.getSchema();
      assertThat(schema.existsType(newName)).as("server %d knows type %s", i, newName).isTrue();
      assertThat(schema.existsType(oldName)).as("server %d still knows type %s", i, oldName).isFalse();
      assertThat(database.countType(newName, false)).as("server %d records", i).isEqualTo(records);
      for (final var bucket : schema.getType(newName).getBuckets(false))
        assertThat(bucket.getName()).as("server %d bucket", i).startsWith(newName + "_");

      final JSONObject file = new JSONObject(
          Files.readString(new File(database.getDatabasePath(), "schema.json").toPath(), StandardCharsets.UTF_8));
      assertThat(file.getJSONObject("types").has(newName)).as("server %d schema.json", i).isTrue();
      assertThat(file.getJSONObject("types").has(oldName)).as("server %d schema.json", i).isFalse();

      assertThat(filesNamedAfter(database, newName)).as("server %d files of %s", i, newName).isNotEmpty();
      assertThat(filesNamedAfter(database, oldName)).as("server %d files of %s", i, oldName).isEmpty();

      if (!schema.getType(newName).getAllIndexes(false).isEmpty())
        try (final ResultSet rs = database.query("sql", "SELECT FROM " + newName + " WHERE id = 7")) {
          assertThat(rs.hasNext()).as("server %d index lookup", i).isTrue();
        }
    }
  }

  /** The component files on disk whose name is the type's name followed by its separator. */
  private static List<String> filesNamedAfter(final Database database, final String typeName) {
    final List<String> names = new ArrayList<>();
    final File[] files = new File(database.getDatabasePath()).listFiles();
    if (files != null)
      for (final File f : files)
        if (f.getName().startsWith(typeName + "_"))
          names.add(f.getName());
    return names;
  }

  private Database leader() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    return getServerDatabase(leaderIndex, getDatabaseName());
  }
}
