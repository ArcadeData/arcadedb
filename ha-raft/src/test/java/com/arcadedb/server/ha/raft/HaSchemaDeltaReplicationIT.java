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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.Component;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.function.ToLongFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * How a schema change reaches the followers, end to end on ONE two-node cluster shared by every test
 * ({@link SharedClusterRaftHATest}). Merged from {@code Issue6988IncrementalSchemaApplyIT},
 * {@code Issue6989SchemaDeltaReplicationIT} (its drop test) and {@code Issue6990BulkSchemaScriptIT}, which ran on the
 * same fixture: two servers, {@code majority} quorum and {@code arcadedb.ha.schemaDelta} on (#6988 set the quorum
 * explicitly, #6989 both, #6990 had both as the defaults), no other configuration override.
 * <ul>
 * <li><b>#6988</b>: {@code ArcadeStateMachine.applySchemaEntry} used to end every committed DDL entry with a full
 * {@code LocalSchema.load()}, which drops every {@code Component} in the database and re-instantiates one per file,
 * reading page 0 of each. Building a schema was therefore O(entries x total files), quadratic in the number of types and
 * serialized on the single Ratis apply thread (#6982: 1209 types took about 2h53m to reach the followers). The
 * regression is pinned by IDENTITY, not by elapsed time: a component the entry did not touch must survive the apply as
 * the very same instance. An elapsed-time budget could express the same claim only at a type count that would take hours
 * to build. A DROP retires files, which the incremental path refuses, so it still takes the full rebuild.</li>
 * <li><b>#6989</b>: with {@code arcadedb.ha.schemaDelta} on, a DDL against a large schema must reach the follower
 * through a Raft entry proportional to the change rather than to the schema. The tests check that the leader actually
 * took the delta path (it falls back to the document by itself whenever the cached base is not what the followers hold,
 * so a green functional assertion alone would pass even if no delta was ever shipped) and that the follower ends up with
 * exactly the schema the leader has.</li>
 * <li><b>#6990</b>: a DDL script has to reach the followers as ONE Raft entry, not one per statement. The Raft entry
 * boundary is the outermost {@code recordFileChanges} session on the leader's calling thread, and every DDL entry point
 * opens one of its own; a script is neither a session nor a transaction, so twelve statements were twelve entries,
 * twelve serialized copies of the full schema and twelve quorum round trips, each with the database write lock held.
 * {@link ArcadeStateMachine#TEST_SCHEMA_ENTRY_COUNTER} counts the PUBLISHING schema entries a node applies; the leader
 * returns from {@code applySchemaEntry} before the counter on the {@code originatedLocally} path, so with two servers the
 * count is the follower's. The control run, with the batching turned off, is the same script measured on the code path
 * the change replaced.</li>
 * </ul>
 * Why these share a cluster: every type a test creates is named through {@link #uniqueName(String)}, and every
 * assertion is relative to a baseline the test captures itself: the instance of a component it registered, a counter
 * read before its own DDL ({@code schemaDeltasShipped()}), or a fresh {@code SchemaEntryRecorder} it installs (and
 * removes in {@link #cleanupHooks()}). The one setting a test flips, {@code arcadedb.schemaBulkDDLScript} on the leader's
 * database configuration (#6990's control), is restored to its default in a {@code finally}. No test stops a node, moves
 * leadership or counts the types of the shared database.
 * <p>
 * Siblings, each with the fixture its tests need: {@link HaSchemaDeltaReplicationFreshClusterIT} (#6989's
 * single-property test, which needs a cluster that has never shipped a schema so its seed primes the delta base),
 * {@link HaSchemaDeltaReplicationFullRebuildIT} (#6988's companion, {@code arcadedb.ha.schemaIncrementalApply=false})
 * and {@link HaSchemaDeltaReplicationRestartIT} (#8728, three servers, persistent Raft storage, restarts a follower).
 * <p>
 * #6989's and #6990's source classes were tagged {@code slow}, so their tests are here.
 */
class HaSchemaDeltaReplicationIT extends SharedClusterRaftHATest {

  /** #6988. */
  private static final int TYPE_COUNT = 40;

  /** #6990: four statements per type, so the count separates "one per statement" from "one per script" by a wide margin. */
  private static final int TYPES               = 3;
  private static final int STATEMENTS_PER_TYPE = 4;

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.HA_QUORUM, "majority");
    config.setValue(GlobalConfiguration.HA_SCHEMA_DELTA, true);
  }

  @Override
  protected int getServerCount() {
    return 2;
  }

  @AfterEach
  void cleanupHooks() {
    ArcadeStateMachine.TEST_SCHEMA_ENTRY_COUNTER = null;
  }

  // ------------------------------------------------------------------ incremental schema apply (#6988)

  /** Issue #6988, from {@code Issue6988IncrementalSchemaApplyIT}. */
  @Test
  void followerKeepsUntouchedComponentsAcrossManySchemaEntries() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int replicaIndex = leaderIndex == 0 ? 1 : 0;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    final String anchor = uniqueName("Issue6988Anchor");
    final String typePrefix = uniqueName("Issue6988Type") + "_";

    // Every DDL lambda below is idempotent on purpose. A committed Raft entry whose acknowledgement is lost - a
    // leadership transfer mid-commit is enough, and cluster formation is exactly when that happens - makes
    // LocalDatabase.transaction() re-run the lambda, and schema changes are not rolled back with the records.
    leaderDb.transaction(() -> leaderDb.getSchema().getOrCreateDocumentType(anchor));
    assertClusterConsistency();

    final Database replicaDb = getServerDatabase(replicaIndex, getDatabaseName());
    final LocalSchema replicaSchema = replicaDb.getSchema().getEmbedded();

    final int anchorFileId = replicaSchema.getType(anchor).getBuckets(false).getFirst().getFileId();
    final Component anchorBefore = replicaSchema.getFileByIdIfExists(anchorFileId);
    assertThat(anchorBefore).as("the follower must have registered the anchor type's bucket").isNotNull();

    // One transaction per type, so each one becomes its own committed SCHEMA_ENTRY on the follower.
    for (int i = 0; i < TYPE_COUNT; i++) {
      final int index = i;
      leaderDb.transaction(() -> {
        final DocumentType type = leaderDb.getSchema().getOrCreateDocumentType(typePrefix + index);
        type.getOrCreateProperty("id", Type.STRING);
        type.getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
      });
    }

    assertClusterConsistency();

    for (int i = 0; i < TYPE_COUNT; i++) {
      final String typeName = typePrefix + i;
      assertThat(replicaDb.getSchema().existsType(typeName)).as("replica should have type %s", typeName).isTrue();
      assertThat(replicaDb.getSchema().getType(typeName).existsProperty("id")).isTrue();
      assertThat(replicaDb.getSchema().getType(typeName).getAllIndexes(false))
          .as("replica should have the index of type %s registered", typeName)
          .isNotEmpty();
    }

    assertThat(replicaSchema.getFileByIdIfExists(anchorFileId))
        .as("applying %d schema entries must not re-instantiate a component none of them touched", TYPE_COUNT)
        .isSameAs(anchorBefore);
  }

  /** Issue #6988, from {@code Issue6988IncrementalSchemaApplyIT}. */
  @Test
  void replicatedIndexesStillAnswerLookupsAfterAnIncrementalApply() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int replicaIndex = leaderIndex == 0 ? 1 : 0;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    final String indexed = uniqueName("Issue6988Indexed");
    final String fillerPrefix = uniqueName("Issue6988Filler") + "_";

    // See the note in followerKeepsUntouchedComponentsAcrossManySchemaEntries: DDL must be replay-safe here.
    leaderDb.transaction(() -> {
      final DocumentType type = leaderDb.getSchema().getOrCreateDocumentType(indexed);
      type.getOrCreateProperty("code", Type.STRING);
      type.getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "code");
    });

    // A few more entries after the index creation, so the index registration has to survive later incremental applies.
    for (int i = 0; i < 5; i++) {
      final int index = i;
      leaderDb.transaction(() -> leaderDb.getSchema().getOrCreateDocumentType(fillerPrefix + index));
    }

    leaderDb.transaction(() -> {
      for (int i = 0; i < 50; i++)
        leaderDb.newDocument(indexed).set("code", "code-" + i).save();
    });

    assertClusterConsistency();

    final Database replicaDb = getServerDatabase(replicaIndex, getDatabaseName());
    assertThat(replicaDb.getSchema().getType(indexed).getAllIndexes(false)).isNotEmpty();

    try (final ResultSet result = replicaDb.query("sql", "SELECT FROM " + indexed + " WHERE code = ?", "code-42")) {
      assertThat(result.hasNext()).as("the replicated index must resolve a lookup on the follower").isTrue();
      assertThat(result.next().<String>getProperty("code")).isEqualTo("code-42");
    }
  }

  /** Issue #6988, from {@code Issue6988IncrementalSchemaApplyIT}. */
  @Test
  void droppingATypeStillFallsBackToTheFullRebuild() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("A Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int replicaIndex = leaderIndex == 0 ? 1 : 0;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    final String doomed = uniqueName("Issue6988Doomed");
    final String survivor = uniqueName("Issue6988Survivor");

    leaderDb.transaction(() -> {
      leaderDb.getSchema().getOrCreateDocumentType(doomed);
      leaderDb.getSchema().getOrCreateDocumentType(survivor);
    });
    assertClusterConsistency();

    // A DROP retires files, which the incremental path refuses: the entry must go through the full rebuild and the
    // follower must end up with the type gone and everything else intact.
    leaderDb.transaction(() -> {
      if (leaderDb.getSchema().existsType(doomed))
        leaderDb.getSchema().dropType(doomed);
    });
    assertClusterConsistency();

    final Database replicaDb = getServerDatabase(replicaIndex, getDatabaseName());
    assertThat(replicaDb.getSchema().existsType(doomed)).isFalse();
    assertThat(replicaDb.getSchema().existsType(survivor)).isTrue();
  }

  // ------------------------------------------------------------------ schema delta (#6989)

  /**
   * Issue #6989, from {@code Issue6989SchemaDeltaReplicationIT}.
   * <p>
   * Shares the cluster because its one counter assertion is a delta against a value it reads itself, after its own
   * seed: whether that seed shipped as a document (fresh cluster) or as a delta (a base an earlier test primed), the
   * drop that follows has a base to diff against.
   */
  @Test
  @Tag("slow")
  void droppingATypeReachesTheReplicaThroughADelta() {
    final int leaderIndex = findLeaderIndex();
    final int replicaIndex = leaderIndex == 0 ? 1 : 0;
    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    final String doomedPrefix = uniqueName("Doomed") + "_";
    requireAPristineDatabase(leaderDb, doomedPrefix + "0");

    leaderDb.transaction(() -> {
      final Schema schema = leaderDb.getSchema();
      for (int i = 0; i < 20; i++)
        schema.createVertexType(doomedPrefix + i).createProperty("id_" + i, Type.STRING);
    });

    final long deltasBefore = schemaDeltasShipped();

    leaderDb.transaction(() -> leaderDb.getSchema().dropType(doomedPrefix + "7"));

    assertThat(schemaDeltasShipped() - deltasBefore).as("the drop went out as a delta").isGreaterThan(0);

    assertClusterConsistency();

    final Schema replicaSchema = getServerDatabase(replicaIndex, getDatabaseName()).getSchema();
    assertThat(replicaSchema.existsType(doomedPrefix + "7")).as("the dropped type is gone on the replica").isFalse();
    assertThat(replicaSchema.existsType(doomedPrefix + "6")).isTrue();
    assertThat(replicaSchema.existsType(doomedPrefix + "8")).isTrue();
  }

  /**
   * #6989: the seed assumes it is building its types from nothing. {@code BaseGraphServerTest} points the servers at
   * {@code ./target/databasesN}, which the build's stale-database clean does NOT match (its fileset is
   * {@code target/databases}, without the index), so a run killed part-way leaves them behind and the seed fails with
   * a bare "Cannot create type ... because already exists" that names neither the cause nor the fix. Say both here.
   */
  private void requireAPristineDatabase(final Database leaderDb, final String firstSeededType) {
    assertThat(leaderDb.getSchema().existsType(firstSeededType))
        .as("this test seeds its own schema and needs a pristine database; remove the leftover "
            + "ha-raft/target/databases* directories an aborted run left behind")
        .isFalse();
  }

  /** #6989: the leader can move between tests, so ask every node rather than guessing which one shipped. */
  private long schemaDeltasShipped() {
    return sumOverNodes(RaftReplicatedDatabase::getSchemaDeltasShipped);
  }

  private long sumOverNodes(final ToLongFunction<RaftReplicatedDatabase> counter) {
    long total = 0;
    for (int i = 0; i < getServerCount(); i++) {
      final DatabaseInternal wrapped =
          ((DatabaseInternal) getServerDatabase(i, getDatabaseName())).getWrappedDatabaseInstance();
      if (wrapped instanceof final RaftReplicatedDatabase raft)
        total += counter.applyAsLong(raft);
    }
    return total;
  }

  // ------------------------------------------------------------------ bulk DDL script (#6990)

  private static String ddlScript(final String prefix) {
    final StringBuilder script = new StringBuilder();
    for (int i = 0; i < TYPES; i++)
      script.append("CREATE VERTEX TYPE ").append(prefix).append(i).append(";\n")
          .append("CREATE PROPERTY ").append(prefix).append(i).append(".name STRING;\n")
          .append("CREATE PROPERTY ").append(prefix).append(i).append(".age INTEGER;\n")
          .append("CREATE INDEX ON ").append(prefix).append(i).append(" (name) NOTUNIQUE;\n");
    return script.toString();
  }

  private void assertReplicated(final int serverIndex, final String prefix) {
    final Database db = getServerDatabase(serverIndex, getDatabaseName());
    for (int i = 0; i < TYPES; i++) {
      final String typeName = prefix + i;
      assertThat(db.getSchema().existsType(typeName)).as("type %s on server %d", typeName, serverIndex).isTrue();
      final DocumentType type = db.getSchema().getType(typeName);
      assertThat(type.getPropertyNames()).as("properties of %s on server %d", typeName, serverIndex)
          .contains("name", "age");
      assertThat(db.getSchema().existsIndex(typeName + "[name]"))
          .as("index of %s on server %d", typeName, serverIndex).isTrue();
    }
  }

  /** Issue #6990, from {@code Issue6990BulkSchemaScriptIT}. */
  @Test
  @Tag("slow")
  void aDdlScriptReachesTheFollowerAsOneSchemaEntry() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("leader elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = 1 - leaderIndex;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    final String unbatchedPrefix = uniqueName("Unbatched") + "_";
    final String batchedPrefix = uniqueName("Batched") + "_";

    // CONTROL: the pre-existing path, one session per statement. Measured first so the comparison is against this
    // cluster on this run and not against a remembered number.
    final boolean bulkDdlScriptBefore = leaderDb.getConfiguration().getValueAsBoolean(GlobalConfiguration.SCHEMA_BULK_DDL_SCRIPT);
    leaderDb.getConfiguration().setValue(GlobalConfiguration.SCHEMA_BULK_DDL_SCRIPT, false);
    final ArcadeStateMachine.SchemaEntryRecorder unbatched = new ArcadeStateMachine.SchemaEntryRecorder();
    ArcadeStateMachine.TEST_SCHEMA_ENTRY_COUNTER = unbatched;
    try {
      leaderDb.command("sqlscript", ddlScript(unbatchedPrefix));
      waitForAllServers();
    } finally {
      leaderDb.getConfiguration().setValue(GlobalConfiguration.SCHEMA_BULK_DDL_SCRIPT, bulkDdlScriptBefore);
    }

    assertReplicated(followerIndex, unbatchedPrefix);
    assertThat(unbatched.count())
        .as("without the bulk scope the follower applies at least one schema entry per statement")
        .isGreaterThanOrEqualTo(TYPES * STATEMENTS_PER_TYPE);

    // THE FIX: the same script, batched.
    final ArcadeStateMachine.SchemaEntryRecorder batched = new ArcadeStateMachine.SchemaEntryRecorder();
    ArcadeStateMachine.TEST_SCHEMA_ENTRY_COUNTER = batched;
    leaderDb.command("sqlscript", ddlScript(batchedPrefix));
    waitForAllServers();

    assertThat(batched.count())
        .as("a DDL-only script must reach the follower as ONE schema entry, whatever its statement count")
        .isEqualTo(1);

    // One entry is only worth anything if it carried the whole schema.
    assertReplicated(leaderIndex, batchedPrefix);
    assertReplicated(followerIndex, batchedPrefix);
  }

  /**
   * Issue #6990, from {@code Issue6990BulkSchemaScriptIT}.
   * <p>
   * The failure contract, which is the half that matters for correctness rather than for speed. Schema DDL has no
   * rollback, so a script that throws part way cannot be undone on the leader; what the scope guarantees is that the
   * follower ends up holding exactly what the leader holds. Aborting the session instead would publish nothing, and
   * the leader would keep the prefix while the follower had none of it - a divergence the per-statement path never
   * produced.
   */
  @Test
  @Tag("slow")
  void aScriptThatFailsHalfWayLeavesTheFollowerHoldingWhatTheLeaderHolds() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("leader elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = 1 - leaderIndex;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    final String partialA = uniqueName("PartialA");
    final String partialB = uniqueName("PartialB");
    final String noSuchType = uniqueName("NoSuchTypeAtAll");

    final ArcadeStateMachine.SchemaEntryRecorder failed = new ArcadeStateMachine.SchemaEntryRecorder();
    ArcadeStateMachine.TEST_SCHEMA_ENTRY_COUNTER = failed;

    assertThatThrownBy(() -> leaderDb.command("sqlscript", """
        CREATE VERTEX TYPE %1$s;
        CREATE PROPERTY %1$s.name STRING;
        CREATE PROPERTY %2$s.name STRING;
        CREATE VERTEX TYPE %3$s;
        """.formatted(partialA, noSuchType, partialB)))
        .as("the caller is still told which statement failed")
        .hasMessageContaining(noSuchType);

    waitForAllServers();

    assertThat(failed.count())
        .as("the prefix a failed script completed is published as ONE entry, not one per statement that succeeded")
        .isEqualTo(1);

    for (final int serverIndex : new int[] { leaderIndex, followerIndex }) {
      final Database db = getServerDatabase(serverIndex, getDatabaseName());
      assertThat(db.getSchema().existsType(partialA))
          .as("the prefix that succeeded is on server %d", serverIndex).isTrue();
      assertThat(db.getSchema().getType(partialA).getPropertyNames())
          .as("the prefix's properties are on server %d", serverIndex).contains("name");
      assertThat(db.getSchema().existsType(partialB))
          .as("nothing after the failing statement ran, on server %d either", serverIndex).isFalse();
    }
  }
}
