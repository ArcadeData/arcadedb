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
package com.arcadedb.engine;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6143: the diagnostic that lists the files a node holds and nothing claims.
 * <p>
 * A schema session that ships instalments and then loses leadership cannot send its own compensating removal, so the
 * files its instalments created stay on the other nodes with nothing referencing them - and only the node that FAILED
 * logs anything about it. This is what lets an operator ask the nodes that actually hold them.
 * <p>
 * The first test is the important one, and it is the reason the classification in {@link UnreferencedFiles} refuses to
 * guess: a diagnostic that cries orphan over a healthy database is worse than no diagnostic at all, because the
 * obvious response to it is to delete something. It builds one of everything the classification could get wrong -
 * paired external-property buckets, a compacted sub-index and its bloom filter, a manual index bound to no type,
 * time-series internals - and demands silence, before and after a reopen.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6143UnreferencedFilesTest extends TestHelper {

  @Test
  void aHealthyDatabaseHasNothingToReport() throws Exception {
    buildOneOfEverything();

    assertThat(UnreferencedFiles.scan(db()))
        .as("every file of a healthy database is claimed by something; a finding here is a false positive, and the "
            + "obvious reaction to one is to delete a file that is in use")
        .isEmpty();

    // A reopen is a different starting point, not a repetition: the components come from the directory scan rather
    // than from the DDL that created them, which is exactly how a node holding an abandoned file sees it too.
    reopenDatabase();

    assertThat(UnreferencedFiles.scan(db())).as("and still nothing after a reopen").isEmpty();
  }

  /**
   * The shape a follower holds WHILE an abandoned instalment sequence is live: the apply path created the file, and
   * only a later schema reload would have turned it into a component. Reproduced by creating the file exactly as
   * {@code ArcadeStateMachine.createNewFiles} does - through the file manager, with the schema untouched.
   */
  @Test
  void aFileWithNoSchemaComponentIsReported() throws Exception {
    final FileManager fileManager = db().getFileManager();
    final int fileId = fileManager.newFileId();
    final String fileName = "abandoned." + fileId + ".65536.v1." + LocalBucket.BUCKET_EXT;
    fileManager.getOrCreateFile(fileId, database.getDatabasePath() + File.separator + fileName);

    try {
      final List<UnreferencedFiles.UnreferencedFile> found = UnreferencedFiles.scan(db());

      assertThat(found).hasSize(1);
      assertThat(found.getFirst().fileId()).isEqualTo(fileId);
      assertThat(found.getFirst().fileName()).isEqualTo(fileName);
      assertThat(found.getFirst().reason()).contains("no schema component");
    } finally {
      // Dropped rather than left for the test teardown: an unreferenced file is precisely what the database drop
      // does not know about, and leaving it behind would leak the directory.
      fileManager.dropFile(fileId);
    }
  }

  /**
   * The same file after the next schema reload adopts it as a component: a bucket in nobody's type. A bucket created
   * with {@code CREATE BUCKET} and never attached to one looks identical, and is reported for the same reason -
   * nothing can read or write it while it belongs to no type.
   */
  @Test
  void aBucketNoTypeClaimsIsReported() {
    final String bucketName = "orphan_bucket";
    database.getSchema().createBucket(bucketName);
    final int fileId = database.getSchema().getBucketByName(bucketName).getFileId();

    try {
      final List<UnreferencedFiles.UnreferencedFile> found = UnreferencedFiles.scan(db());

      assertThat(found).hasSize(1);
      assertThat(found.getFirst().fileId()).isEqualTo(fileId);
      assertThat(found.getFirst().reason()).contains("belongs to no type");
    } finally {
      database.getSchema().dropBucket(bucketName);
    }
  }

  /**
   * What an abandoned index rebuild leaves: the index files are there and the index is registered, but no type
   * reaches it. Every file the index owns is reported, not only the one whose component could be classified.
   */
  @Test
  void anIndexNoTypeReferencesIsReported() {
    final String typeName = "Detached";
    final LocalDocumentType type = (LocalDocumentType) database.getSchema().createDocumentType(typeName);
    type.createProperty("name", Type.STRING);
    final TypeIndex typeIndex = database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, typeName,
        "name");

    final List<Integer> indexFileIds = typeIndex.getFileIds();
    assertThat(indexFileIds).as("the index must own at least one file to make this test mean anything").isNotEmpty();

    // Unlinks the index from its type without touching the files - the state left by a rebuild that created the new
    // index files and never published the schema that names them.
    type.removeTypeIndexInternal(typeIndex);

    final List<UnreferencedFiles.UnreferencedFile> found = UnreferencedFiles.scan(db());

    assertThat(found.stream().map(UnreferencedFiles.UnreferencedFile::fileId).toList())
        .as("every file of an index no type reaches is reported").containsAll(indexFileIds);
    assertThat(found).allSatisfy(
        file -> assertThat(file.reason()).contains("referenced by no type"));

    // Put it back so the type drop below reclaims the files.
    database.getSchema().dropType(typeName);
    assertThat(UnreferencedFiles.scan(db())).isEmpty();
  }

  /**
   * The report is what {@code CHECK DATABASE} publishes, in both modes, and it never repairs - and it is a result
   * key of its own rather than a WARNING. That last part is load-bearing: an unreferenced file is not a defect in
   * the data, the state is one supported operations produce, and every caller that reads an empty warning list as
   * "this database is clean" - {@code TestHelper.checkDatabaseIntegrity} among them - would otherwise start calling
   * a healthy database unhealthy.
   */
  @Test
  void checkDatabaseReportsThemWithoutRemovingThem() {
    final String bucketName = "orphan_bucket_checked";
    database.getSchema().createBucket(bucketName);
    final int fileId = database.getSchema().getBucketByName(bucketName).getFileId();

    try {
      final Map<String, Object> result = new DatabaseChecker(database).setFix(true).setVerboseLevel(0).check();

      assertThat((Collection<String>) result.get("unreferencedFiles"))
          .as("the finding has to reach the SQL result, not only the log")
          .anyMatch(s -> s.contains(bucketName));
      assertThat(db().getFileManager().existsFile(fileId))
          .as("FIX must not delete it: reclaiming disk is not worth the risk of removing a file whose reference this "
              + "walk simply does not know how to follow")
          .isTrue();
      assertThat((Collection<String>) result.get("warnings"))
          .as("and it must NOT be a warning: a bucket outside every type is a supported state, so folding it in "
              + "would make every zero-warnings assertion in the suite fail on a healthy database")
          .isEmpty();
    } finally {
      database.getSchema().dropBucket(bucketName);
    }
  }

  /**
   * The gauge publishes {@code count()}, which shares the walk with {@code scan()} and skips only the building of
   * each finding's description. Sharing it is the point - two walks would be two chances to disagree - so the two
   * must answer the same number on the same database.
   */
  @Test
  void countAgreesWithScan() {
    assertThat(UnreferencedFiles.count(db())).as("a healthy database").isZero();

    final String bucketName = "orphan_bucket_counted";
    database.getSchema().createBucket(bucketName);
    try {
      assertThat(UnreferencedFiles.count(db())).isEqualTo(UnreferencedFiles.scan(db()).size()).isEqualTo(1);
    } finally {
      database.getSchema().dropBucket(bucketName);
    }
  }

  @Test
  void aCleanCheckDatabaseSaysNoneRatherThanNothing() {
    final Map<String, Object> result = new DatabaseChecker(database).setVerboseLevel(0).check();

    assertThat(result).containsKey("unreferencedFiles");
    assertThat((Collection<String>) result.get("unreferencedFiles")).isEmpty();
  }

  // ---------------------------------------------------------------------------------------------------------------

  /**
   * One of every component kind whose reference the classification has to follow (or deliberately not follow):
   * primary buckets, a paired external-property bucket, LSM unique/not-unique indexes, a full-text index, a HASH
   * index, a compacted sub-index with its bloom filter, a manual index bound to no type, a vertex and an edge type,
   * and a time-series type with its own internal components.
   */
  private void buildOneOfEverything() throws Exception {
    final Schema schema = database.getSchema();

    // A hub whose edge list PROMOTES, so the per-type super-node stripe pool exists. This is the class CI caught and
    // this fixture did not have: those buckets belong to no type either, and StripedEdgeList reaches them by
    // composing the type name. Restored in the finally below - it is a global.
    final int savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(64);

    // Read when the mutable index is constructed, so it has to be set BEFORE the index below is created. Zero makes
    // any index compactable, which - together with the small index page size - is what puts the compacted sub-index
    // and its bloom filter, the two files no schema JSON names, under this test.
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);

    final LocalDocumentType doc = (LocalDocumentType) schema.createDocumentType("Doc", 3);
    doc.createProperty("name", Type.STRING);
    doc.createProperty("text", Type.STRING);
    doc.createProperty("code", Type.INTEGER);
    doc.createProperty("blob", Type.STRING).setExternal(true);
    schema.buildTypeIndex("Doc", new String[] { "name" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true)
        .withPageSize(1024).create();
    schema.createTypeIndex(Schema.INDEX_TYPE.FULL_TEXT, false, "Doc", "text");
    schema.createTypeIndex(Schema.INDEX_TYPE.HASH, false, "Doc", "code");

    final LocalDocumentType vertex = (LocalDocumentType) schema.createVertexType("V");
    vertex.createProperty("id", Type.INTEGER);
    schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "V", "id");
    schema.createEdgeType("E");

    schema.buildTimeSeriesType().withName("Sensor").withTimestamp("ts").withTag("sensor", Type.STRING)
        .withField("value", Type.DOUBLE).withShards(2).create();

    schema.buildManualIndex("manualIdx", new Type[] { Type.STRING }).withType(Schema.INDEX_TYPE.LSM_TREE)
        .withUnique(false).create();

    try {
      database.transaction(() -> {
        for (int i = 0; i < 2_000; i++)
          database.newDocument("Doc").set("name", "n" + i).set("text", "some text " + i).set("code", i)
              .set("blob", "x".repeat(300)).save();

        final MutableVertex hub = database.newVertex("V").set("id", -1).save();
        for (int i = 0; i < 300; i++)
          database.newVertex("V").set("id", i).save().newEdge("E", hub);
      });
    } finally {
      GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
    }

    assertThat(schema.getBuckets().stream().anyMatch(b -> b.getName().contains("_sn_stripe_")))
        .as("the hub must actually have promoted, or the stripe-pool case is not covered")
        .isTrue();

    // A compacted sub-index and its bloom filter are files no schema JSON names: the mutable index that owns them
    // does, through its header page. Forcing a compaction is what puts that link under test.
    boolean compacted = false;
    for (final Index index : schema.getIndexes())
      if (index instanceof LSMTreeIndex lsm) {
        lsm.scheduleCompaction();
        compacted |= lsm.compact();
      }
    assertThat(compacted).as("at least one index must have compacted, or the sub-index case is not covered").isTrue();

    for (final Index index : schema.getIndexes())
      if (index instanceof IndexInternal internal && internal.getFileIds().size() > 1)
        return;

    throw new AssertionError("no index ended up owning more than one file: the compacted sub-index case is untested");
  }

  /** The scan reads the file manager and the schema registries, both of which live on the internal interface. */
  private DatabaseInternal db() {
    return (DatabaseInternal) database;
  }
}
