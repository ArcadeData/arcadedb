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
import com.arcadedb.engine.UnreferencedFiles.MemoizedCount;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class UnreferencedFilesTest {

  @Nested
  class UnreferencedFilesDetection extends TestHelper {

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

  @Nested
  class MemoizedUnreferencedCount extends TestHelper {

    /** No underscore, so the derived-name rule can never attribute it to a type and call it claimed. */
    private static final String DETACHED_BUCKET = "detachedbucket";

    @Test
    void anUnchangedDatabaseIsAnsweredFromTheCache() {
      final MemoizedCount memoized = new MemoizedCount();

      assertThat(memoized.get(db())).as("a healthy database").isZero();
      assertThat(memoized.getRecomputations()).as("the first call has nothing cached").isEqualTo(1);

      for (int i = 0; i < 10; i++)
        assertThat(memoized.get(db())).isZero();

      assertThat(memoized.getRecomputations())
          .as("nothing changed, so the walk - one getFileIds() per index, each taking that index's read lock - must "
              + "not have run again")
          .isEqualTo(1);
    }

    /**
     * The first half of the gate. A standalone {@code CREATE BUCKET} leaves a file no type claims, and it is a FILE
     * that appeared, which is what {@code FileManager.getModificationCount()} answers for.
     */
    @Test
    void aNewFileInvalidatesTheEntry() {
      final MemoizedCount memoized = new MemoizedCount();
      assertThat(memoized.get(db())).isZero();

      database.getSchema().createBucket(DETACHED_BUCKET);
      try {
        assertThat(memoized.get(db())).as("the new file is claimed by nothing and must be counted").isEqualTo(1);
        assertThat(memoized.getRecomputations()).isEqualTo(2);
      } finally {
        database.getSchema().dropBucket(DETACHED_BUCKET);
      }

      assertThat(memoized.get(db())).as("and dropping it takes the count back down").isZero();
    }

    /**
     * The second half of the gate, and the one a file counter alone cannot cover: detaching a bucket from its type
     * creates and drops nothing, so the answer changes with NO file having moved. Asserting the file modification
     * count is unchanged across the mutation is what makes this a test of the schema-version half specifically.
     */
    @Test
    void aSchemaChangeThatStopsClaimingAFileInvalidatesTheEntry() {
      final MemoizedCount memoized = new MemoizedCount();

      final DocumentType type = database.getSchema().createDocumentType("Issue6168Owner");
      type.createProperty("id", Type.INTEGER);
      final Bucket detached = database.getSchema().createBucket(DETACHED_BUCKET);
      type.addBucket(detached);

      assertThat(memoized.get(db())).as("a bucket its type lists is claimed").isZero();

      final long filesBefore = db().getFileManager().getModificationCount();
      type.removeBucket(detached);
      assertThat(db().getFileManager().getModificationCount())
          .as("detaching a bucket creates and drops no file, so only the schema version can have moved")
          .isEqualTo(filesBefore);

      assertThat(memoized.get(db())).as("nothing claims the bucket any more").isEqualTo(1);
      assertThat(memoized.get(db())).isEqualTo(UnreferencedFiles.count(db()));

      database.getSchema().dropBucket(DETACHED_BUCKET);
      database.getSchema().dropType("Issue6168Owner");
    }

    /**
     * The property that actually matters, asserted over a sequence rather than at one point: whatever the gate does,
     * the memoized answer is the answer a fresh walk gives. A gate that missed a change would show up here as a
     * disagreement at the step after it.
     */
    @Test
    void theMemoizedAnswerAgreesWithAFreshWalkAtEveryStep() {
      final MemoizedCount memoized = new MemoizedCount();
      assertThatBothAgree(memoized);

      final DocumentType type = database.getSchema().createDocumentType("Issue6168Seq");
      type.createProperty("id", Type.INTEGER);
      assertThatBothAgree(memoized);

      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
      assertThatBothAgree(memoized);

      final Bucket detached = database.getSchema().createBucket(DETACHED_BUCKET);
      assertThatBothAgree(memoized);

      type.addBucket(detached);
      assertThatBothAgree(memoized);

      type.removeBucket(detached);
      assertThatBothAgree(memoized);

      database.getSchema().dropBucket(DETACHED_BUCKET);
      assertThatBothAgree(memoized);

      database.getSchema().dropType("Issue6168Seq");
      assertThatBothAgree(memoized);
    }

    private void assertThatBothAgree(final MemoizedCount memoized) {
      assertThat(memoized.get(db())).isEqualTo(UnreferencedFiles.count(db()));
    }

    /** The count reads the file manager and the schema registries, both of which live on the internal interface. */
    private DatabaseInternal db() {
      return (DatabaseInternal) database;
    }
  }

  @Nested
  class ReclaimUnreferencedFiles extends TestHelper {

    /**
     * The headline case and the one the issue is actually about: a file the file manager holds with no schema
     * component ever built for it - reproduced, like {@code UnreferencedFilesTest.UnreferencedFilesDetection}, exactly as
     * {@code ArcadeStateMachine.createNewFiles} does on a follower mid-instalment.
     */
    @Test
    void reclaimRemovesAFileWithNoSchemaComponent() throws Exception {
      final FileManager fileManager = db().getFileManager();
      final int fileId = fileManager.newFileId();
      final String fileName = "abandoned." + fileId + ".65536.v1." + LocalBucket.BUCKET_EXT;
      fileManager.getOrCreateFile(fileId, database.getDatabasePath() + File.separator + fileName);

      final Map<String, Object> result = new DatabaseChecker(db()).setFix(true).setReclaimUnreferencedFiles(true)
          .setVerboseLevel(0).check();

      assertThat((Collection<String>) result.get("unreferencedFiles"))
          .as("the finding is still reported, same as plain FIX")
          .anyMatch(s -> s.contains(fileName));
      assertThat((Collection<String>) result.get("reclaimedUnreferencedFiles"))
          .as("and this time it was also reclaimed")
          .anyMatch(s -> s.contains(fileName));
      assertThat(fileManager.existsFile(fileId)).as("the file must actually be gone from disk/registry").isFalse();
    }

    /**
     * Without {@code RECLAIM UNREFERENCED FILES}, plain {@code CHECK DATABASE FIX} must keep behaving exactly as
     * #6143 left it: report only, never delete. This is the regression guard for the opt-in boundary.
     */
    @Test
    void plainFixDoesNotReclaimWithoutTheClause() throws Exception {
      final FileManager fileManager = db().getFileManager();
      final int fileId = fileManager.newFileId();
      final String fileName = "abandoned." + fileId + ".65536.v1." + LocalBucket.BUCKET_EXT;
      fileManager.getOrCreateFile(fileId, database.getDatabasePath() + File.separator + fileName);

      try {
        final Map<String, Object> result = new DatabaseChecker(db()).setFix(true).setVerboseLevel(0).check();

        assertThat((Collection<String>) result.get("unreferencedFiles")).anyMatch(s -> s.contains(fileName));
        assertThat((Collection<String>) result.get("reclaimedUnreferencedFiles"))
            .as("nothing is reclaimed unless explicitly asked for").isEmpty();
        assertThat(fileManager.existsFile(fileId)).as("plain FIX must not delete it").isTrue();
      } finally {
        fileManager.dropFile(fileId);
      }
    }

    /**
     * A bucket no type claims IS a registered schema component. Reclaim must leave it alone: deleting only its file
     * would strand a {@code LocalBucket} the schema still lists, pointing at nothing.
     */
    @Test
    void reclaimLeavesAnUnownedBucketUntouched() {
      final String bucketName = "issue6189_orphan_bucket";
      database.getSchema().createBucket(bucketName);
      final int fileId = database.getSchema().getBucketByName(bucketName).getFileId();

      try {
        final Map<String, Object> result = new DatabaseChecker(db()).setFix(true).setReclaimUnreferencedFiles(true)
            .setVerboseLevel(0).check();

        assertThat((Collection<String>) result.get("unreferencedFiles")).anyMatch(s -> s.contains(bucketName));
        assertThat((Collection<String>) result.get("reclaimedUnreferencedFiles"))
            .as("an unowned bucket is a schema component, not a raw file - reclaim must not touch it")
            .noneMatch(s -> s.contains(bucketName));
        assertThat(db().getFileManager().existsFile(fileId)).isTrue();
      } finally {
        database.getSchema().dropBucket(bucketName);
      }
    }

    /**
     * Same reasoning for an automatic index no type references: the index is still a registered schema component,
     * so its files stay out of reach of the raw-file reclaim.
     */
    @Test
    void reclaimLeavesAnUnownedIndexUntouched() {
      final String typeName = "Issue6189Detached";
      final LocalDocumentType type = (LocalDocumentType) database.getSchema().createDocumentType(typeName);
      type.createProperty("name", Type.STRING);
      final TypeIndex typeIndex = database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, typeName,
          "name");
      final List<Integer> indexFileIds = typeIndex.getFileIds();
      assertThat(indexFileIds).isNotEmpty();

      type.removeTypeIndexInternal(typeIndex);

      try {
        final Map<String, Object> result = new DatabaseChecker(db()).setFix(true).setReclaimUnreferencedFiles(true)
            .setVerboseLevel(0).check();

        assertThat((Collection<String>) result.get("unreferencedFiles")).anyMatch(s -> s.contains("Issue6189Detached"));
        assertThat((Collection<String>) result.get("reclaimedUnreferencedFiles")).isEmpty();
        for (final int indexFileId : indexFileIds)
          assertThat(db().getFileManager().existsFile(indexFileId)).isTrue();
      } finally {
        database.getSchema().dropType(typeName);
      }
    }

    @Test
    void reclaimIsANoOpOnAHealthyDatabase() {
      final Map<String, Object> result = new DatabaseChecker(db()).setFix(true).setReclaimUnreferencedFiles(true)
          .setVerboseLevel(0).check();

      assertThat(result).containsKey("unreferencedFiles").containsKey("reclaimedUnreferencedFiles");
      assertThat((Collection<String>) result.get("unreferencedFiles")).isEmpty();
      assertThat((Collection<String>) result.get("reclaimedUnreferencedFiles")).isEmpty();
    }

    /** {@code RECLAIM UNREFERENCED FILES} removes files, so - like {@code DELETE ORPHANS} (#6090) - it needs FIX. */
    @Test
    void reclaimWithoutFixIsRefused() {
      assertThatThrownBy(() -> database.command("sql", "CHECK DATABASE RECLAIM UNREFERENCED FILES"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("RECLAIM UNREFERENCED FILES");
    }

    /** End-to-end through the SQL statement, not only the {@link DatabaseChecker} API directly. */
    @Test
    void reclaimWorksThroughTheSqlStatement() throws Exception {
      final FileManager fileManager = db().getFileManager();
      final int fileId = fileManager.newFileId();
      final String fileName = "abandoned." + fileId + ".65536.v1." + LocalBucket.BUCKET_EXT;
      fileManager.getOrCreateFile(fileId, database.getDatabasePath() + File.separator + fileName);

      try (final ResultSet rs = database.command("sql", "CHECK DATABASE FIX RECLAIM UNREFERENCED FILES")) {
        final Result row = rs.next();
        assertThat((Collection<String>) row.getProperty("reclaimedUnreferencedFiles")).anyMatch(s -> s.contains(fileName));
      }

      assertThat(fileManager.existsFile(fileId)).isFalse();
    }

    /**
     * The race a reviewer of #6189 flagged: {@code UnreferencedFiles.scan} reads live, unlocked registries, and the
     * {@code NO_SCHEMA_COMPONENT} shape it proves is also what a legitimate, still-in-progress instalment sequence
     * looks like before it publishes. If a schema component attaches to a file's id AFTER it was found but BEFORE
     * {@code reclaimUnreferencedFiles} gets to it, deleting it would strand a live component. Reproduced
     * deterministically - no real concurrency needed - by handing the package-private reclaim step a finding for a
     * file id that has, by the time it runs, gained a real schema component: exactly what a scan-then-act gap
     * without a re-check would miss.
     */
    @Test
    void reclaimSkipsAFileASchemaComponentClaimedAfterTheScan() {
      // A real schema component now sitting at some file id - stands in for the schema reload that races the reclaim.
      final String bucketName = "issue6189_raced_bucket";
      database.getSchema().createBucket(bucketName);
      final int fileId = database.getSchema().getBucketByName(bucketName).getFileId();
      final String fileName = db().getFileManager().getFile(fileId).getFileName();

      try {
        // Seeds `result` with every key check() initializes, exactly as a real run would - the seam this test uses
        // does not re-derive that setup, only the one step under test.
        final DatabaseChecker checker = new DatabaseChecker(db()).setFix(true).setReclaimUnreferencedFiles(true)
            .setVerboseLevel(0);
        final Map<String, Object> result = checker.check();

        // A finding claiming NO_SCHEMA_COMPONENT for a file id that, right now, IS claimed - the stale snapshot the
        // race produces. UnreferencedFile's canonical constructor is public precisely so this is constructible.
        final UnreferencedFiles.UnreferencedFile staleFinding = new UnreferencedFiles.UnreferencedFile(fileId,
            fileName, UnreferencedFiles.Kind.NO_SCHEMA_COMPONENT, "the file exists on this node but no schema "
            + "component was ever built for it");

        checker.reclaimUnreferencedFiles(List.of(staleFinding));

        assertThat((Collection<String>) result.get("reclaimedUnreferencedFiles"))
            .as("a file a schema component now claims must never be deleted, whatever an earlier scan said")
            .noneMatch(s -> s.contains(fileName));
        assertThat((Collection<String>) result.get("warnings"))
            .as("the skip must be visible, not silent")
            .anyMatch(w -> w.contains(fileName) && w.contains("no longer safe"));
        assertThat(db().getFileManager().existsFile(fileId))
            .as("the live bucket's file must survive").isTrue();
        assertThat(database.getSchema().getBucketByName(bucketName).getFileId())
            .as("and the schema's own view of it must be unaffected").isEqualTo(fileId);
      } finally {
        database.getSchema().dropBucket(bucketName);
      }
    }

    /**
     * A second reviewer note on #6189: {@code FileManager.dropFile} silently no-ops when the file id no longer
     * resolves, so without its own check the reclaim step would still count - and log - a no-op drop as a genuine
     * reclaim. Reproduced the same way as the schema race above: hand the package-private step a finding for a file
     * id that is already gone by the time it runs.
     */
    @Test
    void reclaimSkipsAFileAlreadyGoneWithoutCountingIt() throws Exception {
      // Seeds `result` with every key check() initializes, on a healthy database - BEFORE the raceable file below
      // exists, so this call's own real scan/reclaim cannot touch it and this test stays a check on the package-
      // private step alone, not a second exercise of the whole check() pipeline.
      final DatabaseChecker checker = new DatabaseChecker(db()).setFix(true).setReclaimUnreferencedFiles(true)
          .setVerboseLevel(0);
      final Map<String, Object> result = checker.check();

      final FileManager fileManager = db().getFileManager();
      final int fileId = fileManager.newFileId();
      final String fileName = "already_gone." + fileId + ".65536.v1." + LocalBucket.BUCKET_EXT;
      fileManager.getOrCreateFile(fileId, database.getDatabasePath() + File.separator + fileName);

      // Gone before the reclaim step ever sees it - stands in for a concurrent reclaim, or any other drop, winning
      // the race. The finding itself is stale in exactly the way #6189's first review round already covers for the
      // schema side; this covers the file-manager side.
      fileManager.dropFile(fileId);

      final UnreferencedFiles.UnreferencedFile staleFinding = new UnreferencedFiles.UnreferencedFile(fileId, fileName,
          UnreferencedFiles.Kind.NO_SCHEMA_COMPONENT, "the file exists on this node but no schema component was ever "
          + "built for it");

      checker.reclaimUnreferencedFiles(List.of(staleFinding));

      assertThat((Collection<String>) result.get("reclaimedUnreferencedFiles"))
          .as("nothing was actually deleted by this call, so it must not be counted as reclaimed")
          .noneMatch(s -> s.contains(fileName));
      assertThat((Collection<String>) result.get("warnings"))
          .as("an already-gone file is not a problem worth warning about, unlike the schema-race case above")
          .noneMatch(w -> w.contains(fileName));
    }

    /** The scan/reclaim read and write the file manager and the schema registries, both on the internal interface. */
    private DatabaseInternal db() {
      return (DatabaseInternal) database;
    }
  }
}
