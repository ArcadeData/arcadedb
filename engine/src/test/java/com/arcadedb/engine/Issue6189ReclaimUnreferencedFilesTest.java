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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #6189: {@code CHECK DATABASE FIX RECLAIM UNREFERENCED FILES}, the operator-triggered
 * reclaim built on top of the #6143 diagnostic ({@link UnreferencedFiles}).
 * <p>
 * The clause deletes only {@link UnreferencedFiles.Kind#NO_SCHEMA_COMPONENT} findings - what an abandoned schema-WAL
 * instalment sequence leaves on a follower. The other two provable shapes, {@code UNOWNED_BUCKET} and
 * {@code UNOWNED_INDEX}, still name a live schema component and are left for the operator to remove through the
 * ordinary {@code DROP BUCKET} / index tooling instead, exactly as plain {@code FIX} already leaves them (#6143).
 */
class Issue6189ReclaimUnreferencedFilesTest extends TestHelper {

  /**
   * The headline case and the one the issue is actually about: a file the file manager holds with no schema
   * component ever built for it - reproduced, like {@code Issue6143UnreferencedFilesTest}, exactly as
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
