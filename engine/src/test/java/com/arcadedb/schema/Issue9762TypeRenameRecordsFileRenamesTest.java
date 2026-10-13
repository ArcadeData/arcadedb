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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.FileManager;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9762: a type rename moves its files before it opens the schema recording session that publishes it, so it hands
 * that session the renames itself. Under HA the session ships them to the followers, which move the same files; a file
 * left out is a file a follower keeps under the old name while its schema names the new one.
 * <p>
 * The session is opened here by hand, the way the HA layer opens it around the publishing frame, and what the rename
 * recorded is compared with the files that actually moved on disk.
 */
class Issue9762TypeRenameRecordsFileRenamesTest extends TestHelper {

  @Test
  void aDocumentTypeRenameRecordsEveryBucketAndIndexFileItMoved() {
    database.command("sql", "CREATE DOCUMENT TYPE R BUCKETS 3");
    database.command("sql", "CREATE PROPERTY R.id INTEGER");
    database.command("sql", "CREATE INDEX ON R (id) UNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < 20; i++)
        database.newDocument("R").set("id", i).save();
    });

    final Map<Integer, String> before = componentNames();
    final Map<Integer, String> recorded = renameRecording("R", "R2");

    assertThat(recorded).as("the 3 buckets and the 3 sub-index files").hasSize(6);
    assertRecordedIsWhatMoved(before, recorded, "R2");
  }

  @Test
  void aVertexTypeRenameRecordsItsEdgeBucketsToo() {
    database.command("sql", "CREATE VERTEX TYPE Vx BUCKETS 2");
    database.command("sql", "CREATE EDGE TYPE Ex");
    database.transaction(() -> {
      MutableVertex previous = null;
      for (int i = 0; i < 10; i++) {
        final MutableVertex v = database.newVertex("Vx").save();
        if (previous != null)
          previous.newEdge("Ex", v).save();
        previous = v;
      }
    });

    final Map<Integer, String> before = componentNames();
    final List<Bucket> involved = database.getSchema().getType("Vx").getInvolvedBuckets();
    final Map<Integer, String> recorded = renameRecording("Vx", "Vx2");

    assertThat(involved.size()).as("buckets and their edge buckets").isGreaterThan(2);
    for (final Bucket bucket : involved)
      assertThat(recorded).as("bucket %s", bucket.getName()).containsKey(bucket.getFileId());
    assertRecordedIsWhatMoved(before, recorded, "Vx2");
  }

  @Test
  void anotherTypesFilesAreNeverRecorded() {
    database.command("sql", "CREATE DOCUMENT TYPE A");
    database.command("sql", "CREATE DOCUMENT TYPE B");
    final int bucketOfB = database.getSchema().getType("B").getBuckets(false).getFirst().getFileId();

    final Map<Integer, String> recorded = renameRecording("A", "A2");

    assertThat(recorded).isNotEmpty().doesNotContainKey(bucketOfB);
  }

  @Test
  void withoutASessionNothingIsRecordedAndTheRenameStillHappens() {
    database.command("sql", "CREATE DOCUMENT TYPE N");

    database.getSchema().getType("N").rename("N2");

    assertThat(database.getSchema().existsType("N2")).isTrue();
    assertThat(fileManager().getRecordedRenames()).isEmpty();
  }

  /**
   * The publishing session refuses after the files were moved - under HA, leadership lost between the pre-check and the
   * session. The wrapper refuses the session the way {@code RaftReplicatedDatabase} would, through the same seam the HA
   * server installs it on. The rename is put back: type, buckets and index files under the old name, after a reopen too.
   */
  @Test
  void aRefusalFromThePublishingSessionPutsEverythingBack() {
    database.command("sql", "CREATE DOCUMENT TYPE P BUCKETS 2");
    database.command("sql", "CREATE PROPERTY P.id INTEGER");
    database.command("sql", "CREATE INDEX ON P (id) UNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.newDocument("P").set("id", i).save();
    });
    final Map<Integer, String> before = componentNames();

    final LocalDatabase local = (LocalDatabase) database;
    final DatabaseInternal previous = local.getWrappedDatabaseInstance();
    final boolean[] refused = new boolean[1];
    local.setWrappedDatabaseInstance((DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
        new Class<?>[] { DatabaseInternal.class }, (proxy, method, args) -> {
          if ("recordFileChanges".equals(method.getName())) {
            refused[0] = true;
            throw new NeedRetryException("leadership lost (issue 9762 test)");
          }
          try {
            return method.invoke(local, args);
          } catch (final InvocationTargetException e) {
            throw e.getCause();
          }
        }));
    try {
      assertThatThrownBy(() -> database.getSchema().getType("P").rename("P2")).isInstanceOf(NeedRetryException.class)
          .hasMessageContaining("leadership lost");
    } finally {
      local.setWrappedDatabaseInstance(previous);
    }

    assertThat(refused[0]).as("the refusal came from the publishing session, after the files moved").isTrue();
    assertThat(componentNames()).as("every file back under its old name").isEqualTo(before);
    assertThat(database.getSchema().existsType("P")).isTrue();
    assertThat(database.getSchema().existsType("P2")).isFalse();

    reopenDatabase();

    assertThat(database.getSchema().existsType("P")).isTrue();
    assertThat(database.getSchema().existsType("P2")).isFalse();
    assertThat(database.countType("P", false)).isEqualTo(10);
    assertThat(database.query("sql", "SELECT FROM P WHERE id = 7").hasNext()).isTrue();
  }

  /** Renames {@code from} inside a recording session and returns the renames it recorded. */
  private Map<Integer, String> renameRecording(final String from, final String to) {
    final FileManager fileManager = fileManager();
    assertThat(fileManager.startRecordingChanges()).isTrue();
    try {
      database.getSchema().getType(from).rename(to);
      return fileManager.getRecordedRenames();
    } finally {
      fileManager.stopRecordingChanges();
    }
  }

  /** Every recorded rename names the file's current component, and every file that changed name was recorded. */
  private void assertRecordedIsWhatMoved(final Map<Integer, String> before, final Map<Integer, String> recorded,
      final String newTypeName) {
    final Map<Integer, String> after = componentNames();
    for (final Map.Entry<Integer, String> rename : recorded.entrySet()) {
      assertThat(after.get(rename.getKey())).as("file %d", rename.getKey()).isEqualTo(rename.getValue());
      assertThat(rename.getValue()).startsWith(newTypeName + "_");
    }
    for (final Map.Entry<Integer, String> file : after.entrySet())
      if (before.containsKey(file.getKey()) && !before.get(file.getKey()).equals(file.getValue()))
        assertThat(recorded).as("moved file %d '%s' -> '%s'", file.getKey(), before.get(file.getKey()), file.getValue())
            .containsEntry(file.getKey(), file.getValue());

    final DocumentType type = database.getSchema().getType(newTypeName);
    for (final TypeIndex index : type.getAllIndexes(false))
      for (final IndexInternal sub : index.getIndexesOnBuckets())
        assertThat(recorded).as("index file of %s", sub.getName()).containsKey(sub.getFileId());
  }

  private Map<Integer, String> componentNames() {
    final Map<Integer, String> names = new HashMap<>();
    for (final ComponentFile file : fileManager().getFiles())
      if (file != null && file.getComponentName() != null)
        names.put(file.getFileId(), file.getComponentName());
    return names;
  }

  private FileManager fileManager() {
    return ((DatabaseInternal) database).getFileManager();
  }
}
