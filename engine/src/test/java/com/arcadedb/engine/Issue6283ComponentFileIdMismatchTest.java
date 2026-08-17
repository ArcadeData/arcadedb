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
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6283 (item 1): a {@link PaginatedComponent} must never end up holding a file whose id is not the one it
 * was built with. {@code FileManager.getOrCreateFile()} is keyed by the component NAME, so a component that takes
 * a fresh id from {@code newFileId()} and only then asks for its file is handed the already-registered one - and
 * from that moment addresses pages of an id that is not the file it holds. Before the fix the construction
 * succeeded and the damage surfaced much later, as "File with id N was not found" thrown from
 * {@code PageManager.loadPage} (issue #6198), or worse, as reads of some other component's file.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6283ComponentFileIdMismatchTest extends TestHelper {

  @Test
  void componentBuiltOnAForeignFileIdFailsAtConstruction() {
    final DatabaseInternal db = (DatabaseInternal) database;

    database.getSchema().createDocumentType("Issue6283Type", 1);
    final Bucket existing = database.getSchema().getType("Issue6283Type").getBuckets(false).getFirst();
    final String name = existing.getName();
    final int registeredFileId = existing.getFileId();

    // Same component name, id-allocating constructor: it mints a new id, then getOrCreateFile() - keyed by the
    // name - hands it back the file registered under that name, which carries registeredFileId.
    assertThatThrownBy(() -> newBucketOnAllocatedId(db, name))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(name)
        .hasMessageContaining("has id " + registeredFileId);

    // The failed construction left the registered file alone, so the bucket is still fully usable.
    assertThat(db.getFileManager().getFileByComponentName(name).getFileId()).isEqualTo(registeredFileId);
    database.transaction(() -> database.newDocument("Issue6283Type").set("id", 1).save());
    assertThat(database.countType("Issue6283Type", false)).isEqualTo(1);
  }

  @Test
  void everyRegisteredComponentHoldsItsOwnFile() {
    final DatabaseInternal db = (DatabaseInternal) database;

    database.getSchema().createVertexType("Issue6283Vertex", 3).createProperty("name", Type.STRING)
        .createIndex(Schema.INDEX_TYPE.LSM_TREE, true);
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.newVertex("Issue6283Vertex").set("name", "v" + i).save();
    });

    final LocalSchema schema = (LocalSchema) database.getSchema();
    int checked = 0;
    for (final ComponentFile file : db.getFileManager().getFiles()) {
      if (file == null)
        continue;
      if (schema.getFileByIdIfExists(file.getFileId()) instanceof PaginatedComponent component) {
        assertThat(component.getComponentFile().getFileId()).as("component '%s'", component.getName())
            .isEqualTo(component.getFileId());
        checked++;
      }
    }
    // The dictionary, three buckets and the index files at the very least: without this the loop above would
    // pass by walking nothing.
    assertThat(checked).isGreaterThan(3);
  }

  /**
   * A component whose id comes from {@code FileManager.newFileId()}, which is the constructor the invariant
   * guards. Nothing here is bucket-specific: {@link LocalBucket} is simply the shortest real subclass to build.
   */
  private LocalBucket newBucketOnAllocatedId(final DatabaseInternal db, final String name) throws IOException {
    return new LocalBucket(db, name, db.getDatabasePath() + File.separator + name, ComponentFile.MODE.READ_WRITE,
        GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getValueAsInteger(), LocalBucket.CURRENT_VERSION);
  }
}
