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
import com.arcadedb.exception.SchemaException;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Invariants between a component and the file it holds (file id, page size, version, mode, name): the component must
 * take every file property from the file it is built on, and the agreement lives in the API rather than in the caller.
 * Covers issues #6283, #6314 and #6340, one section each.
 */
class ComponentFileAgreementTest extends TestHelper {
  // Issue #6283 (item 1): a {@link PaginatedComponent} must never end up holding a file whose id is not the one it
  // was built with. {@code FileManager.getOrCreateFile()} is keyed by the component NAME, so a component that takes
  // a fresh id from {@code newFileId()} and only then asks for its file is handed the already-registered one - and
  // from that moment addresses pages of an id that is not the file it holds. Before the fix the construction
  // succeeded and the damage surfaced much later, as "File with id N was not found" thrown from
  // {@code PageManager.loadPage} (issue #6198), or worse, as reads of some other component's file.
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

  // Issue #6314, items 1 and 2: the two remaining places where a component and the file it holds could disagree
  // about how to address that file.
  // <p>
  // Item 1's guard is the page-size sibling of the file-id one issue #6283 added to {@link PaginatedComponent}: the
  // page size IS the stride, so a component that carries a different one from its file reads page N from
  // {@code N * theWrongNumber} - real bytes at the wrong offset, never an exception, and a {@code pageCount}
  // computed from the wrong divisor on top.
  // <p>
  // Item 2 is the by-id mirror of #6283's by-name hazard, pushed down from the one caller that had it into
  // {@link FileManager} itself.
  /**
   * The file's page size is baked into its name, so this fixture builds the component with a page size the name
   * does not carry - which is exactly the shape a component re-deriving the value from a live configuration
   * setting ends up in when that setting changed between two runs.
   */
  @Test
  void aComponentThatDisagreesWithItsFileAboutThePageSizeFailsAtConstruction() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int filePageSize = GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getValueAsInteger();
    final String name = "issue6314_stride";
    final int fileId = db.getFileManager().newFileId();
    final String filePath =
        db.getDatabasePath() + File.separator + name + "." + fileId + "." + filePageSize + ".v" + LocalBucket.CURRENT_VERSION
            + "." + LocalBucket.BUCKET_EXT;

    try {
      assertThatThrownBy(
          () -> new LocalBucket(db, name, filePath, fileId, ComponentFile.MODE.READ_WRITE, filePageSize / 4,
              LocalBucket.CURRENT_VERSION))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining(name)
          .hasMessageContaining("page size " + (filePageSize / 4))
          .hasMessageContaining("has page size " + filePageSize);
    } finally {
      // The construction failed AFTER getOrCreateFile() had registered the file, exactly as the file-id guard
      // leaves it (issue #6283): nothing reclaims it, which is deliberate on a path no legitimate caller takes.
      db.getFileManager().dropFile(fileId);
    }
  }

  /**
   * The same construction with the page size the file name carries is the legitimate one, and it must still work:
   * a guard that rejected it would be a guard nobody could open a database through.
   */
  @Test
  void aComponentThatAgreesWithItsFileIsBuiltNormally() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int filePageSize = GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getValueAsInteger();
    final String name = "issue6314_agreed";
    final int fileId = db.getFileManager().newFileId();
    final String filePath =
        db.getDatabasePath() + File.separator + name + "." + fileId + "." + filePageSize + ".v" + LocalBucket.CURRENT_VERSION
            + "." + LocalBucket.BUCKET_EXT;

    final LocalBucket bucket = new LocalBucket(db, name, filePath, fileId, ComponentFile.MODE.READ_WRITE, filePageSize,
        LocalBucket.CURRENT_VERSION);
    try {
      assertThat(bucket.getPageSize()).isEqualTo(filePageSize);
      assertThat(bucket.getComponentFile().getPageSize()).isEqualTo(filePageSize);
      assertThat(bucket.getComponentFile().getFileId()).isEqualTo(fileId);
    } finally {
      bucket.close();
      db.getFileManager().dropFile(fileId);
    }
  }

  /**
   * Item 2: {@code getOrCreateFile(int, String)} is keyed by the id alone, so before the fix an id already
   * registered handed its file back whatever file name the caller had asked for. The only production caller (the
   * HA follower's {@code createNewFiles}) checks this itself before calling, which is why nothing was reachable -
   * the point of the guard is that the guarantee now belongs to the API rather than to whoever remembers.
   */
  @Test
  void theByIdGetOrCreateFileRefusesToHandBackAFileWithAnotherName() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final FileManager fileManager = db.getFileManager();
    final int fileId = fileManager.newFileId();
    final String registeredName = "issue6314_registered." + fileId + ".65536.v0.pcf";
    final String otherName = "issue6314_other." + fileId + ".65536.v0.pcf";

    final ComponentFile created = fileManager.getOrCreateFile(fileId, db.getDatabasePath() + File.separator + registeredName);
    try {
      // Asking again for the same file is the idempotent case and stays idempotent.
      assertThat(fileManager.getOrCreateFile(fileId, db.getDatabasePath() + File.separator + registeredName))
          .isSameAs(created);

      assertThatThrownBy(() -> fileManager.getOrCreateFile(fileId, db.getDatabasePath() + File.separator + otherName))
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining(registeredName)
          .hasMessageContaining(otherName);
    } finally {
      fileManager.dropFile(fileId);
    }
  }

  // Issue #6340, items 1 to 3: the last three places where a component and the file it holds could disagree about a
  // property that belongs to the file.
  // <p>
  // The set is the one issues #6283 (the file id) and #6314 (the page size, and the by-id name check) have been
  // closing one member at a time: a component built on an already-registered file must take EVERY file property from
  // that file, and the agreement must live in the API rather than in whoever remembered to check.
  /**
   * Item 1: the mode was the one file property with no accessor, so a component built on an existing file had to
   * guess it while reading the other four off the file.
   */
  @Test
  void aRegisteredFileReportsTheModeItWasOpenedWith() {
    final DatabaseInternal db = (DatabaseInternal) database;
    for (final ComponentFile file : db.getFileManager().getFiles())
      if (file != null)
        assertThat(file.getMode()).isEqualTo(ComponentFile.MODE.READ_WRITE);
  }

  /**
   * Item 2: the by-name {@code getOrCreateFile} consulted the caller's mode only on the miss path, so a hit handed
   * the registered file back whatever mode it carried. The direction that matters is the quiet one - asking for
   * READ_ONLY and being given a writable file is a weaker guarantee than the caller requested.
   */
  @Test
  void theByNameGetOrCreateFileRefusesToHandBackAFileOpenInAnotherMode() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final FileManager fileManager = db.getFileManager();
    final int pageSize = GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getValueAsInteger();
    final String name = "issue6340_mode";
    final int fileId = fileManager.newFileId();
    final String fileName = name + "." + fileId + "." + pageSize + ".v" + LocalBucket.CURRENT_VERSION + "."
        + LocalBucket.BUCKET_EXT;
    final String filePath = db.getDatabasePath() + File.separator + fileName;

    final ComponentFile created = fileManager.getOrCreateFile(name, filePath, ComponentFile.MODE.READ_WRITE);
    try {
      // Asking again in the mode the file carries is the idempotent case and stays idempotent.
      assertThat(fileManager.getOrCreateFile(name, filePath, ComponentFile.MODE.READ_WRITE)).isSameAs(created);

      assertThatThrownBy(() -> fileManager.getOrCreateFile(name, filePath, ComponentFile.MODE.READ_ONLY))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining(fileName)
          .hasMessageContaining("already open in mode READ_WRITE")
          .hasMessageContaining("requested in mode READ_ONLY");
    } finally {
      fileManager.dropFile(fileId);
    }
  }

  /**
   * Item 3: the version is the third fact baked into a component file's name and was the only one of the three not
   * asserted against the file the component ends up holding. It decides how the pages are INTERPRETED, so a
   * component that carries a different one from its file misreads real bytes rather than raising anything.
   */
  @Test
  void aComponentThatDisagreesWithItsFileAboutTheVersionFailsAtConstruction() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int pageSize = GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getValueAsInteger();
    final String name = "issue6340_version";
    final int fileId = db.getFileManager().newFileId();
    // The file name says version 1 - a paired external-property bucket, which is a real layout with a 256-slot page
    // table instead of 2048 - while the component below claims LocalBucket.CURRENT_VERSION. That is the shape a
    // component re-deriving its version from a constant instead of taking the one parsed off the name ends up in.
    final String filePath =
        db.getDatabasePath() + File.separator + name + "." + fileId + "." + pageSize + ".v1." + LocalBucket.BUCKET_EXT;

    try {
      assertThatThrownBy(
          () -> new LocalBucket(db, name, filePath, fileId, ComponentFile.MODE.READ_WRITE, pageSize,
              LocalBucket.CURRENT_VERSION))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining(name)
          .hasMessageContaining("version " + LocalBucket.CURRENT_VERSION)
          .hasMessageContaining("has version 1");
    } finally {
      // As with the id and page-size guards, the construction failed AFTER getOrCreateFile() had registered the
      // file and nothing reclaims it: deliberate on a path no legitimate caller takes.
      db.getFileManager().dropFile(fileId);
    }
  }

  /**
   * The guard is a tripwire, not a compatibility gate: a component built with the version its own file name
   * carries is the legitimate case and must still open, whatever {@code CURRENT_VERSION} happens to be. Every load
   * path passes the parsed version straight through, so this is the shape every existing database opens in.
   */
  @Test
  void aComponentBuiltOnTheVersionItsFileNameCarriesOpensNormally() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int pageSize = GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getValueAsInteger();
    final String name = "issue6340_otherversion";
    final int fileId = db.getFileManager().newFileId();
    final String filePath =
        db.getDatabasePath() + File.separator + name + "." + fileId + "." + pageSize + ".v1." + LocalBucket.BUCKET_EXT;

    final LocalBucket bucket = new LocalBucket(db, name, filePath, fileId, ComponentFile.MODE.READ_WRITE, pageSize, 1);
    try {
      assertThat(bucket.getVersion()).isEqualTo(1);
      assertThat(bucket.getComponentFile().getVersion()).isEqualTo(1);
      assertThat(bucket.getComponentFile().getMode()).isEqualTo(ComponentFile.MODE.READ_WRITE);
    } finally {
      bucket.close();
      db.getFileManager().dropFile(fileId);
    }
  }
}
