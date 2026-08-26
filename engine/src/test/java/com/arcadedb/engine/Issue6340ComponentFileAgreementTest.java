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
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6340, items 1 to 3: the last three places where a component and the file it holds could disagree about a
 * property that belongs to the file.
 * <p>
 * The set is the one issues #6283 (the file id) and #6314 (the page size, and the by-id name check) have been
 * closing one member at a time: a component built on an already-registered file must take EVERY file property from
 * that file, and the agreement must live in the API rather than in whoever remembered to check.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6340ComponentFileAgreementTest extends TestHelper {

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
