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
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6314, items 1 and 2: the two remaining places where a component and the file it holds could disagree
 * about how to address that file.
 * <p>
 * Item 1's guard is the page-size sibling of the file-id one issue #6283 added to {@link PaginatedComponent}: the
 * page size IS the stride, so a component that carries a different one from its file reads page N from
 * {@code N * theWrongNumber} - real bytes at the wrong offset, never an exception, and a {@code pageCount}
 * computed from the wrong divisor on top.
 * <p>
 * Item 2 is the by-id mirror of #6283's by-name hazard, pushed down from the one caller that had it into
 * {@link FileManager} itself.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6314FileAgreementTest extends TestHelper {

  /**
   * The file's page size is baked into its name, so this fixture builds the component with a page size the name
   * does not carry - which is exactly the shape a component re-deriving the value from a live configuration
   * setting ends up in when that setting changed between two runs.
   */
  @Test
  void aComponentThatDisagreesWithItsFileAboutThePageSizeFailsAtConstruction() throws IOException {
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
  void aComponentThatAgreesWithItsFileIsBuiltNormally() throws IOException {
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
  void theByIdGetOrCreateFileRefusesToHandBackAFileWithAnotherName() throws IOException {
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
}
