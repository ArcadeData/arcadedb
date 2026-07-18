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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.engine.ComponentFile;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * An open that fails must not delete the WAL files of the database it failed to open.
 * <p>
 * {@code releaseResourcesOnOpenFailure} closes the transaction manager, whose clean-close path deletes
 * every {@code *.wal} file in the database directory. That glob is directory-wide and mode-blind, so a
 * rejected open used to discard the WAL of a database that had not been cleanly closed - exactly the
 * files the following recovery-capable open needs to replay. Everything committed after the last page
 * flush was lost, and the recovery open could then observe a zero-length dictionary file and fail
 * outright while loading the schema.
 */
class FailedOpenPreservesWalTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("WalPreserveType");
  }

  @Test
  void rejectedReadOnlyOpenDoesNotDeleteTheWalNeededForRecovery() {
    database.transaction(() -> database.newDocument("WalPreserveType").set("k", 1).save());

    // Crash without a clean close: the lock marker stays on disk, so the next open must run recovery.
    ((DatabaseInternal) database).kill();
    database.close();

    assertThat(walFiles()).as("the killed database must leave WAL files to replay").isNotEmpty();

    // A READ_ONLY open of a database that needs recovery is rejected; releasing the resources it acquired
    // must not touch the WAL.
    assertThatThrownBy(() -> factory.open(ComponentFile.MODE.READ_ONLY)).isInstanceOf(RuntimeException.class);

    assertThat(walFiles()).as("a failed open must preserve the WAL files it does not own").isNotEmpty();

    // The recovery-capable open still succeeds and the schema created before the crash survived.
    database = factory.open();
    assertThat(database.isOpen()).isTrue();
    assertThat(database.getSchema().existsType("WalPreserveType")).isTrue();
  }

  private File[] walFiles() {
    final File[] files = new File(getDatabasePath()).listFiles((dir, name) -> name.endsWith(".wal"));
    return files == null ? new File[0] : files;
  }
}
