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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #11593: {@code FileManager.newFileId()} reserves a slot of the file table before the component that
 * asked for it opens its file. When the construction fails - the loser of two transactions creating the same super-node
 * stripe bucket at once - the slot stays reserved, and {@code CHECK DATABASE} reported it as an unreferenced file
 * ("no schema component was ever built for it") that no file on disk backs and {@code RECLAIM UNREFERENCED FILES} could
 * not remove. A reserved slot is not a file.
 */
class ReservedFileSlotNotUnreferencedIssue11593Test extends TestHelper {

  @Test
  void aReservedSlotIsNotAnUnreferencedFile() {
    database.command("sql", "CREATE VERTEX TYPE V");
    final DatabaseInternal db = (DatabaseInternal) database;
    assertThat(UnreferencedFiles.scan(db)).isEmpty();

    // The state a failed component construction leaves behind: an id taken, no file opened on it
    db.getFileManager().newFileId();
    db.getFileManager().newFileId();

    assertThat(UnreferencedFiles.scan(db)).isEmpty();
    assertThat(UnreferencedFiles.count(db)).isZero();

    try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
      final Result result = rs.next();
      final Collection<?> unreferenced = result.getProperty("unreferencedFiles");
      assertThat(unreferenced == null || unreferenced.isEmpty()).as("unreferencedFiles: %s", unreferenced).isTrue();
    }

    // A component created after them still gets an id of its own and is claimed as usual
    database.command("sql", "CREATE VERTEX TYPE W");
    assertThat(UnreferencedFiles.scan(db)).isEmpty();
  }
}
