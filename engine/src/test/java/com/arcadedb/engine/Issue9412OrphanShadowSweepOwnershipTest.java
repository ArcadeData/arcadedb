/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.database.Database;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9412: the orphan snapshot-shadow sweep ran before the database lock was taken and on READ_ONLY opens, so an
 * open that did not own the database deleted a file another process could be using for a live snapshot window. Only a
 * READ_WRITE open, once it holds the exclusive lock, may sweep.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9412OrphanShadowSweepOwnershipTest extends TestHelper {

  @Test
  void readOnlyOpenLeavesTheShadowAlone() throws Exception {
    final String path = database.getDatabasePath();
    database.close();

    final File shadow = new File(path, "snapshot-9412." + PageSnapshot.SHADOW_FILE_EXT);
    Files.write(shadow.toPath(), new byte[] { 1, 2, 3 });

    final Database readOnly = factory.open(ComponentFile.MODE.READ_ONLY);
    readOnly.close();
    assertThat(shadow).as("a READ_ONLY open does not own the database").exists();

    database = factory.open();
    assertThat(shadow).as("a READ_WRITE open holding the lock sweeps the orphan").doesNotExist();
  }
}
