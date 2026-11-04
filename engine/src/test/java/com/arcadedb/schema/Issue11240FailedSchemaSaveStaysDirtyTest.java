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
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * A schema save that fails leaves the schema dirty, so the next transaction end or the close writes it (issue #11240): a
 * compaction swap records its migrated file without marking the schema dirty and saves it at once, and when that one save
 * failed nothing ever wrote the swap, so a reopen read the old file as the index.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11240FailedSchemaSaveStaysDirtyTest extends TestHelper {
  @Test
  void aFailedSaveIsRetriedOnceTheFileCanBeWritten() throws Exception {
    final LocalSchema schema = (LocalSchema) database.getSchema().getEmbedded();
    final File directory = new File(database.getDatabasePath());

    assertThat(directory.setWritable(false)).isTrue();
    final boolean enforced = !Files.isWritable(directory.toPath());
    if (!enforced)
      directory.setWritable(true);
    assumeTrue(enforced, "directory permissions are not enforced for this user");

    try {
      // as the compaction swap does: recorded without marking the schema dirty, then saved at once
      schema.setMigratedFileId(1_000, 1_001, false);
      assertThatThrownBy(schema::saveConfigurationOrThrow).isInstanceOf(IOException.class);
      assertThat(schema.isDirty()).as("a failed save leaves the schema to be written again").isTrue();
    } finally {
      assertThat(directory.setWritable(true)).isTrue();
    }

    schema.saveConfigurationOrThrow();
    assertThat(schema.isDirty()).isFalse();

    reopenDatabase();
    assertThat(((LocalSchema) database.getSchema().getEmbedded()).getMigratedFileId(1_000)).isEqualTo(1_001);
  }
}
