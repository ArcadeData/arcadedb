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
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #11224: a schema load that fell back to {@code schema.prev.json} must say so, so a caller that
 * needs the intended files (snapshot install) can refuse instead of going on with a database that looks fine.
 */
class Issue11224LoadedFromPreviousSchemaTest extends TestHelper {
  private static final String TRUNCATED_SCHEMA = "{\"schemaVersion\":42,\"settings\":{\"dateForm";

  @Test
  void cleanLoadDoesNotReportFallback() {
    database.getSchema().createDocumentType("A");
    database.close();
    database = factory.open();
    assertThat(schema().isLoadedFromPreviousSchema()).isFalse();
  }

  @Test
  void corruptPrimaryReportsFallback() throws Exception {
    prepare();
    final File schemaFile = schemaFile();
    Files.writeString(schemaFile.toPath(), TRUNCATED_SCHEMA);
    database = factory.open();
    assertThat(database.getSchema().existsType("A")).isTrue();
    assertThat(schema().isLoadedFromPreviousSchema()).isTrue();
  }

  @Test
  void emptyPrimaryReportsFallback() throws Exception {
    prepare();
    Files.write(schemaFile().toPath(), new byte[0]);
    database = factory.open();
    assertThat(database.getSchema().existsType("A")).isTrue();
    assertThat(schema().isLoadedFromPreviousSchema()).isTrue();
  }

  @Test
  void missingPrimaryReportsFallback() throws Exception {
    prepare();
    assertThat(schemaFile().delete()).isTrue();
    database = factory.open();
    assertThat(database.getSchema().existsType("A")).isTrue();
    assertThat(schema().isLoadedFromPreviousSchema()).isTrue();
  }

  @Test
  void flagClearsOnNextCleanLoad() throws Exception {
    prepare();
    Files.writeString(schemaFile().toPath(), TRUNCATED_SCHEMA);
    database = factory.open();
    assertThat(schema().isLoadedFromPreviousSchema()).isTrue();
    // the open healed schema.json, so the next open reads the primary
    database.close();
    database = factory.open();
    assertThat(schema().isLoadedFromPreviousSchema()).isFalse();
  }

  private void prepare() {
    database.getSchema().createDocumentType("A");
    database.getSchema().createDocumentType("Marker");
    database.close();
  }

  private File schemaFile() {
    return new File(getDatabasePath() + File.separator + LocalSchema.SCHEMA_FILE_NAME);
  }

  private LocalSchema schema() {
    return ((DatabaseInternal) database).getSchema().getEmbedded();
  }
}
