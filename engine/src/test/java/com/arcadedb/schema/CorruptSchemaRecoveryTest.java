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
import com.arcadedb.serializer.json.JSONObject;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #1249: a server that was killed in the middle of a schema save can be left with a corrupt
 * (truncated, non-empty) {@code schema.json}. Historically this aborted database open with an NPE/parse error. The
 * engine now keeps the previous good copy in {@code schema.prev.json}; on a corrupt primary file it must recover from
 * that backup instead of silently resetting the schema to empty.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CorruptSchemaRecoveryTest extends TestHelper {

  @Test
  void recoversFromPreviousSchemaWhenPrimaryIsCorrupt() throws Exception {
    // Create a couple of types + an index, then make a further change so that schema.prev.json is rotated out with a
    // complete, valid schema definition that already contains both types.
    database.getSchema().createDocumentType("Customer").createProperty("name", Type.STRING)
        .createIndex(Schema.INDEX_TYPE.LSM_TREE, true);
    database.getSchema().createDocumentType("Order");
    database.getSchema().getType("Order").createProperty("total", Type.DECIMAL);
    // One final schema change so that schema.prev.json (always one save behind) is rotated out containing the full
    // Customer+Order+total definition we assert on below. The very last change lives only in schema.json and is the
    // one legitimately lost when that file is the corrupt one.
    database.getSchema().createDocumentType("Marker");

    final String databasePath = database.getDatabasePath();
    database.close();

    final File schemaFile = new File(databasePath + File.separator + LocalSchema.SCHEMA_FILE_NAME);
    final File prevFile = new File(databasePath + File.separator + LocalSchema.SCHEMA_PREV_FILE_NAME);

    assertThat(schemaFile.exists()).isTrue();
    assertThat(prevFile.exists()).isTrue();
    // The backup must already carry both types so recovery is meaningful.
    final String prevContent = Files.readString(prevFile.toPath());
    assertThat(prevContent).contains("Customer").contains("Order");

    // Simulate a crash mid-save: schema.json is non-empty but truncated -> invalid JSON.
    try (final FileWriter w = new FileWriter(schemaFile)) {
      w.write("{\"schemaVersion\":42,\"settings\":{\"dateForm");
    }

    // Reopen: the engine must recover from schema.prev.json rather than resetting to an empty schema.
    database = factory.open();

    assertThat(database.getSchema().existsType("Customer")).isTrue();
    assertThat(database.getSchema().existsType("Order")).isTrue();
    assertThat(database.getSchema().getType("Customer").getProperty("name")).isNotNull();
    assertThat(database.getSchema().getType("Customer").existsProperty("name")).isTrue();
    assertThat(database.getSchema().getType("Order").existsProperty("total")).isTrue();

    // The corruption should self-heal: a fresh, valid schema.json is rewritten so the next open is clean.
    final String healed = Files.readString(schemaFile.toPath());
    assertThat(healed).contains("Customer").contains("Order");
    // And it must be parseable JSON again (no trailing truncation).
    assertThat(new JSONObject(healed).has("types")).isTrue();
  }
}
