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
package com.arcadedb.integration.exporter;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.integration.importer.OrientDBImporter;
import com.arcadedb.integration.importer.OrientDBImporterIT;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.zip.GZIPInputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JsonlExporterIT {
  private final static String DATABASE_PATH = "target/databases/performance";
  private final static String FILE          = "target/arcadedb-export.jsonl.tgz";

  private Database emptyDatabase() {
    return new DatabaseFactory(DATABASE_PATH).create();
  }

  @BeforeEach
  @AfterEach
  void beforeTests() {
    TestHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
  }

  @Test
  void exportOK() throws Exception {
    final File databaseDirectory = new File(DATABASE_PATH);

    final File file = new File(FILE);

    final URL inputFile = OrientDBImporterIT.class.getClassLoader().getResource("orientdb-export-small.gz");

    final OrientDBImporter importer = new OrientDBImporter(
        ("-i " + inputFile.getFile() + " -d " + DATABASE_PATH + " -o").split(" "));
    importer.run().close();

    assertThat(importer.isError()).isFalse();
    assertThat(databaseDirectory.exists()).isTrue();

    new Exporter(("-f " + FILE + " -d " + DATABASE_PATH + " -o -format jsonl").split(" ")).exportDatabase();

    assertThat(file.exists()).isTrue();
    assertThat(file.length() > 0).isTrue();

    int lines = 0;
    try (final BufferedReader in = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))))) {
      while (in.ready()) {
        final String line = in.readLine();
        new JSONObject(line);
        ++lines;
      }
    }

    assertThat(lines > 10).isTrue();

  }

  @Test
  void formatError() {
    assertThatThrownBy(() -> {
      emptyDatabase().close();
      new Exporter(("-f " + FILE + " -d " + DATABASE_PATH + " -o -format unknown").split(" ")).exportDatabase();
    }).isInstanceOf(ExportException.class);
  }

  @Test
  void fileCannotBeOverwrittenError() throws Exception {
    assertThatThrownBy(() -> {
      emptyDatabase().close();
      new File(FILE).createNewFile();
      new Exporter(("-f " + FILE + " -d " + DATABASE_PATH + " -format jsonl").split(" ")).exportDatabase();
    }).isInstanceOf(ExportException.class);
  }

  /**
   * Test for issue #1540: unique field is missing from the exported JSONL
   * <p>
   * When exporting a database to JSONL format, the schema indexes should include the "unique" field.
   */
  @Test
  void testExportedIndexesContainUniqueField() throws Exception {
    final File file = new File(FILE);

    // Create a database with indexes (both unique and non-unique)
    try (final Database db = new DatabaseFactory(DATABASE_PATH).create()) {
      db.transaction(() -> {
        final DocumentType type = db.getSchema().createDocumentType("Person");
        type.createProperty("name", String.class);
        type.createProperty("age", Integer.class);
        type.createProperty("email", String.class);

        // Create a non-unique index on age
        type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "age");

        // Create a unique index on email
        type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "email");
      });
    }

    // Export the database
    new Exporter(("-f " + FILE + " -d " + DATABASE_PATH + " -o -format jsonl").split(" ")).exportDatabase();

    assertThat(file.exists()).isTrue();

    // Read the exported file and verify the schema line contains "unique" field for indexes
    JSONObject schemaLine = null;
    try (final BufferedReader in = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))))) {
      while (in.ready()) {
        final String line = in.readLine();
        final JSONObject json = new JSONObject(line);
        if ("schema".equals(json.getString("t"))) {
          schemaLine = json.getJSONObject("c");
          break;
        }
      }
    }

    assertThat(schemaLine).as("Schema line not found in export").isNotNull();

    // Navigate to the Person type indexes
    final JSONObject types = schemaLine.getJSONObject("types");
    assertThat(types.has("Person")).as("Person type should exist in exported schema").isTrue();

    final JSONObject personType = types.getJSONObject("Person");
    final JSONObject indexes = personType.getJSONObject("indexes");
    assertThat(indexes.length()).as("Person type should have 2 indexes exported").isGreaterThanOrEqualTo(2);

    // Verify that each index has the "unique" field
    boolean foundNonUniqueIndex = false;
    boolean foundUniqueIndex = false;

    for (final String indexName : indexes.keySet()) {
      final JSONObject indexJson = indexes.getJSONObject(indexName);

      // Verify the "unique" field exists (issue #1540)
      assertThat(indexJson.has("unique"))
          .as("Index '%s' should have 'unique' field in exported JSONL (issue #1540)", indexName)
          .isTrue();

      // Also verify the value is correctly exported
      final boolean isUnique = indexJson.getBoolean("unique");
      final String properties = indexJson.getJSONArray("properties").toString();

      if (properties.contains("age")) {
        assertThat(isUnique).as("Index on 'age' should be non-unique").isFalse();
        foundNonUniqueIndex = true;
      } else if (properties.contains("email")) {
        assertThat(isUnique).as("Index on 'email' should be unique").isTrue();
        foundUniqueIndex = true;
      }
    }

    assertThat(foundNonUniqueIndex).as("Should find the non-unique index on age").isTrue();
    assertThat(foundUniqueIndex).as("Should find the unique index on email").isTrue();
  }

}
