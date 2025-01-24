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
package com.arcadedb.integration.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URL;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class OrientDBImporterIT {

  private final static String DATABASE_PATH = "target/databases/oriendb-imported";

  @BeforeEach
  @AfterEach
  void cleanUp() {
    TestHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));

  }

  @Test
  public void testImportOK() throws Exception {
    final File databaseDirectory = new File(DATABASE_PATH);

    final URL inputFile = OrientDBImporterIT.class.getClassLoader().getResource("orientdb-export-small.gz");

    final OrientDBImporter importer = new OrientDBImporter(
        ("-i " + inputFile.getFile() + " -d " + DATABASE_PATH + " -s -o").split(" "));
    importer.run().close();

    assertThat(importer.isError()).isFalse();
    assertThat(databaseDirectory.exists()).isTrue();

    try (final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH)) {
      try (final Database database = factory.open()) {
        final DocumentType personType = database.getSchema().getType("Person");
        assertThat(personType).isNotNull();
        assertThat(personType.getProperty("id").getType()).isEqualTo(Type.INTEGER);
        assertThat(database.countType("Person", true)).isEqualTo(500);
        assertThat(database.getSchema().getIndexByName("Person[id]").getType()).isEqualTo(Schema.INDEX_TYPE.LSM_TREE);

        final DocumentType friendType = database.getSchema().getType("Friend");
        assertThat(friendType).isNotNull();
        assertThat(friendType.getProperty("id").getType()).isEqualTo(Type.INTEGER);
        assertThat(database.countType("Friend", true)).isEqualTo(10_000);
        assertThat(database.getSchema().getIndexByName("Friend[id]").getType()).isEqualTo(Schema.INDEX_TYPE.LSM_TREE);

        final File securityFile = new File("./server-users.jsonl");
        assertThat(securityFile.exists()).isTrue();

        final String fileContent = FileUtils.readFileAsString(securityFile);
        final JSONObject security = new JSONObject(fileContent.substring(0, fileContent.indexOf("\n")));
        assertThat(security.getString("name")).isEqualTo("admin");
      }
    }
  }

  @Test
  public void testImportNoFile() throws Exception {
    final URL inputFile = OrientDBImporterIT.class.getClassLoader().getResource("orientdb-export-small.gz");
    final OrientDBImporter importer = new OrientDBImporter(
        ("-i " + inputFile.getFile() + "2 -d " + DATABASE_PATH + " -s -o").split(" "));

    assertThatThrownBy(() -> importer.run())
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(importer.isError()).isTrue();
  }
}
