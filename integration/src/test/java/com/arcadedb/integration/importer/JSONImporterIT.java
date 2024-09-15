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
import com.arcadedb.database.Document;
import com.arcadedb.database.Record;
import com.arcadedb.graph.Vertex;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class JSONImporterIT {
  @Test
  public void importSingleObject() throws IOException {
    final String databasePath = "target/databases/test-import-graph";

    Importer importer = new Importer(
        ("-url file://src/test/resources/importer-one-object.json -database " + databasePath
            + " -documentType Food -forceDatabaseCreate true").split(" "));
    importer.load();

    try (final Database db = new DatabaseFactory(databasePath).open()) {
      assertThat(db.countType("Food", true)).isEqualTo(1);

      final Document food = db.iterateType("Food", true).next().asDocument(true);
      JSONObject json = new JSONObject(FileUtils.readFileAsString(new File("src/test/resources/importer-one-object.json")));

      for (Object name : json.names())
        assertThat(food.has(name.toString())).isTrue();
    }

    TestHelper.checkActiveDatabases();
  }

  @Test
  public void importTwoObjects() throws IOException {
    final String databasePath = "target/databases/test-import-graph";

    Importer importer = new Importer(("-url file://src/test/resources/importer-two-objects.json -database " + databasePath
        + " -documentType Food -forceDatabaseCreate true -mapping {'*':[]}").split(" "));
    importer.load();

    try (final Database db = new DatabaseFactory(databasePath).open()) {
      assertThat(db.countType("Food", true)).isEqualTo(2);
    }

    TestHelper.checkActiveDatabases();
  }

  @Test
  public void importEmployees() throws IOException {
    final String databasePath = "target/databases/test-import-graph";

    final String mapping = "{" + //
        "  \"Users\":[" + //
        "    {" + //
        "      \"@cat\":\"v\"," + //
        "      \"@type\":\"User\"," + //
        "      \"@id\":\"id\"," + //
        "      \"id\":\"<EmployeeID>\"," + //
        "      \"@idType\":\"string\"," + //
        "      \"@strategy\": \"merge\"," + //
        "      \"EmployeeID\": \"@ignore\"," + //
        "      \"ManagerID\":{" + //
        "        \"@cat\":\"e\"," + //
        "        \"@type\":\"HAS_MANAGER\"," + //
        "        \"@cardinality\":\"no-duplicates\"," + //
        "        \"@in\": {" + //
        "          \"@cat\":\"v\"," + //
        "          \"@type\":\"User\"," + //
        "          \"@id\":\"id\"," + //
        "          \"@idType\": \"string\"," + //
        "          \"@strategy\": \"merge\"," + //
        "          \"EmployeeID\": \"@ignore\"," + //
        "          \"id\":\"<../ManagerID>\"" + //
        "        }     " + //
        "      }" + //
        "    }" + //
        "  ]" + //
        "}";

    Importer importer = new Importer(
        new String[] { "-url", "file://src/test/resources/importer-employees.json", "-database", databasePath,
            "-forceDatabaseCreate", "true", "-mapping",
            mapping });
    importer.load();

    try (final Database db = new DatabaseFactory(databasePath).open()) {
      for (Iterator<Record> it = db.iterateType("User", true); it.hasNext(); ) {
        final Vertex vertex = it.next().asVertex();

        final String name = vertex.getString("Name");

        if ("Marcus".equalsIgnoreCase(name)) {
          assertThat(vertex.getString("id")).isEqualTo("1234");
          assertThat(vertex.countEdges(Vertex.DIRECTION.OUT, "HAS_MANAGER")).isEqualTo(0);
          assertThat(vertex.countEdges(Vertex.DIRECTION.IN, "HAS_MANAGER")).isEqualTo(2);
        } else if ("Win".equals(name)) {
          assertThat(vertex.getString("id")).isEqualTo("1230");
          assertThat(vertex.countEdges(Vertex.DIRECTION.OUT, "HAS_MANAGER")).isEqualTo(1);
          assertThat(vertex.countEdges(Vertex.DIRECTION.IN, "HAS_MANAGER")).isEqualTo(0);
        } else if ("Dave".equals(name)) {
          assertThat(vertex.getString("id")).isEqualTo("1232");
          assertThat(vertex.countEdges(Vertex.DIRECTION.OUT, "HAS_MANAGER")).isEqualTo(1);
          assertThat(vertex.countEdges(Vertex.DIRECTION.IN, "HAS_MANAGER")).isEqualTo(1);
        } else if ("Albert".equals(name)) {
          assertThat(vertex.getString("id")).isEqualTo("1239");
          assertThat(vertex.countEdges(Vertex.DIRECTION.OUT, "HAS_MANAGER")).isEqualTo(1);
          assertThat(vertex.countEdges(Vertex.DIRECTION.IN, "HAS_MANAGER")).isEqualTo(0);
        } else
          fail("");
      }

      assertThat(db.countType("User", true)).isEqualTo(4);
    }

    TestHelper.checkActiveDatabases();
  }
}
