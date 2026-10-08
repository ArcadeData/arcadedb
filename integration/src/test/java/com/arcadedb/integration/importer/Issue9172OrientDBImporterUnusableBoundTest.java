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
import com.arcadedb.schema.Property;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9172 (#9026). A MIN or MAX the property's type cannot use is refused since that issue. An OrientDB export can
 * carry one (a bound on a BOOLEAN), and the importer must skip that bound alone, keeping the property and its other
 * attributes, rather than abandoning the rest of the property definition.
 */
class Issue9172OrientDBImporterUnusableBoundTest {

  private static final String DATABASE_PATH = "target/databases/issue-9172-orientdb-unusable-bound";

  @TempDir
  Path tempDir;

  @BeforeEach
  @AfterEach
  void cleanUp() {
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
  }

  @Test
  void anUnusableBoundIsSkippedAndTheRestOfThePropertyIsImported() throws Exception {
    final File export = tempDir.resolve("bound-export.gz").toFile();

    OrientDBExportFixture.write(export, "bounds",
        """
        {"name":"Flag","default-cluster-id":3,"cluster-ids":[3],"cluster-selection":"round-robin",\
        "properties":[{"name":"active","type":"BOOLEAN","min":"0","max":"1","regexp":"true|false","collate":"default"},\
        {"name":"label","type":"STRING","min":"1","max":"10","collate":"default"}]}""",
        "",
        """
        {"@type":"d","@rid":"#3:0","@class":"Flag","@version":1,"active":true,"label":"on"}""");

    final OrientDBImporter importer = new OrientDBImporter(
        ("-i " + export.getAbsolutePath() + " -d " + DATABASE_PATH + " -o").split(" "));
    importer.run().close();

    assertThat(importer.isError()).isFalse();

    try (final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH)) {
      final Database database = factory.open();
      try {
        final Property active = database.getSchema().getType("Flag").getProperty("active");
        assertThat(active.getMin()).isNull();
        assertThat(active.getMax()).isNull();
        // the attribute after the refused bounds is still imported
        assertThat(active.getRegexp()).isEqualTo("true|false");

        final Property label = database.getSchema().getType("Flag").getProperty("label");
        assertThat(label.getMin()).isEqualTo("1");
        assertThat(label.getMax()).isEqualTo("10");

        assertThat(database.countType("Flag", false)).isEqualTo(1);
      } finally {
        database.drop();
      }
    }
  }
}
