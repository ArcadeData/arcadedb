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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12040. A property DEFAULT that cannot be converted to the property's type is refused at DDL time. An OrientDB export
 * can carry one (a text default on an INTEGER), and the importer must leave that default alone, keep the property, and say that
 * the default was ignored - not report the property as an unknown type, which it is not.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12040OrientDBImporterUnconvertibleDefaultTest {

  private static final String DATABASE_PATH = "target/databases/issue-12040-orientdb-unconvertible-default";

  @TempDir
  Path tempDir;

  @BeforeEach
  @AfterEach
  void cleanUp() {
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
  }

  @Test
  void aDefaultThePropertyTypeCannotHoldIsIgnoredAndReportedAsSuch() throws Exception {
    final File export = tempDir.resolve("default-export.gz").toFile();

    OrientDBExportFixture.write(export, "defaults",
        """
        {"name":"Item","default-cluster-id":3,"cluster-ids":[3],"cluster-selection":"round-robin",\
        "properties":[{"name":"weight","type":"INTEGER","default-value":"heavy","collate":"default"},\
        {"name":"label","type":"STRING","default-value":"none","collate":"default"}]}""",
        "",
        """
        {"@type":"d","@rid":"#3:0","@class":"Item","@version":1,"weight":3,"label":"a"}""");

    final PrintStream originalOut = System.out;
    final ByteArrayOutputStream captured = new ByteArrayOutputStream();
    final OrientDBImporter importer = new OrientDBImporter(
        ("-i " + export.getAbsolutePath() + " -d " + DATABASE_PATH + " -o").split(" "));
    try {
      System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
      importer.run().close();
    } finally {
      System.setOut(originalOut);
    }

    assertThat(importer.isError()).isFalse();
    final String output = captured.toString(StandardCharsets.UTF_8);
    assertThat(output).contains("Ignoring the DEFAULT of 'Item.weight'").doesNotContain("Unknown type");

    try (final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH)) {
      final Database database = factory.open();
      try {
        // the property is there, without the default it could not hold
        final Property weight = database.getSchema().getType("Item").getProperty("weight");
        assertThat(weight.getDefaultValueDefinition()).isNull();

        // the one before it is untouched: a bare token is still imported as the string literal it means in OrientDB
        assertThat(database.getSchema().getType("Item").getProperty("label").getDefaultValue()).isEqualTo("none");

        assertThat(database.countType("Item", false)).isEqualTo(1);
      } finally {
        database.drop();
      }
    }
  }
}
