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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6813: in the XML importer {@code lastContent} was cleared only when a new record started, never when a new
 * sub-element started, so an empty or self-closing sub-element inherited the previous sibling's text. This is the
 * sub-element twin of #2759, which {@code XMLImporterFormatTest.noPropertyCarryoverBetweenRecords} fixed for
 * attributes only.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6813XmlEmptySubElementTest {

  @Test
  void emptyAndSelfClosingSubElementsDoNotInheritThePreviousSiblingValue() throws IOException {
    final String databasePath = "target/databases/test-import-6813-empty-subelement";
    final File xmlFile = writeXmlFile("importer-6813-empty-subelement.xml", """
        <?xml version="1.0" encoding="UTF-8"?>
        <rows>
          <row>
            <a>1</a>
            <b></b>
            <c/>
          </row>
          <row>
            <a>2</a>
            <b>two</b>
            <c/>
          </row>
        </rows>""");

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    final Database db = databaseFactory.create();
    try {
      db.command("sql", "IMPORT DATABASE file://" + xmlFile.getAbsolutePath() + " WITH objectNestLevel=1, entityType='VERTEX'");

      assertThat(db.countType("v_row", true)).isEqualTo(2);

      final Result first = selectByA(db, "1");
      assertThat(first.<String>getProperty("a")).isEqualTo("1");
      assertThat(first.<String>getProperty("b")).isEmpty();
      assertThat(first.<String>getProperty("c")).isEmpty();

      // ...AND NOT FROM THE PREVIOUS RECORD'S LAST SUB-ELEMENT EITHER
      final Result second = selectByA(db, "2");
      assertThat(second.<String>getProperty("b")).isEqualTo("two");
      assertThat(second.<String>getProperty("c")).isEmpty();
    } finally {
      db.drop();
      xmlFile.delete();
    }
    TestHelper.checkActiveDatabases();
  }

  /**
   * The flat property model keeps the last text found under a sub-element, including text carried by its own
   * descendants: clearing the state on the sub-element boundary must not change that.
   */
  @Test
  void nestedSubElementsStillContributeTheirTextToTheProperty() throws IOException {
    final String databasePath = "target/databases/test-import-6813-nested-subelement";
    final File xmlFile = writeXmlFile("importer-6813-nested-subelement.xml", """
        <?xml version="1.0" encoding="UTF-8"?>
        <rows>
          <row>
            <a><x>1</x><y>2</y></a>
            <b/>
          </row>
        </rows>""");

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    final Database db = databaseFactory.create();
    try {
      db.command("sql", "IMPORT DATABASE file://" + xmlFile.getAbsolutePath() + " WITH objectNestLevel=1, entityType='VERTEX'");

      assertThat(db.countType("v_row", true)).isEqualTo(1);

      final ResultSet rs = db.query("sql", "SELECT FROM v_row");
      final Result row = rs.next();
      rs.close();

      assertThat(row.<String>getProperty("a")).isEqualTo("2");
      assertThat(row.<String>getProperty("b")).isEmpty();
    } finally {
      db.drop();
      xmlFile.delete();
    }
    TestHelper.checkActiveDatabases();
  }

  private static Result selectByA(final Database db, final String a) {
    try (final ResultSet rs = db.query("sql", "SELECT FROM v_row WHERE a = ?", a)) {
      assertThat(rs.hasNext()).isTrue();
      return rs.next();
    }
  }

  private static File writeXmlFile(final String fileName, final String content) throws IOException {
    final File file = new File("target/" + fileName);
    file.getParentFile().mkdirs();
    try (final FileWriter writer = new FileWriter(file)) {
      writer.write(content);
    }
    return file;
  }
}
