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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12044: the XML importer kept the text of the last character event of an element, and its parser was not coalescing,
 * so the text was split at every entity or character reference and around a CDATA section: {@code Al &amp; Bo &lt;3} was
 * stored as {@code 3}, {@code Montr&#233;al} as {@code al}, with result OK. The same parser also split a long plain text
 * node, with no entity at all, into several events (about 16K characters each), so the tail was all that was stored.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12044XmlEntityTextTest {

  @Test
  void elementTextWithReferencesAndCdataIsStoredWhole() throws Exception {
    final String databasePath = "target/databases/test-import-12044";
    final File xmlFile = new File("target/importer-12044.xml");
    Files.writeString(xmlFile.toPath(), """
        <?xml version="1.0" encoding="UTF-8"?>
        <items>
          <item id="1" title="x &amp; y"><name>Al &amp; Bo &lt;3</name><city>Montr&#233;al</city><note><![CDATA[x<y]]> tail</note><plain>no entities</plain></item>
          <item id="2"><name>caf&#xE9; &quot;quoted&quot;</name><city>Z&#252;rich</city><note>a <![CDATA[<b>]]> c</note><plain>two</plain></item>
        </items>""", StandardCharsets.UTF_8);

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    final Database db = databaseFactory.create();
    try {
      db.command("sql", "IMPORT DATABASE file://" + xmlFile.getAbsolutePath());
      assertThat(db.countType("v_item", true)).isEqualTo(2);

      final Result first = selectById(db, "1");
      assertThat(first.<String>getProperty("title")).isEqualTo("x & y");
      assertThat(first.<String>getProperty("name")).isEqualTo("Al & Bo <3");
      assertThat(first.<String>getProperty("city")).isEqualTo("Montréal");
      assertThat(first.<String>getProperty("note")).isEqualTo("x<y tail");
      assertThat(first.<String>getProperty("plain")).isEqualTo("no entities");

      final Result second = selectById(db, "2");
      assertThat(second.<String>getProperty("name")).isEqualTo("café \"quoted\"");
      assertThat(second.<String>getProperty("city")).isEqualTo("Zürich");
      assertThat(second.<String>getProperty("note")).isEqualTo("a <b> c");
      assertThat(second.<String>getProperty("plain")).isEqualTo("two");
    } finally {
      db.drop();
      xmlFile.delete();
    }
    TestHelper.checkActiveDatabases();
  }

  /**
   * The parser also cuts a long text node that holds no entity or CDATA at all: one element of 30000 characters used to be
   * stored as its last 13672. A reference in the middle of a long text splits it at both ends.
   */
  @Test
  void longElementTextIsStoredWhole() throws Exception {
    final String databasePath = "target/databases/test-import-12044-long-text";
    final File xmlFile = new File("target/importer-12044-long-text.xml");
    final String chunk = "0123456789abcdefghij";
    final String longText = chunk.repeat(1500);
    final String longTextWithReference = chunk.repeat(1000) + "&amp;" + chunk.repeat(1000);
    Files.writeString(xmlFile.toPath(), "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<items>\n  <item id=\"1\"><body>" + longText
        + "</body><tail>" + longTextWithReference + "</tail></item>\n</items>", StandardCharsets.UTF_8);

    final DatabaseFactory databaseFactory = new DatabaseFactory(databasePath);
    if (databaseFactory.exists())
      databaseFactory.open().drop();

    final Database db = databaseFactory.create();
    try {
      db.command("sql", "IMPORT DATABASE file://" + xmlFile.getAbsolutePath());
      assertThat(db.countType("v_item", true)).isEqualTo(1);

      final Result item = selectById(db, "1");
      assertThat(item.<String>getProperty("body")).isEqualTo(longText);
      assertThat(item.<String>getProperty("tail")).isEqualTo(chunk.repeat(1000) + "&" + chunk.repeat(1000));
    } finally {
      db.drop();
      xmlFile.delete();
    }
    TestHelper.checkActiveDatabases();
  }

  private static Result selectById(final Database db, final String id) {
    try (final ResultSet rs = db.query("sql", "SELECT FROM v_item WHERE id = ?", id)) {
      return rs.next();
    }
  }
}
