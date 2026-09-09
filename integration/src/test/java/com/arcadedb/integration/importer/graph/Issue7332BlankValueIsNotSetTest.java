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
package com.arcadedb.integration.importer.graph;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7332, item 1: #7269 aligned five JSONL accessors on "an empty value means not set" and left the sixth -
 * {@code get()}, which is the accessor a STRING property routes through.
 * <p>
 * So the same blank column behaved differently by source format: nothing was stored when the row came from CSV, an
 * empty string when it came from JSONL or XML. A downstream {@code IS NULL} filter, or a mandatory-property check,
 * then answered by which file the row was exported to rather than by what the row said - and the three files below
 * carry exactly the same data.
 * <p>
 * The rule now lives on {@link GraphImporter.RecordReader#get} with a helper to satisfy it, rather than in three
 * copies that drifted apart once already.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7332BlankValueIsNotSetTest {

  private static final String DB_PATH  = "target/databases/issue-7332-blank-value";
  private static final String BASE_DIR = "target/issue-7332-sources";

  private Database database;

  @BeforeEach
  void setUp() throws Exception {
    FileUtils.deleteRecursively(new File(DB_PATH));
    new File(BASE_DIR).mkdirs();
    database = new DatabaseFactory(DB_PATH).create();
    database.transaction(() -> database.getSchema().createVertexType("Person").createProperty("nickname", Type.STRING));

    Files.writeString(new File(BASE_DIR, "people.csv").toPath(), """
        id,name,nickname
        1,Alice,Ali
        2,Bob,
        """, StandardCharsets.UTF_8);

    Files.writeString(new File(BASE_DIR, "people.jsonl").toPath(), """
        {"id":"1","name":"Alice","nickname":"Ali"}
        {"id":"2","name":"Bob","nickname":""}
        """, StandardCharsets.UTF_8);

    Files.writeString(new File(BASE_DIR, "people.xml").toPath(), """
        <people>
          <row id="1" name="Alice" nickname="Ali"/>
          <row id="2" name="Bob" nickname=""/>
        </people>
        """, StandardCharsets.UTF_8);
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      if (database.isTransactionActive())
        database.rollbackAllNested();
      if (database.isOpen())
        database.drop();
      database = null;
    }
    FileUtils.deleteRecursively(new File(DB_PATH));
    FileUtils.deleteRecursively(new File(BASE_DIR));
  }

  /** The reader level, where the divergence was: an empty value is "not set" whatever the file format is. */
  @Test
  void everySourceReadsABlankValueAsNotSet() throws Exception {
    assertThat(nicknamesOf(CsvRowSource.from(BASE_DIR, "people.csv"))).containsExactly("Ali", null);
    assertThat(nicknamesOf(JsonlRowSource.from(BASE_DIR, "people.jsonl")))
        .as("the accessor a STRING property routes through, which #7269 left returning \"\"")
        .containsExactly("Ali", null);
    assertThat(nicknamesOf(new XmlRowSource(new File(BASE_DIR, "people.xml").getPath(), "row", false)))
        .containsExactly("Ali", null);
  }

  /** A value that is present but only whitespace is a data error, not a blank cell, on every source alike. */
  @Test
  void aWhitespaceOnlyValueIsStillAValue() throws Exception {
    Files.writeString(new File(BASE_DIR, "spaces.jsonl").toPath(), "{\"id\":\"1\",\"nickname\":\" \"}\n",
        StandardCharsets.UTF_8);
    assertThat(nicknamesOf(JsonlRowSource.from(BASE_DIR, "spaces.jsonl"))).containsExactly(" ");

    Files.writeString(new File(BASE_DIR, "spaces.csv").toPath(), "id,nickname\n1, \n", StandardCharsets.UTF_8);
    assertThat(nicknamesOf(CsvRowSource.from(BASE_DIR, "spaces.csv"))).containsExactly(" ");
  }

  /**
   * The child-element form of the same rule. {@code XmlRowSource} trims child text, for pretty-printed XML whose
   * element text carries the surrounding newline and indentation - but trimming a value that is ONLY whitespace
   * down to {@code ""} would hand it to {@code emptyAsNull} and lose it, while an empty {@code <tag/>} really is
   * "not set". Both halves are asserted here, because a fix for either one alone breaks the other.
   */
  @Test
  void anXmlChildElementKeepsItsIndentationTrimmedAndItsWhitespaceValue() throws Exception {
    Files.writeString(new File(BASE_DIR, "children.xml").toPath(), """
        <people>
          <row id="1">
            <nickname>
              Ali
            </nickname>
          </row>
          <row id="2">
            <nickname> </nickname>
          </row>
          <row id="3">
            <nickname></nickname>
          </row>
        </people>
        """, StandardCharsets.UTF_8);

    assertThat(nicknamesOf(new XmlRowSource(new File(BASE_DIR, "children.xml").getPath(), "row", true)))
        .as("indentation is trimmed off a real value, a whitespace-only value survives, an empty element is unset")
        .containsExactly("Ali", " ", null);
  }

  /** The consequence the issue is about: the imported records agree, whichever file they came from. */
  @Test
  void theImportedPropertyIsAbsentWhateverTheSourceFormatWas() throws Exception {
    importPeople("PersonCsv", CsvRowSource.from(BASE_DIR, "people.csv"));
    importPeople("PersonJsonl", JsonlRowSource.from(BASE_DIR, "people.jsonl"));
    importPeople("PersonXml", new XmlRowSource(new File(BASE_DIR, "people.xml").getPath(), "row", false));

    for (final String type : new String[] { "PersonCsv", "PersonJsonl", "PersonXml" }) {
      assertThat(database.countType(type, true)).as(type + " imported both rows").isEqualTo(2);
      assertThat(nicknameOf(type, "1")).as(type + " keeps the value that is there").isEqualTo("Ali");
      assertThat(hasNickname(type, "2"))
          .as(type + ": a blank column stores nothing, so IS NULL answers the same for all three files")
          .isFalse();
    }
  }

  private void importPeople(final String typeName, final GraphImporter.RecordSource source) throws Exception {
    database.transaction(() -> database.getSchema().createVertexType(typeName));
    try (final GraphImporter importer = GraphImporter.builder(database)
        .vertex(typeName, source, v -> {
          v.id("id");
          v.property("id", "id");
          v.property("name", "name");
          v.property("nickname", "nickname");
        })
        .build()) {
      importer.run();
    }
  }

  private String nicknameOf(final String typeName, final String id) {
    final Document doc = documentOf(typeName, id);
    return doc == null ? null : doc.getString("nickname");
  }

  private boolean hasNickname(final String typeName, final String id) {
    final Document doc = documentOf(typeName, id);
    return doc != null && doc.getPropertyNames().contains("nickname");
  }

  private Document documentOf(final String typeName, final String id) {
    return database.query("sql", "select from " + typeName).stream()
        .map(r -> r.toElement().asDocument(true))
        .filter(d -> id.equals(d.getString("id")))
        .findFirst().orElse(null);
  }

  private static List<String> nicknamesOf(final GraphImporter.RecordSource source) throws Exception {
    final List<String> values = new ArrayList<>();
    source.forEach(record -> values.add(record.get("nickname")));
    return values;
  }
}
