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

import com.arcadedb.TestHelper;
import com.arcadedb.integration.importer.format.XMLImporterFormat;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for XMLImporterFormat to ensure properties don't carry over between records.
 * Regression test for issue #2759.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class XMLImporterFormatTest extends TestHelper {

  /**
   * Test case for issue #2759: Ensure properties from previous records don't carry over
   * to subsequent records when attributes are missing.
   * <p>
   * Given XML with records having different sets of attributes:
   * - Record 1: id="1", name="Alice", age="30"
   * - Record 2: id="2", name="Bob" (no age attribute)
   * <p>
   * Expected: Record 2 should NOT have an age property
   * Bug behavior: Record 2 incorrectly inherits age="30" from Record 1
   */
  @Test
  void noPropertyCarryoverBetweenRecords() throws Exception {
    final File xmlFile = createTempXMLFile("test-carryover.xml",
        """
        <?xml version="1.0" encoding="UTF-8"?>
        <users>
          <user id="1" name="Alice" age="30"/>
          <user id="2" name="Bob"/>
          <user id="3" name="Charlie" age="25"/>
          <user id="4" name="Diana"/>
        </users>""");

    try {
      // Import XML as vertices
      database.command("sql",
          "IMPORT DATABASE file://" + xmlFile.getAbsolutePath() + " WITH objectNestLevel=1, entityType='VERTEX'");

      // Verify all users were imported
      assertThat(database.countType("v_user", true)).isEqualTo(4);

      // Verify Record 1 has all properties
      final ResultSet rs1 = database.query("sql", "SELECT FROM v_user WHERE id = '1'");
      assertThat(rs1.hasNext()).isTrue();
      final Result user1 = rs1.next();
      assertThat(user1.<String>getProperty("id")).isEqualTo("1");
      assertThat(user1.<String>getProperty("name")).isEqualTo("Alice");
      assertThat(user1.<String>getProperty("age")).isEqualTo("30");
      assertThat(rs1.hasNext()).isFalse();
      rs1.close();

      // Verify Record 2 does NOT have age property (the bug fix)
      final ResultSet rs2 = database.query("sql", "SELECT FROM v_user WHERE id = '2'");
      assertThat(rs2.hasNext()).isTrue();
      final Result user2 = rs2.next();
      assertThat(user2.<String>getProperty("id")).isEqualTo("2");
      assertThat(user2.<String>getProperty("name")).isEqualTo("Bob");
      assertThat((boolean) user2.hasProperty("age")).isFalse(); // This is the critical assertion
      assertThat(rs2.hasNext()).isFalse();
      rs2.close();

      // Verify Record 3 has age
      final ResultSet rs3 = database.query("sql", "SELECT FROM v_user WHERE id = '3'");
      assertThat(rs3.hasNext()).isTrue();
      final Result user3 = rs3.next();
      assertThat(user3.<String>getProperty("id")).isEqualTo("3");
      assertThat(user3.<String>getProperty("name")).isEqualTo("Charlie");
      assertThat(user3.<String>getProperty("age")).isEqualTo("25");
      assertThat(rs3.hasNext()).isFalse();
      rs3.close();

      // Verify Record 4 does NOT have age property
      final ResultSet rs4 = database.query("sql", "SELECT FROM v_user WHERE id = '4'");
      assertThat(rs4.hasNext()).isTrue();
      final Result user4 = rs4.next();
      assertThat(user4.<String>getProperty("id")).isEqualTo("4");
      assertThat(user4.<String>getProperty("name")).isEqualTo("Diana");
      assertThat((boolean) user4.hasProperty("age")).isFalse(); // This is the critical assertion
      assertThat(rs4.hasNext()).isFalse();
      rs4.close();

    } finally {
      xmlFile.delete();
    }
  }

  /**
   * Security regression test: DTD processing must be disabled so an internal entity (Billion Laughs / XXE vector,
   * CWE-776/CWE-611) is never expanded into imported data.
   */
  @Test
  void dtdEntityExpansionIsDisabled() throws Exception {
    final File xmlFile = createTempXMLFile("test-xxe.xml",
        """
        <?xml version="1.0"?>
        <!DOCTYPE records [
          <!ENTITY lol "LOL-EXPANDED-SECRET">
        ]>
        <records>
          <record id="1" payload="&lol;"/>
        </records>""");

    try {
      try {
        database.command("sql",
            "IMPORT DATABASE file://" + xmlFile.getAbsolutePath() + " WITH objectNestLevel=1, entityType='VERTEX'");
      } catch (final Exception e) {
        // ACCEPTABLE: THE PARSER REJECTS THE DTD. THE GUARANTEE WE ASSERT IS THAT NO ENTITY EXPANSION HAPPENED.
      }

      if (database.getSchema().existsType("v_record")) {
        final ResultSet rs = database.query("sql", "SELECT FROM v_record");
        while (rs.hasNext()) {
          final Result r = rs.next();
          assertThat(r.<String>getProperty("payload")).doesNotContain("LOL-EXPANDED-SECRET");
        }
        rs.close();
      }
    } finally {
      xmlFile.delete();
    }
  }

  /**
   * Regression test for issue #6813: an empty or self-closing sub-element must not inherit the text value of the
   * previous sibling.
   * <p>
   * {@code lastContent} used to be cleared only when a new record started, never when a new sub-element started, so
   * {@code <city></city>} following {@code <name>Bob</name>} was imported as {@code city="Bob"}.
   * <p>
   * The file is deliberately pretty-printed: the whitespace CHARACTERS events between the elements are what issue
   * #2759 taught the CHARACTERS handler to ignore, and that behaviour must survive this fix.
   */
  @Test
  void noContentCarryoverBetweenSubElements() throws Exception {
    final File xmlFile = createTempXMLFile("test-subelement-carryover.xml",
        """
        <?xml version="1.0" encoding="UTF-8"?>
        <users>
          <user>
            <id>u1</id>
            <name>Alice</name>
            <city>Rome</city>
          </user>
          <user>
            <id>u2</id>
            <name>Bob</name>
            <city></city>
          </user>
          <user>
            <id>u3</id>
            <name>Charlie</name>
            <city/>
          </user>
          <user>
            <id>u4</id>
            <city></city>
            <name>Diana</name>
          </user>
        </users>""");

    try {
      database.command("sql",
          "IMPORT DATABASE file://" + xmlFile.getAbsolutePath() + " WITH objectNestLevel=1, entityType='VERTEX'");

      assertThat(database.countType("v_user", true)).isEqualTo(4);

      // A POPULATED SUB-ELEMENT STILL KEEPS ITS OWN TEXT, DESPITE THE FORMATTING WHITESPACE AROUND IT (ISSUE #2759)
      final Result user1 = selectUser("u1");
      assertThat(user1.<String>getProperty("name")).isEqualTo("Alice");
      assertThat(user1.<String>getProperty("city")).isEqualTo("Rome");

      // AN EMPTY SUB-ELEMENT MUST NOT INHERIT THE PREVIOUS SIBLING'S TEXT
      final Result user2 = selectUser("u2");
      assertThat(user2.<String>getProperty("name")).isEqualTo("Bob");
      assertThat(user2.<String>getProperty("city")).isNull();

      // SAME FOR A SELF-CLOSING SUB-ELEMENT
      final Result user3 = selectUser("u3");
      assertThat(user3.<String>getProperty("name")).isEqualTo("Charlie");
      assertThat(user3.<String>getProperty("city")).isNull();

      // AN EMPTY SUB-ELEMENT MUST NOT SWALLOW THE TEXT OF THE SIBLING THAT FOLLOWS IT EITHER
      final Result user4 = selectUser("u4");
      assertThat(user4.<String>getProperty("name")).isEqualTo("Diana");
      assertThat(user4.<String>getProperty("city")).isNull();

    } finally {
      xmlFile.delete();
    }
  }

  /**
   * Regression test for issue #6813, schema-analysis half: {@code analyze()} carried the same stale
   * {@code lastContent} across sibling sub-elements, so an empty {@code <score/>} sampled the value of the
   * {@code <name>} that preceded it and demoted the inferred type from LONG to STRING.
   */
  @Test
  void analyzeDoesNotCarryContentBetweenSubElements() throws Exception {
    final String xml = """
        <?xml version="1.0" encoding="UTF-8"?>
        <rows>
          <row>
            <name>Alice</name>
            <score>10</score>
          </row>
          <row>
            <name>Bob</name>
            <score/>
          </row>
        </rows>""";

    final AnalyzedSchema analyzedSchema = new AnalyzedSchema(100);
    final SourceSchema sourceSchema = new XMLImporterFormat().analyze(AnalyzedEntity.EntityType.VERTEX,
        newParser(xml), new ImporterSettings(), analyzedSchema);

    assertThat(sourceSchema).isNotNull();

    final AnalyzedEntity entity = analyzedSchema.getEntity("v_row");
    assertThat(entity).isNotNull();

    final AnalyzedProperty score = entity.getProperty("score");
    assertThat(score).isNotNull();
    // "Bob" MUST NEVER BE SAMPLED AS A VALUE OF score
    assertThat(score.getContents()).containsExactly("10");
    assertThat(score.getType()).isEqualTo(Type.LONG);

    final AnalyzedProperty name = entity.getProperty("name");
    assertThat(name).isNotNull();
    assertThat(name.getContents()).containsExactlyInAnyOrder("Alice", "Bob");
  }

  /**
   * Builds a {@link Parser} over an in-memory XML document, re-openable so {@code analyze()} can reset it.
   */
  private static Parser newParser(final String xml) throws IOException {
    final byte[] bytes = xml.getBytes(StandardCharsets.UTF_8);
    final Source source = new Source("memory", new ByteArrayInputStream(bytes), bytes.length, false, s -> {
      s.inputStream = new ByteArrayInputStream(bytes);
      return null;
    }, null);
    return new Parser(source, 0);
  }

  /**
   * Returns the single v_user vertex with the given id, failing if there is not exactly one.
   */
  private Result selectUser(final String id) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM v_user WHERE id = ?", id)) {
      assertThat(rs.hasNext()).isTrue();
      final Result user = rs.next();
      assertThat(rs.hasNext()).isFalse();
      return user;
    }
  }

  /**
   * Helper method to create a temporary XML file for testing.
   */
  private File createTempXMLFile(final String fileName, final String content) throws IOException {
    final File file = new File("target/" + fileName);
    file.getParentFile().mkdirs();
    try (final FileWriter writer = new FileWriter(file)) {
      writer.write(content);
    }
    return file;
  }

  @Override
  protected String getDatabasePath() {
    return "target/databases/test-xml-importer";
  }
}
