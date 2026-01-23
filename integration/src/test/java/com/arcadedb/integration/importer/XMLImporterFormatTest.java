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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

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
  void testNoPropertyCarryoverBetweenRecords() throws IOException {
    final File xmlFile = createTempXMLFile("test-carryover.xml",
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<users>\n" +
            "  <user id=\"1\" name=\"Alice\" age=\"30\"/>\n" +
            "  <user id=\"2\" name=\"Bob\"/>\n" +
            "  <user id=\"3\" name=\"Charlie\" age=\"25\"/>\n" +
            "  <user id=\"4\" name=\"Diana\"/>\n" +
            "</users>");

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
