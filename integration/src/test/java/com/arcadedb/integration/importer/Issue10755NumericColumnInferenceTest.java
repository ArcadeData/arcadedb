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
import com.arcadedb.database.Identifiable;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10755: the CSV and XML schema analysis declared a column numeric when Java could parse its values as a literal, so
 * {@code 10D} was stored as {@code 10.0} and the ZIP code {@code 02134} as {@code 2134}, the original text kept nowhere.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10755NumericColumnInferenceTest {

  /**
   * The census. The forms are generated from the productions of the grammars that used to decide - the
   * {@code Double.valueOf(String)} grammar (FloatTypeSuffix, HexFloatingPointLiteral, Signed NaN, Signed Infinity, and the
   * leading/trailing whitespace {@code String.trim()} strips) and {@code Long.parseLong}'s sign and leading-zero allowances - and
   * not listed by hand, which is what misses a production. Each one is first proven to be accepted by the old predicate, so the
   * census cannot pass vacuously, then a column of it must be analyzed as STRING.
   */
  @Test
  void everyJavaLiteralOnlyFormIsAString() {
    final List<String> forms = new ArrayList<>();
    final String[] decimals = { "1", "10", "1.5", "0.25", "1e3" };

    // FloatTypeSuffix
    for (final String base : decimals)
      for (final char suffix : new char[] { 'f', 'F', 'd', 'D' })
        forms.add(base + suffix);

    // HexFloatingPointLiteral: HexSignificand BinaryExponent FloatTypeSuffix?
    for (final String prefix : new String[] { "0x", "0X" })
      for (final String significand : new String[] { "1", "1.", ".8", "1.8" })
        for (final String exponent : new String[] { "p3", "P3", "p-1", "P+2" }) {
          forms.add(prefix + significand + exponent);
          forms.add("-" + prefix + significand + exponent + "d");
        }

    // Signed NaN, Signed Infinity
    for (final String sign : new String[] { "", "+", "-" }) {
      forms.add(sign + "NaN");
      forms.add(sign + "Infinity");
    }

    // leading and trailing whitespace: every character String.trim() strips, which is what Double.valueOf() applies
    for (char ws = 0; ws <= ' '; ws++)
      for (final String base : new String[] { "1", "1.5" }) {
        forms.add(ws + base);
        forms.add(base + ws);
      }

    // the optional sign: a leading '+' is accepted by both parsers
    for (final String base : decimals)
      forms.add("+" + base);

    // Long.parseLong's leading zeros, and the decimal forms of the same padding
    for (final String base : new String[] { "0", "7", "2134", "00", "0.5", "1.5" }) {
      forms.add("0" + base);
      forms.add("-0" + base);
    }
    forms.add("-0");

    // a dangling dot, which Double.valueOf() accepts as "1.0"
    forms.add("1.");
    forms.add("-1.");
    forms.add(".5");

    // values whose number is not what the text says: a magnitude a double cannot hold, an integer a double would round
    forms.add("1e400");
    forms.add("99999999999999999999");
    forms.add("-9223372036854775809");

    for (final String form : forms) {
      assertThat(javaParses(form)).as("census entry '%s' must be one the old predicate accepted", printable(form)).isTrue();
      assertThat(analyze(form)).as("a column of '%s'", printable(form)).isEqualTo(Type.STRING);
    }
  }

  @Test
  void canonicalNumbersAreStillNumeric() {
    for (final String form : new String[] { "0", "7", "-12", "42", "9223372036854775807", "-9223372036854775808" })
      assertThat(analyze(form)).as("a column of '%s'", form).isEqualTo(Type.LONG);

    for (final String form : new String[] { "1.5", "-0.25", "0.0", "-0.0", "1e5", "1E5", "1.5e-3", "-2.5E+10", "0e0" })
      assertThat(analyze(form)).as("a column of '%s'", form).isEqualTo(Type.DOUBLE);

    // a column mixing integers and decimals is DOUBLE, one mixing a padded value with numbers is STRING
    assertThat(analyze("1", "2.5", "3")).isEqualTo(Type.DOUBLE);
    assertThat(analyze("1", "02", "3")).isEqualTo(Type.STRING);
    assertThat(analyze("1.5", "10D")).isEqualTo(Type.STRING);
  }

  @Test
  void csvImportKeepsTheTextOfNonCanonicalNumbers() throws Exception {
    final File csv = writeFile("importer-10755-units.csv", """
        id,unit,zip,code,price
        1,10D,02134,abc,1.5
        2,3F,07306,def,2
        3,12D,01002,ghi,3.25
        """);
    final String databasePath = "target/databases/test-import-10755-csv";
    FileUtils.deleteRecursively(new File(databasePath));

    try {
      new Importer(("-url file://" + csv.getAbsolutePath() + " -database " + databasePath + " -forceDatabaseCreate true")
          .split(" ")).load();

      try (final Database db = new DatabaseFactory(databasePath).open()) {
        assertThat(db.getSchema().getType("Document").getProperty("id").getType()).isEqualTo(Type.LONG);
        assertThat(db.getSchema().getType("Document").getProperty("unit").getType()).isEqualTo(Type.STRING);
        assertThat(db.getSchema().getType("Document").getProperty("zip").getType()).isEqualTo(Type.STRING);
        assertThat(db.getSchema().getType("Document").getProperty("price").getType()).isEqualTo(Type.DOUBLE);

        final Map<Long, Document> byId = new HashMap<>();
        for (final Iterator<?> it = db.iterateType("Document", true); it.hasNext(); ) {
          final Document doc = ((Identifiable) it.next()).asDocument();
          byId.put(doc.getLong("id"), doc);
        }
        assertThat(byId).hasSize(3);
        assertThat(byId.get(1L).get("unit")).isEqualTo("10D");
        assertThat(byId.get(1L).get("zip")).isEqualTo("02134");
        assertThat(byId.get(2L).get("unit")).isEqualTo("3F");
        assertThat(byId.get(2L).get("zip")).isEqualTo("07306");
        assertThat(byId.get(3L).get("zip")).isEqualTo("01002");
        assertThat(byId.get(3L).get("price")).isEqualTo(3.25);
      }
    } finally {
      dropDatabase(databasePath);
      csv.delete();
      TestHelper.checkActiveDatabases();
    }
  }

  @Test
  void xmlImportKeepsTheTextOfNonCanonicalNumbers() throws Exception {
    final File xml = writeFile("importer-10755-places.xml", """
        <?xml version="1.0" encoding="UTF-8"?>
        <places>
          <place id="1" zip="02134" unit="10D"/>
          <place id="2" zip="07306" unit="3F"/>
        </places>
        """);
    final String databasePath = "target/databases/test-import-10755-xml";
    FileUtils.deleteRecursively(new File(databasePath));

    try {
      new Importer(("-url file://" + xml.getAbsolutePath() + " -database " + databasePath + " -forceDatabaseCreate true")
          .split(" ")).load();

      try (final Database db = new DatabaseFactory(databasePath).open()) {
        final List<Object> zips = new ArrayList<>();
        final List<Object> units = new ArrayList<>();
        for (final Iterator<?> it = db.iterateType("v_place", true); it.hasNext(); ) {
          final Document doc = ((Identifiable) it.next()).asDocument();
          zips.add(doc.get("zip"));
          units.add(doc.get("unit"));
        }
        assertThat(zips).containsExactlyInAnyOrder("02134", "07306");
        assertThat(units).containsExactlyInAnyOrder("10D", "3F");
      }
    } finally {
      dropDatabase(databasePath);
      xml.delete();
      TestHelper.checkActiveDatabases();
    }
  }

  private static Type analyze(final String... values) {
    final AnalyzedProperty property = new AnalyzedProperty("p", Type.STRING, 100, 0);
    for (final String value : values)
      property.setLastContent(value);
    property.endParsing();
    return property.getType();
  }

  private static boolean javaParses(final String text) {
    try {
      Long.parseLong(text);
      return true;
    } catch (final NumberFormatException e) {
      // fall through to the decimal probe, as AnalyzedProperty used to
    }
    try {
      Double.parseDouble(text);
      return true;
    } catch (final NumberFormatException e) {
      return false;
    }
  }

  private static String printable(final String text) {
    final StringBuilder b = new StringBuilder();
    for (final char c : text.toCharArray())
      if (c < ' ')
        b.append("\\u").append(String.format("%04x", (int) c));
      else
        b.append(c);
    return b.toString();
  }

  private static File writeFile(final String name, final String content) throws Exception {
    final File file = new File("target/" + name);
    Files.writeString(file.toPath(), content, StandardCharsets.UTF_8);
    return file;
  }

  private static void dropDatabase(final String databasePath) {
    final DatabaseFactory factory = new DatabaseFactory(databasePath);
    if (factory.exists())
      factory.open().drop();
    else
      FileUtils.deleteRecursively(new File(databasePath));
  }
}
