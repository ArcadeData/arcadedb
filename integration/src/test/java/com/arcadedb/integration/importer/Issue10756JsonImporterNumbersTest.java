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
import com.arcadedb.integration.TestHelper;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10756: the JSON importer read every number token with {@code nextDouble()}, so {@code 42} was stored as {@code 42.0}
 * and {@code 9007199254740993} as {@code 9007199254740992}, while {@link JSONObject} - the reader of the same token for the
 * HTTP API - kept both. The importer now calls that decoder, so the two cannot disagree: the corpus below is fed to both,
 * as an object property and as an array entry, and every value must come back identical, type included.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10756JsonImporterNumbersTest {
  /** One token per arm of {@link JSONObject#parseNumber(String)}. */
  private static final String[] CORPUS = {
      "42",                                     // Integer
      "-0",                                     // Integer zero
      "-2147483648",                            // Integer.MIN_VALUE
      "2147483648",                             // just past Integer: Long
      "9007199254740993",                       // past 2^53: a double would round it
      "-9223372036854775807",                   // a long a double would round
      "-9223372036854775808",                   // Long.MIN_VALUE
      "18446744073709551617",                   // past the long range: BigDecimal
      "123456789012345678901234567890",         // far past it
      "1.5",                                    // Double
      "1.50",                                   // trailing zero: still the double 1.5
      "0.1",                                    // the double that prints shortest
      "1.234567890123456789",                   // more digits than a double: BigDecimal
      "1e309",                                  // overflows a double: BigDecimal
      "1e-400",                                 // underflows a double: BigDecimal
      "0.0",                                    // zero is exact
      "-1.5E10",                                // exponent form
  };

  @Test
  void theImporterDecodesEveryTokenAsTheJsonReaderDoes() throws Exception {
    final StringBuilder json = new StringBuilder("[{\"values\":[");
    for (int i = 0; i < CORPUS.length; i++) {
      if (i > 0)
        json.append(',');
      json.append(CORPUS[i]);
    }
    json.append(']');
    for (int i = 0; i < CORPUS.length; i++)
      json.append(",\"v").append(i).append("\":").append(CORPUS[i]);
    json.append("}]");

    final String databasePath = importSource("importer-10756-corpus.json", json.toString(), "");
    try (final Database db = new DatabaseFactory(databasePath).open()) {
      final Document doc = db.iterateType("Document", true).next().asDocument();

      final List<Object> expected = new ArrayList<>();
      for (int i = 0; i < CORPUS.length; i++) {
        final Object reference = new JSONObject("{\"v\":" + CORPUS[i] + "}").get("v");
        expected.add(reference);

        final Object imported = doc.get("v" + i);
        assertThat(imported).as(CORPUS[i]).isEqualTo(reference);
        assertThat(imported.getClass()).as(CORPUS[i]).isEqualTo(reference.getClass());
      }

      final List<?> values = (List<?>) doc.get("values");
      assertThat(values).isEqualTo(expected);
      for (int i = 0; i < CORPUS.length; i++)
        assertThat(values.get(i).getClass()).as(CORPUS[i]).isEqualTo(expected.get(i).getClass());

      // THE VALUES OF THE ISSUE'S REPRO, SPELLED OUT
      assertThat(doc.get("v0")).isEqualTo(42);
      assertThat(doc.get("v4")).isEqualTo(9007199254740993L);
      assertThat(doc.get("v5")).isEqualTo(-9223372036854775807L);
      assertThat(doc.get("v8")).isEqualTo(new BigDecimal("123456789012345678901234567890"));
      assertThat(doc.get("v12")).isEqualTo(new BigDecimal("1.234567890123456789"));
    } finally {
      dropDatabase(databasePath);
    }
  }

  /** A token only a hostile payload writes is refused like the HTTP API refuses it, not expanded into a huge BigDecimal. */
  @Test
  void aHostileNumberTokenIsRefusedAsARowError() throws Exception {
    final String databasePath = importSource("importer-10756-hostile.json",
        "[{\"id\":1,\"bomb\":1e999999999},{\"id\":2,\"ok\":3}]", " -onRowError skip");
    try (final Database db = new DatabaseFactory(databasePath).open()) {
      assertThat(db.countType("Document", true)).isEqualTo(1);
      assertThat(db.iterateType("Document", true).next().asDocument().get("id")).isEqualTo(2);
    } finally {
      dropDatabase(databasePath);
    }
  }

  /**
   * An {@code @id} declared from the first record's integer used to be INTEGER, so a later id past 2^31 was refused. An
   * integral id column is a LONG one.
   */
  @Test
  void anIntegralIdColumnIsDeclaredLong() throws Exception {
    final String source = "{\"Items\":[{\"code\":1,\"name\":\"a\"},{\"code\":3000000000,\"name\":\"b\"},"
        + "{\"code\":9007199254740993,\"name\":\"c\"},{\"code\":9007199254740992,\"name\":\"d\"}]}";
    final String databasePath = importSource("importer-10756-intid.json", source,
        " -mapping {\"Items\":[{\"@cat\":\"v\",\"@type\":\"Item\",\"@id\":\"code\"}]}");
    try (final Database db = new DatabaseFactory(databasePath).open()) {
      assertThat(db.getSchema().getType("Item").getProperty("code").getType()).isEqualTo(Type.LONG);
      assertThat(db.countType("Item", true)).isEqualTo(4);
      assertThat(db.query("sql", "select name from Item where code = 9007199254740993").next().<String>getProperty("name"))
          .isEqualTo("c");
    } finally {
      dropDatabase(databasePath);
    }
  }

  /**
   * An {@code @id} declared from a first decimal id used to be DOUBLE, so two later ids that differ past the 17th digit
   * were one key of the unique index, and the second record was silently skipped as already imported.
   */
  @Test
  void aDecimalIdColumnIsDeclaredDecimalSoNoTwoIdsCollide() throws Exception {
    final String source = "{\"Items\":[{\"code\":0.5,\"name\":\"a\"},{\"code\":1.2345678901234567891,\"name\":\"b\"},"
        + "{\"code\":1.2345678901234567892,\"name\":\"c\"}]}";
    final String databasePath = importSource("importer-10756-decid.json", source,
        " -mapping {\"Items\":[{\"@cat\":\"v\",\"@type\":\"Item\",\"@id\":\"code\"}]}");
    try (final Database db = new DatabaseFactory(databasePath).open()) {
      assertThat(db.getSchema().getType("Item").getProperty("code").getType()).isEqualTo(Type.DECIMAL);
      assertThat(db.countType("Item", true)).isEqualTo(3);
    } finally {
      dropDatabase(databasePath);
    }
  }

  private static String importSource(final String fileName, final String content, final String extraArgs) throws Exception {
    final File source = new File("target/" + fileName);
    Files.writeString(source.toPath(), content, StandardCharsets.UTF_8);
    final String databasePath = "target/databases/test-import-10756-" + fileName.replace('.', '-');
    FileUtils.deleteRecursively(new File(databasePath));
    try {
      new Importer(("-url file://" + source.getAbsolutePath() + " -database " + databasePath + " -forceDatabaseCreate true"
          + extraArgs).split(" ")).load();
    } finally {
      source.delete();
      TestHelper.checkActiveDatabases();
    }
    return databasePath;
  }

  private static void dropDatabase(final String databasePath) {
    final DatabaseFactory factory = new DatabaseFactory(databasePath);
    if (factory.exists())
      factory.open().drop();
    else
      FileUtils.deleteRecursively(new File(databasePath));
  }
}
