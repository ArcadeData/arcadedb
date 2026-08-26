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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6749. The OrientDB importer used to read every JSON number with `nextDouble()`, so a LONG beyond 2^53 was
 * already rounded before the schema had a chance to narrow it, and every schemaless integer landed in ArcadeDB as a
 * DOUBLE. The number is now parsed from its raw literal and typed with the `@fieldTypes` hint the export carries.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6749OrientDBImporterNumberTypesTest {

  private static final String DATABASE_PATH = "target/databases/issue-6749-orientdb-numbers";

  @TempDir
  Path tempDir;

  @BeforeEach
  @AfterEach
  void cleanUp() {
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
  }

  @Test
  void everyNumberKeepsItsPrecisionAndItsType() throws Exception {
    final File export = tempDir.resolve("numbers-export.gz").toFile();

    OrientDBExportFixture.write(export, "numbers",
        """
        {"name":"Account","default-cluster-id":3,"cluster-ids":[3],"cluster-selection":"round-robin",\
        "properties":[{"name":"balance","type":"LONG","collate":"default"}]}""",
        "",
        """
        {"@type":"d","@rid":"#3:0","@class":"Account","@version":1,"balance":9007199254740993,"code":42,\
        "ratio":1.5,"price":1234567890123456789012.25,"amount":0.1,"huge":123456789012345678901234567890,\
        "@fieldTypes":"balance=l,ratio=f,price=c"}""");

    final OrientDBImporter importer = new OrientDBImporter(
        ("-i " + export.getAbsolutePath() + " -d " + DATABASE_PATH + " -o").split(" "));
    importer.run().close();

    assertThat(importer.isError()).isFalse();

    try (final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH)) {
      final Database database = factory.open();
      try {
        final Document account = readAccount(database);

        // DECLARED LONG: THE 53RD BIT MUST SURVIVE THE ROUND TRIP. WITH nextDouble() THIS WAS 9007199254740992.
        assertThat(account.get("balance")).isEqualTo(9007199254740993L);

        // SCHEMALESS INTEGER: AN INTEGER IN ORIENTDB, SO AN INTEGER HERE TOO - NOT A DOUBLE.
        assertThat(account.get("code")).isEqualTo(42);

        // THE `f` HINT MAKES IT A FLOAT, THE MISSING HINT LEAVES IT THE DEFAULT DOUBLE.
        assertThat(account.get("ratio")).isEqualTo(1.5f);
        assertThat(account.get("amount")).isEqualTo(0.1d);

        // THE `c` HINT KEEPS EVERY DIGIT OF A DECIMAL WIDER THAN A DOUBLE.
        assertThat(account.get("price")).isEqualTo(new BigDecimal("1234567890123456789012.25"));

        // AN INTEGRAL LITERAL WIDER THAN A LONG STILL CANNOT BE TRUNCATED, EVEN WITHOUT A HINT.
        assertThat(account.get("huge")).isEqualTo(new BigDecimal("123456789012345678901234567890"));
      } finally {
        database.drop();
      }
    }
  }

  /**
   * A number nested in an embedded document or in a collection is parsed by the same code path and must be typed the
   * same way. A collection carries no per-element hint in the export, so its entries fall back to the default
   * inference - which is still an integer rather than a double.
   */
  @Test
  void nestedAndCollectionNumbersAreTypedToo() throws Exception {
    final File export = tempDir.resolve("nested-export.gz").toFile();

    OrientDBExportFixture.write(export, "nested",
        """
        {"name":"Reading","default-cluster-id":3,"cluster-ids":[3],"cluster-selection":"round-robin"}""",
        "",
        """
        {"@type":"d","@rid":"#3:0","@class":"Reading","@version":1,"samples":[1,2,3],\
        "meta":{"ticks":9007199254740993,"@fieldTypes":"ticks=l"}}""");

    final OrientDBImporter importer = new OrientDBImporter(
        ("-i " + export.getAbsolutePath() + " -d " + DATABASE_PATH + " -o").split(" "));
    importer.run().close();

    assertThat(importer.isError()).isFalse();

    try (final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH)) {
      final Database database = factory.open();
      try {
        final Document reading = readOne(database, "Reading");

        assertThat((Iterable<Object>) reading.get("samples")).containsExactly(1, 2, 3);
        assertThat(((Map<?, ?>) reading.get("meta")).get("ticks")).isEqualTo(9007199254740993L);
      } finally {
        database.drop();
      }
    }
  }

  private static Document readAccount(final Database database) {
    return readOne(database, "Account");
  }

  private static Document readOne(final Database database, final String typeName) {
    try (final ResultSet result = database.query("sql", "select from " + typeName)) {
      assertThat(result.hasNext()).isTrue();
      return result.next().getElement().orElseThrow();
    }
  }
}
