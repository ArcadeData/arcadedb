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
package com.arcadedb.index.hash;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Regression test for issue #7766: {@code HashIndex.convertKeys()} only ran {@code Type.convert()} and never
 * lowercased a {@code COLLATE CI} String component the way {@code LSMTreeIndexAbstract
 * .convertKeysToDeclaredTypes()} does, so a {@code UNIQUE_HASH}/{@code NOTUNIQUE_HASH} index with {@code COLLATE CI}
 * silently ignored the collation: the unique constraint let case-variant duplicates through, and a case-insensitive
 * query planned through the index answered fewer rows than the same query with no index at all.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7766HashIndexCollateCiTest {
  private static final String DB_PATH = "target/databases/Issue7766HashIndexCollateCiTest";

  private Database database;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  @Test
  void uniqueHashCiIndexRejectsACaseVariantDuplicate() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Product");
      database.command("sql", "CREATE PROPERTY Product.name STRING");
      database.command("sql", "CREATE INDEX ON Product (name COLLATE CI) UNIQUE_HASH");
      database.command("sql", "INSERT INTO Product SET name = 'Hello'");
    });

    assertThat(catchThrowable(() -> database.transaction(() -> database.command("sql", "INSERT INTO Product SET name = 'HELLO'"))))
        .as("a UNIQUE_HASH index with COLLATE CI must fold case before enforcing uniqueness, exactly like a UNIQUE LSM_TREE one")
        .isInstanceOf(DuplicatedKeyException.class);
  }

  @Test
  void notUniqueHashCiIndexAnswersTheSameAsNoIndexAtAll() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Indexed");
      database.command("sql", "CREATE PROPERTY Indexed.name STRING");
      database.command("sql", "CREATE INDEX ON Indexed (name COLLATE CI) NOTUNIQUE_HASH");
      database.command("sql", "INSERT INTO Indexed SET name = 'Hello'");

      // Control: no index at all on the same data shape.
      database.command("sql", "CREATE DOCUMENT TYPE Plain");
      database.command("sql", "INSERT INTO Plain SET name = 'Hello'");
    });

    database.transaction(() -> {
      final List<String> indexed = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT name FROM Indexed WHERE name.toLowerCase() = 'hello'")) {
        while (rs.hasNext())
          indexed.add(rs.next().getProperty("name"));
      }

      final List<String> unindexed = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT name FROM Plain WHERE name.toLowerCase() = 'hello'")) {
        while (rs.hasNext())
          unindexed.add(rs.next().getProperty("name"));
      }

      assertThat(indexed).as("adding the CI HASH index must not change the answer").isEqualTo(unindexed);
      assertThat(indexed).containsExactly("Hello");
    });
  }

  @Test
  void uniqueHashCiIndexSurvivesACloseAndReopen() {
    // HashIndex.toJSON() must persist "collations" the same way LSMTreeIndex.toJSON() does, otherwise the CI flag
    // read back by setMetadata(JSONObject) on reload is empty and folding silently stops after a restart.
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Product");
      database.command("sql", "CREATE PROPERTY Product.name STRING");
      database.command("sql", "CREATE INDEX ON Product (name COLLATE CI) UNIQUE_HASH");
      database.command("sql", "INSERT INTO Product SET name = 'Hello'");
    });

    database.close();
    database = new DatabaseFactory(DB_PATH).open();

    assertThat(catchThrowable(() -> database.transaction(() -> database.command("sql", "INSERT INTO Product SET name = 'HELLO'"))))
        .as("the CI collation must still be enforced after the index metadata is reloaded from disk")
        .isInstanceOf(DuplicatedKeyException.class);

    database.transaction(() -> {
      final List<String> indexed = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT name FROM Product WHERE name.toLowerCase() = 'hello'")) {
        while (rs.hasNext())
          indexed.add(rs.next().getProperty("name"));
      }
      assertThat(indexed).containsExactly("Hello");
    });
  }

  @Test
  void plainHashUniqueIndexWithNoCollationStillEnforcesExactCaseUniqueness() {
    // Control: without COLLATE CI, a HASH UNIQUE index behaves exactly as before - same spelling twice is refused,
    // different case is a different key and is accepted.
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Product");
      database.command("sql", "CREATE PROPERTY Product.name STRING");
      database.command("sql", "CREATE INDEX ON Product (name) UNIQUE_HASH");
      database.command("sql", "INSERT INTO Product SET name = 'Hello'");
    });

    assertThat(catchThrowable(() -> database.transaction(() -> database.command("sql", "INSERT INTO Product SET name = 'Hello'"))))
        .isInstanceOf(DuplicatedKeyException.class);

    database.transaction(() -> database.command("sql", "INSERT INTO Product SET name = 'HELLO'"));

    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", "SELECT FROM Product")) {
        int count = 0;
        while (rs.hasNext()) {
          rs.next();
          count++;
        }
        assertThat(count).isEqualTo(2);
      }
    });
  }
}
