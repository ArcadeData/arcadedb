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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Test for GitHub issue #3417: Cypher round(n, precision) doesn't support precision.
 */
class Issue3417Test {
  private Database database;
  private static final String DB_PATH = "./target/databases/test-issue3417";

  @BeforeEach
  void setUp() {
    new File(DB_PATH).mkdirs();
    database = new DatabaseFactory(DB_PATH).create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void testRoundWithPrecision() {
    // Exact query from the issue
    try (final ResultSet rs = database.command("opencypher", "RETURN round(3.141592, 2) as result")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Double) row.getProperty("result")).isCloseTo(3.14, within(0.0001));
    }
  }

  @Test
  void testRoundWithoutPrecision() {
    try (final ResultSet rs = database.command("opencypher", "RETURN round(3.141592) as result")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      // round(3.141592) should return 3.0
      assertThat((Double) row.getProperty("result")).isCloseTo(3.0, within(0.0001));
    }
  }

  @Test
  void testRoundWithZeroPrecision() {
    try (final ResultSet rs = database.command("opencypher", "RETURN round(3.7, 0) as result")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Double) row.getProperty("result")).isCloseTo(4.0, within(0.0001));
    }
  }

  @Test
  void testRoundWithHighPrecision() {
    try (final ResultSet rs = database.command("opencypher", "RETURN round(3.141592, 4) as result")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Double) row.getProperty("result")).isCloseTo(3.1416, within(0.00001));
    }
  }

  @Test
  void testRoundNegativeNumber() {
    try (final ResultSet rs = database.command("opencypher", "RETURN round(-2.555, 2) as result")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Double) row.getProperty("result")).isCloseTo(-2.56, within(0.0001));
    }
  }

  @Test
  void testRoundNull() {
    try (final ResultSet rs = database.command("opencypher", "RETURN round(null) as result")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat((Object) row.getProperty("result")).isNull();
    }
  }
}
