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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9033: a FLOAT property holding 0f and -0f under a NOTUNIQUE LSM index made DELETE FROM fail at commit with a
 * NegativeArraySizeException from LSMTreeIndexAbstract.readEntryValues, because the page held the two spellings of one key.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9033FloatSignedZeroDeleteTest extends TestHelper {

  @Test
  void deleteFloatZeroAndNegativeZero() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE FI").close();
      database.command("sql", "CREATE PROPERTY FI.x FLOAT").close();
      database.command("sql", "CREATE INDEX ON FI (x) NOTUNIQUE").close();
    });
    database.transaction(() -> {
      database.newDocument("FI").set("x", 0f).save();
      database.newDocument("FI").set("x", -0f).save();
    });
    database.transaction(() -> database.command("sql", "DELETE FROM FI").close());
    assertThat(count("FI")).isZero();
  }

  @Test
  void lookupAndDeleteBothSpellings() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE FJ").close();
      database.command("sql", "CREATE PROPERTY FJ.x FLOAT").close();
      database.command("sql", "CREATE INDEX ON FJ (x) NOTUNIQUE").close();
    });
    database.transaction(() -> {
      database.newDocument("FJ").set("x", 0f).save();
      database.newDocument("FJ").set("x", -0f).save();
      database.newDocument("FJ").set("x", 1.5f).save();
    });
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM FJ WHERE x = 0")) {
      assertThat(rs.next().<Number>getProperty("n").longValue()).isEqualTo(2L);
    }
    database.transaction(() -> database.command("sql", "DELETE FROM FJ WHERE x = -0.0").close());
    assertThat(count("FJ")).isEqualTo(1L);
  }

  private long count(final String type) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM " + type)) {
      return rs.next().<Number>getProperty("n").longValue();
    }
  }
}
