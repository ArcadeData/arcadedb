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
package com.arcadedb.function.sql.time;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code sysdate()} stored into a DATETIME held the real instant only when the JVM ran in UTC: the JVM-local wall clock
 * was handed to the engine, which stores a {@code LocalDateTime} as a UTC wall clock, and {@code sysdate('zone')}
 * labelled that same local wall clock with the zone instead of reading the clock there (issue #8892).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8892SysdateInstantTest extends TestHelper {
  private static final long TOLERANCE_MS = 60_000L;

  private TimeZone previousZone;

  @BeforeEach
  void rememberZone() {
    previousZone = TimeZone.getDefault();
  }

  @AfterEach
  void restoreZone() {
    TimeZone.setDefault(previousZone);
  }

  @Test
  void sysdateStoresTheRealInstantOnAnyJvmZone() {
    for (final String jvmZone : new String[] { "UTC", "Asia/Seoul", "America/New_York" }) {
      TimeZone.setDefault(TimeZone.getTimeZone(jvmZone));
      final String type = "E_" + jvmZone.replace('/', '_');
      database.command("sql", "CREATE DOCUMENT TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".at DATETIME");

      final long before = System.currentTimeMillis();
      database.transaction(() -> {
        database.command("sql", "INSERT INTO " + type + " SET k = 'sysdate', at = sysdate()");
        database.command("sql", "INSERT INTO " + type + " SET k = 'utc', at = sysdate('UTC')");
        database.command("sql", "INSERT INTO " + type + " SET k = 'seoul', at = sysdate('Asia/Seoul')");
        database.command("sql", "INSERT INTO " + type + " SET k = 'ny', at = sysdate('America/New_York')");
        database.newDocument(type).set("k", "java", "at", new Date()).save();
      });

      try (final ResultSet rs = database.query("sql", "SELECT k, at.asLong() AS ms FROM " + type)) {
        int rows = 0;
        while (rs.hasNext()) {
          final var row = rs.next();
          final long ms = row.<Long>getProperty("ms");
          assertThat(Math.abs(ms - before)).as("JVM zone %s, %s", jvmZone, row.<String>getProperty("k")).isLessThan(TOLERANCE_MS);
          rows++;
        }
        assertThat(rows).isEqualTo(5);
      }
    }
  }
}
