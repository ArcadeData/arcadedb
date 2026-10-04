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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9173: every indexed query recorded statistics into a new, discarded QueryStats instance, paying a key string
 * and a map per query for nothing.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9173QueryStatsTest extends TestHelper {

  @Test
  void statsAreSharedAndNeverRecordedWhileNothingReadsThem() {
    assertThat(QueryStats.get(database)).isSameAs(QueryStats.get(database));
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Item");
      database.command("sql", "CREATE PROPERTY Item.k LONG");
      database.command("sql", "CREATE INDEX ON Item (k) UNIQUE");
      database.command("sql", "INSERT INTO Item SET k = 1");
    });
    for (int i = 0; i < 10; i++)
      try (final ResultSet rs = database.query("sql", "SELECT FROM Item WHERE k = ?", 1L)) {
        assertThat(rs.hasNext()).isTrue();
      }
    assertThat(QueryStats.get(database).stats).isEmpty();
    assertThat(QueryStats.get(database).getIndexStats("Item[k]", 1, false, false)).isEqualTo(-1);
  }
}
