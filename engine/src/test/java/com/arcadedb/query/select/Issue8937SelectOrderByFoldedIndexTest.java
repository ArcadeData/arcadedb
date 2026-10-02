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
package com.arcadedb.query.select;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8937: {@code database.select()} elided its ORDER BY whenever the single index used matched the
 * order property, but a {@code COLLATE ci} index holds folded keys, so its iteration order is not the order of the values and
 * {@code orderBy + limit} returned the wrong rows.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8937SelectOrderByFoldedIndexTest extends TestHelper {

  private static final String[] VALUES = { "AZb", "AZ", "azc", "Az", "AY", "B", "a[", "abc", "Mzz", "m", "Zoo", "apple", "Banana" };

  private void load(final String type, final boolean collateCi) {
    database.command("sql", "CREATE DOCUMENT TYPE " + type);
    database.command("sql", "CREATE PROPERTY " + type + ".s STRING");
    if (collateCi)
      database.command("sql", "CREATE INDEX ON " + type + " (s COLLATE ci) NOTUNIQUE");
    else
      database.command("sql", "CREATE INDEX ON " + type + " (s) NOTUNIQUE");
    database.transaction(() -> {
      for (final String v : VALUES)
        database.newDocument(type).set("s", v).save();
    });
  }

  private List<String> ordered(final String type, final boolean asc, final int limit) {
    final Select q = database.select().fromType(type).where().property("s").gt().value("A").orderBy("s", asc);
    return (limit > 0 ? q.limit(limit) : q).documents().toList().stream().map(d -> d.getString("s")).toList();
  }

  @Test
  void limitedOrderByOnAFoldedIndexMatchesTheUnindexedAnswer() {
    load("Ci", true);
    load("Nx", false);
    database.command("sql", "CREATE DOCUMENT TYPE Plain");
    database.command("sql", "CREATE PROPERTY Plain.s STRING");
    database.transaction(() -> {
      for (final String v : VALUES)
        database.newDocument("Plain").set("s", v).save();
    });

    for (final boolean asc : new boolean[] { true, false }) {
      final List<String> expected = ordered("Plain", asc, 0);
      assertThat(ordered("Ci", asc, 0)).isEqualTo(expected);
      assertThat(ordered("Nx", asc, 0)).isEqualTo(expected);
      assertThat(ordered("Ci", asc, 3)).isEqualTo(expected.subList(0, 3));
    }
    assertThat(ordered("Ci", true, 3)).containsExactly("AY", "AZ", "AZb");
  }
}
