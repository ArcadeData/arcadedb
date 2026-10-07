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
import com.arcadedb.database.Document;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #9302: the native {@code select()} range cursor (gt/ge/lt/le/between) was built over a
 * {@code COLLATE ci} index, whose keys are folded, so the cursor yielded a different set of rows than the unindexed query and
 * the residual filter could not bring back the rows it never returned.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9302SelectRangeFoldedIndexTest extends TestHelper {

  private static final String[] VALUES = { "AZb", "AZ", "azc", "Az", "AY", "B", "a[", "abc", "Mzz", "m", "Zoo", "apple", "Banana" };

  private void load(final String type, final String index) {
    database.command("sql", "CREATE DOCUMENT TYPE " + type);
    database.command("sql", "CREATE PROPERTY " + type + ".s STRING");
    if (index != null)
      database.command("sql", index);
    database.transaction(() -> {
      for (final String v : VALUES)
        database.newDocument(type).set("s", v).save();
    });
  }

  private static List<String> sorted(final SelectWhereAfterBlock query) {
    return query.documents().toList().stream().map((final Document d) -> d.getString("s")).sorted().toList();
  }

  private SelectWhereLeftBlock where(final String type) {
    return database.select().fromType(type).where();
  }

  private List<String> run(final String type, final String op) {
    final SelectWhereOperatorBlock property = where(type).property("s");
    return sorted(switch (op) {
      case "lt" -> property.lt().value("m");
      case "le" -> property.le().value("m");
      case "gt" -> property.gt().value("B");
      case "ge" -> property.ge().value("B");
      default -> property.between().values("B", "m");
    });
  }

  @Test
  void everyRangeOperatorOnAFoldedIndexMatchesTheUnindexedAnswer() {
    load("Ci", "CREATE INDEX ON Ci (s COLLATE ci) NOTUNIQUE");
    load("Cs", "CREATE INDEX ON Cs (s) NOTUNIQUE");
    load("Plain", null);

    for (final String op : new String[] { "lt", "le", "gt", "ge", "between" }) {
      final List<String> expected = run("Plain", op);
      assertThat(expected).as(op).isNotEmpty();
      assertThat(run("Ci", op)).as("ci " + op).isEqualTo(expected);
      assertThat(run("Cs", op)).as("cs " + op).isEqualTo(expected);
    }
  }

  @Test
  void rangeUnderOrOnAFoldedIndexStaysComplete() {
    load("Ci", "CREATE INDEX ON Ci (s COLLATE ci) NOTUNIQUE");
    load("Plain", null);

    final List<String> expected = sorted(where("Plain").property("s").gt().value("B").or().property("s").eq().value("AY"));
    assertThat(expected).contains("AY", "abc");
    assertThat(sorted(where("Ci").property("s").gt().value("B").or().property("s").eq().value("AY"))).isEqualTo(expected);
  }
}
