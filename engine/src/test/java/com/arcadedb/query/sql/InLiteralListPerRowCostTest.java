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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A literal list on the right of {@code IN} is turned into one membership probe per execution, but the decision "this right-hand
 * side is constant" was re-taken for every row by walking the whole list from the parse tree ({@code isLiteral} over every item),
 * so a scan cost rows x listSize before a single comparison was made: 5,000 literals over 100,000 rows took 6 s, against 35 ms
 * for the same list bound as a parameter.
 */
class InLiteralListPerRowCostTest extends TestHelper {

  @Test
  void aLongLiteralInListIsNotWalkedOncePerRow() {
    final int rows = 30_000;
    database.getSchema().createDocumentType("InLit");
    database.transaction(() -> {
      for (int i = 0; i < rows; i++)
        database.newDocument("InLit").set("id", i).save();
    });

    final StringBuilder list = new StringBuilder();
    for (int i = 0; i < rows; i++)
      list.append(i * 2).append(i + 1 < rows ? "," : "");

    final StallAwareStopwatch watch = StallAwareStopwatch.start();
    long matched = 0;
    try (final ResultSet result = database.query("sql", "SELECT id FROM InLit WHERE id IN [" + list + "]")) {
      while (result.hasNext()) {
        result.next();
        matched++;
      }
    }

    assertThat(matched).isEqualTo(rows / 2);
    watch.assertGaveUpWithin(7_000, "one probe built per execution from one walk per row of a " + rows + "-item list");
  }
}
