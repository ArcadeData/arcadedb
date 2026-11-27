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
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for #12755: {@code ts.movingAvg} read a window of 0, a negative window, a fraction or an options map without
 * {@code window} as a window of 1 (or truncated it), answering the unaveraged series.
 */
class MovingAvgWindowIssue12755Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Ma12755");
    database.transaction(() -> {
      for (int i = 1; i <= 6; i++)
        database.newDocument("Ma12755").set("v", i).save();
    });
  }

  @Test
  void aWindowThatIsNotAtLeastOneWholeRowIsRefused() {
    for (final String window : List.of("0", "-2", "{}", "{ window: 0 }", "{ window: -1 }", "2.5", "{ window: 2.5 }", "null"))
      assertThatThrownBy(() -> average(window)).as("window " + window).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void aValidWindowStillAverages() {
    assertThat(average("3")).containsExactly(1.0, 1.5, 2.0, 3.0, 4.0, 5.0);
    assertThat(average("{ window: 2 }")).containsExactly(1.0, 1.5, 2.5, 3.5, 4.5, 5.5);
    assertThat(average("1")).containsExactly(1.0, 2.0, 3.0, 4.0, 5.0, 6.0);
    assertThat(average("'2'")).containsExactly(1.0, 1.5, 2.5, 3.5, 4.5, 5.5);
    assertThat(average("100")).containsExactly(1.0, 1.5, 2.0, 2.5, 3.0, 3.5);
  }

  @SuppressWarnings("unchecked")
  private List<Double> average(final String window) {
    try (final ResultSet rs = database.query("sql", "SELECT ts.movingAvg(v, " + window + ") AS r FROM Ma12755")) {
      return (List<Double>) rs.next().getProperty("r");
    }
  }
}
