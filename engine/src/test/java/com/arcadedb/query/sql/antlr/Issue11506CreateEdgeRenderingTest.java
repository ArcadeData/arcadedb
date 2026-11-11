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
package com.arcadedb.query.sql.antlr;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.parser.Statement;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #11506: CREATE EDGE rendered UNIDIRECTIONAL before IF NOT EXISTS and the SET/CONTENT body, an order
 * the grammar does not accept, so the rendering of an edge with both did not parse back.
 */
class Issue11506CreateEdgeRenderingTest extends TestHelper {

  @Test
  void theClausesAreRenderedInTheOrderTheGrammarAccepts() {
    assertThat(render("CREATE EDGE E FROM #1:0 TO #2:0 CONTENT {\"w\":1} UNIDIRECTIONAL")).isEqualTo(
        "CREATE EDGE E FROM #1:0 TO #2:0 CONTENT {\"w\": 1} UNIDIRECTIONAL");
    assertThat(render("CREATE EDGE E FROM #1:0 TO #2:0 SET w = 1 UNIDIRECTIONAL")).isEqualTo(
        "CREATE EDGE E FROM #1:0 TO #2:0 SET w = 1 UNIDIRECTIONAL");
    assertThat(render("CREATE EDGE E FROM #1:0 TO #2:0 IF NOT EXISTS SET w = 1 UNIDIRECTIONAL")).isEqualTo(
        "CREATE EDGE E FROM #1:0 TO #2:0 IF NOT EXISTS SET w = 1 UNIDIRECTIONAL");

    for (final String query : List.of(//
        "CREATE EDGE E FROM #1:0 TO #2:0 CONTENT {\"w\":1} UNIDIRECTIONAL",//
        "CREATE EDGE E FROM #1:0 TO #2:0 SET w = 1 UNIDIRECTIONAL",//
        "CREATE EDGE E FROM #1:0 TO #2:0 IF NOT EXISTS SET w = 1 UNIDIRECTIONAL",//
        "CREATE EDGE E FROM #1:0 TO #2:0 IF NOT EXISTS CONTENT {\"w\":1} UNIDIRECTIONAL",//
        "CREATE EDGE E FROM #1:0 TO #2:0 IF NOT EXISTS UNIDIRECTIONAL",//
        // controls: rendered in the right order already
        "CREATE EDGE E FROM #1:0 TO #2:0 SET w = 1",//
        "CREATE EDGE E FROM #1:0 TO #2:0 UNIDIRECTIONAL",//
        "CREATE EDGE E FROM #1:0 TO #2:0 IF NOT EXISTS SET w = 1")) {
      final String rendered = render(query);
      assertThat(parse(rendered)).as("'%s' rendered as '%s'", query, rendered).isEqualTo(parse(query));
      assertThat(render(rendered)).isEqualTo(rendered);
    }
  }

  private static Statement parse(final String query) {
    return new SQLAntlrParser(null).parse(query);
  }

  private static String render(final String query) {
    final StringBuilder builder = new StringBuilder();
    parse(query).toString((Map<String, Object>) null, builder);
    return builder.toString();
  }
}
