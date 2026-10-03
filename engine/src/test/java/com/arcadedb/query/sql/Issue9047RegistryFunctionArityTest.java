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
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #9047: registry functions (text.*, date.*, agg.* ...) called with too few arguments ended in a raw
 * ArrayIndexOutOfBoundsException instead of a CommandSemanticException, in SQL and in openCypher.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9047RegistryFunctionArityTest extends TestHelper {
  @Test
  void tooFewArgumentsIsASemanticError() {
    for (final String call : new String[] { "text.regexReplace('abc', 'y')", "text.replace('abc')", "date.parse()", "convert.toInteger()",
        "map.merge()", "agg.first()", "util.md5()" }) {
      assertThatThrownBy(() -> drain("sql", "SELECT " + call + " AS r")).as("sql " + call).isInstanceOf(CommandSemanticException.class);
      assertThatThrownBy(() -> drain("opencypher", "RETURN " + call + " AS r")).as("cypher " + call)
          .isInstanceOf(CommandSemanticException.class);
    }
  }

  @Test
  void theMessageNamesTheFunctionAndTheCounts() {
    assertThatThrownBy(() -> drain("sql", "SELECT text.replace('abc') AS r")).isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("text.replace").hasMessageContaining("got 1");
  }

  @Test
  void tooManyArgumentsIsASemanticError() {
    assertThatThrownBy(() -> drain("sql", "SELECT text.replace('a', 'b', 'c', 'd') AS r")).isInstanceOf(CommandSemanticException.class);
    assertThatThrownBy(() -> drain("opencypher", "RETURN text.replace('a', 'b', 'c', 'd') AS r")).isInstanceOf(CommandSemanticException.class);
  }

  @Test
  void aggregatorsWithWrongArityAreRejected() {
    assertThatThrownBy(() -> drain("opencypher", "UNWIND [1,2] AS x RETURN agg.first() AS r")).isInstanceOf(CommandSemanticException.class);
    assertThatThrownBy(() -> drain("opencypher", "UNWIND [1,2] AS x RETURN x, agg.first() AS r")).isInstanceOf(CommandSemanticException.class);
  }

  private void drain(final String language, final String query) {
    try (final ResultSet rs = database.query(language, query)) {
      while (rs.hasNext())
        rs.next();
    }
  }
}
