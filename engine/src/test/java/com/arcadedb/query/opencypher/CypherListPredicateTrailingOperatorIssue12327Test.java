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

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12327: a list predicate ({@code all}, {@code any}, {@code none}, {@code single}) followed by a short operator
 * ({@code none(...)=a}, {@code any(...)<>b}) was taken for the whole expression by a text-length tolerance, and the
 * trailing comparison was silently dropped. The same tolerance decided whether a list literal was the whole expression.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherListPredicateTrailingOperatorIssue12327Test extends TestHelper {

  @Test
  void theReportedQueriesKeepTheComparison() {
    assertThat(single("RETURN none(x IN [1] WHERE x = 2) AS r")).isEqualTo(true);
    assertThat(single("WITH false AS a RETURN none(x IN [1] WHERE x = 2)=a AS r")).isEqualTo(false);
    assertThat(single("WITH false AS a RETURN (none(x IN [1] WHERE x = 2))=a AS r")).isEqualTo(false);
  }

  @Test
  void everyListPredicateKeepsATwoCharacterTrailingComparison() {
    // none(...) is true, any(...) is true, all(...) is false, single(...) is true
    assertThat(single("WITH false AS a RETURN none(x IN [1] WHERE x = 2)=a AS r")).isEqualTo(false);
    assertThat(single("WITH true AS b RETURN any(x IN [1, 2] WHERE x = 2)<>b AS r")).isEqualTo(false);
    assertThat(single("WITH false AS c RETURN all(x IN [1, 2] WHERE x = 2)<>c AS r")).isEqualTo(false);
    assertThat(single("WITH true AS d RETURN single(x IN [1, 2] WHERE x = 2)=d AS r")).isEqualTo(true);
    assertThat(single("WITH false AS e RETURN single(x IN [1, 2] WHERE x = 2)=e AS r")).isEqualTo(false);
    // <= and >= on booleans: false < true
    assertThat(single("WITH false AS f RETURN none(x IN [1] WHERE x = 2)<=f AS r")).isEqualTo(false);
    assertThat(single("WITH true AS g RETURN all(x IN [1, 2] WHERE x = 2)>=g AS r")).isEqualTo(false);
    // With whitespace, which never counted in the text compared
    assertThat(single("WITH false AS a RETURN none(x IN [1] WHERE x = 2) = a AS r")).isEqualTo(false);
  }

  @Test
  void longerTrailingOperatorsAndOperandsKeepWorking() {
    assertThat(single("RETURN none(x IN [1] WHERE x = 2) = false AS r")).isEqualTo(false);
    assertThat(single("RETURN any(x IN [1, 2] WHERE x = 2) AND false AS r")).isEqualTo(false);
    assertThat(single("RETURN none(x IN [1] WHERE x = 2) IS NULL AS r")).isEqualTo(false);
    assertThat(single("RETURN NOT none(x IN [1] WHERE x = 2) AS r")).isEqualTo(false);
    // A predicate as an operand on the right
    assertThat(single("WITH false AS a RETURN a=none(x IN [1] WHERE x = 2) AS r")).isEqualTo(false);
  }

  @Test
  void aListLiteralFollowedByAShortOperatorIsNotTheWholeExpression() {
    assertThat(single("WITH [2] AS a RETURN [1]+a AS r")).isEqualTo(List.of(1L, 2L));
    assertThat(single("WITH [1] AS a RETURN [1]=a AS r")).isEqualTo(true);
    assertThat(single("WITH [2] AS a RETURN [1]<>a AS r")).isEqualTo(true);
    assertThat(single("RETURN [1, 2, 3][1] AS r")).isEqualTo(2L);
    assertThat(single("RETURN [x IN [1, 2] | x * 2] AS r")).isEqualTo(List.of(2L, 4L));
  }

  private Object single(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).as(query).isTrue();
      final Object value = rs.next().getProperty("r");
      assertThat(rs.hasNext()).as(query).isFalse();
      return value;
    }
  }
}
