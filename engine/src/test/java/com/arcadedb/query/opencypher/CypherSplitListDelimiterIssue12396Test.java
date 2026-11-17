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
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #12396: {@code split()} only accepted a single STRING delimiter, while Neo4j 5 also
 * accepts {@code split(original :: STRING, splitDelimiters :: LIST<STRING>)}, which splits the text on any of the
 * delimiters of the list.
 * <p>
 * The expected values are the ones Neo4j 5.26.33 answers (measured for this issue):
 * <ul>
 *   <li>{@code split('a1b2c', ['1','2'])} is {@code ['a','b','c']}</li>
 *   <li>{@code split('abc', ['b','a'])} is {@code ['','','c']}</li>
 *   <li>{@code split(null, ['1'])} is {@code null}</li>
 *   <li>{@code split('abc', [])} is {@code ['abc']}</li>
 *   <li>{@code split('abc', [''])} is {@code ['a','b','c']}</li>
 *   <li>{@code split('abc', ['b',null])} is {@code null}</li>
 * </ul>
 * A delimiter of the list is a literal, never a pattern: {@code split('aXb', ['.'])} is {@code ['aXb']}.
 */
class CypherSplitListDelimiterIssue12396Test extends TestHelper {

  @Test
  void splitsOnAnyOfTheDelimitersOfTheList() {
    assertThat(split("split('a1b2c', ['1','2']) AS r")).containsExactly("a", "b", "c");
    assertThat(split("split('abc', ['b','a']) AS r")).containsExactly("", "", "c");
    assertThat(split("split('a1b', ['1']) AS r")).containsExactly("a", "b");
    assertThat(split("split('a,b,c', [',',';']) AS r")).containsExactly("a", "b", "c");
    assertThat(split("split('a-b_c', ['-','_']) AS r")).containsExactly("a", "b", "c");
  }

  @Test
  void theDelimitersAreLiteralsAndNotPatterns() {
    assertThat(split("split('aXb', ['.']) AS r")).containsExactly("aXb");
    assertThat(split("split('axb', ['a|b']) AS r")).containsExactly("axb");
    assertThat(split("split('a*b', ['*']) AS r")).containsExactly("a", "b");
    assertThat(split("split('a.b', ['.']) AS r")).containsExactly("a", "b");
  }

  @Test
  void theLeftmostDelimiterWinsAndTheFirstOfTheListBreaksATie() {
    // 'a' is found before 'aa', and the list order decides between two delimiters found at the same place
    assertThat(split("split('aaa', ['a','aa']) AS r")).containsExactly("", "", "", "");
    assertThat(split("split('ab', ['ab','a']) AS r")).containsExactly("", "");
    assertThat(split("split('abc', ['abc']) AS r")).containsExactly("", "");
    assertThat(split("split('abc', ['abcd']) AS r")).containsExactly("abc");
    assertThat(split("split('a,b,,c', [',',',']) AS r")).containsExactly("a", "b", "", "c");
  }

  @Test
  void anEmptyListSplitsNothing() {
    assertThat(split("split('abc', []) AS r")).containsExactly("abc");
  }

  @Test
  void anEmptyDelimiterSplitsTheTextIntoItsCharacters() {
    assertThat(split("split('abc', ['']) AS r")).containsExactly("a", "b", "c");
    // the other delimiters of the list are cut out of the text before it is split into characters
    assertThat(split("split('abc', ['','a']) AS r")).containsExactly("b", "c");
    assertThat(split("split('abc', ['','ab']) AS r")).containsExactly("c");
  }

  @Test
  void aNullTextOrDelimiterIsNull() {
    assertThat(split("split(null, ['1']) AS r")).isNull();
    assertThat(split("split('abc', null) AS r")).isNull();
    assertThat(split("split('abc', ['b',null]) AS r")).isNull();
  }

  @Test
  void aDelimiterThatIsNotAStringIsAClientTypeError() {
    assertThatThrownBy(() -> split("split('abc', [1]) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("split()")
        .hasMessageContaining("STRING");
    assertThatThrownBy(() -> split("split('abc', [1,'b']) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("split()")
        .hasMessageContaining("STRING");
    assertThatThrownBy(() -> split("split('abc', true) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("split()")
        .hasMessageContaining("STRING");
    // the primary argument keeps its own type check
    assertThatThrownBy(() -> split("split(5, ['1']) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("split()")
        .hasMessageContaining("STRING");
  }

  @Test
  void theSingleStringDelimiterStillWorks() {
    assertThat(split("split('a,b', ',') AS r")).containsExactly("a", "b");
    assertThat(split("split('abc', '') AS r")).containsExactly("a", "b", "c");
  }

  @SuppressWarnings("unchecked")
  private List<String> split(final String projection) {
    try (final ResultSet rs = database.query("opencypher", "RETURN " + projection)) {
      assertThat(rs.hasNext()).isTrue();
      return (List<String>) rs.next().getProperty("r");
    }
  }
}
