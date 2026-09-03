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
package com.arcadedb.query.sql.method;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7027: {@code split()} answered a {@code String[]}, and the collection methods each
 * mishandled that receiver in their own way - {@code join()} and {@code asString()} leaked the array's identity
 * {@code toString()} into the result set, {@code sort()} returned it unsorted with no error, {@code asList()} wrapped
 * the whole array as one element, {@code first()}/{@code last()} did not exist as methods at all. Every cell below is
 * the receiver matrix from the report, with the {@code List} literal as the control in the same run.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@SuppressWarnings("unchecked")
class Issue7027CollectionMethodsOnArrayReceiverTest extends TestHelper {

  @Test
  void splitAnswersAListLikeTheSplitFunctionAndTheCypherSplit() {
    assertThat(scalar("SELECT 'a,b'.split(',') AS r")).isInstanceOf(List.class).isEqualTo(List.of("a", "b"));
  }

  @Test
  void joinHonoursEveryCollectionReceiver() {
    assertThat(scalar("SELECT ['a','b'].join('-') AS r")).isEqualTo("a-b");
    assertThat(scalar("SELECT 'a,b'.split(',').join('-') AS r")).isEqualTo("a-b");
    assertThat(scalar("SELECT 'a,b'.split(',').asList().join('-') AS r")).isEqualTo("a-b");
    // A SET HAS NO ORDER, BUT THE SEPARATOR MUST BE HONOURED RATHER THAN THE SET'S OWN toString() RETURNED
    assertThat(scalar("SELECT ['b','a'].asSet().join('-') AS r")).isIn("a-b", "b-a");
  }

  @Test
  void sortSortsAnArrayInsteadOfReturningItUntouched() {
    assertThat(scalar("SELECT 'b,a'.split(',').sort() AS r")).isEqualTo(List.of("a", "b"));
    assertThat(scalar("SELECT 'a,b'.split(',').sort(false) AS r")).isEqualTo(List.of("b", "a"));
    assertThat(scalar("SELECT ['b','a'].asSet().sort() AS r")).isEqualTo(List.of("a", "b"));
  }

  @Test
  void sizeFirstAndLastWorkOnTheSplitResult() {
    assertThat(scalar("SELECT 'a,b'.split(',').size() AS r")).isEqualTo(2);
    assertThat(scalar("SELECT 'a,b'.split(',').first() AS r")).isEqualTo("a");
    assertThat(scalar("SELECT 'a,b'.split(',').last() AS r")).isEqualTo("b");
    // THE METHOD FORM IS THE SAME OPERATION AS THE FUNCTION FORM
    assertThat(scalar("SELECT first('a,b'.split(',')) AS r")).isEqualTo("a");
    assertThat(scalar("SELECT last(['a','b']) AS r")).isEqualTo("b");
    // A SCALAR IS AN IDENTITY, EXACTLY AS THE FUNCTIONS TREAT IT
    assertThat(scalar("SELECT 'abc'.first() AS r")).isEqualTo("abc");
    assertThat(scalar("SELECT 'abc'.last() AS r")).isEqualTo("abc");
  }

  @Test
  void asStringNeverLeaksTheJavaArrayIdentity() {
    final Object rendered = scalar("SELECT 'a,b'.split(',').asString() AS r");
    assertThat(rendered).isEqualTo("[a, b]");
    assertThat(scalar("SELECT ['a','b'].asString() AS r")).as("the list control renders the same").isEqualTo(rendered);
  }

  @Test
  void asListAsSetAndTransformSeeTheArrayElements() {
    assertThat(scalar("SELECT 'a,b'.split(',').asList() AS r")).isEqualTo(List.of("a", "b"));
    assertThat((Set<Object>) scalar("SELECT 'a,b,a'.split(',').asSet() AS r")).containsExactlyInAnyOrder("a", "b");
    assertThat(scalar("SELECT 'a,b'.split(',').transform('toUpperCase') AS r")).isEqualTo(List.of("A", "B"));
  }

  @Test
  void aJsonArrayParameterIsACollectionReceiverToo() {
    // A NUMERIC JSON ARRAY PARAMETER ARRIVES AS A PRIMITIVE ARRAY; A STRING ONE AS A String[]. ALSO PINS THAT THE
    // METHOD CHAIN AFTER A PARAMETER IS APPLIED AT ALL: THE AST BUILDER USED TO DROP IT AND ANSWER THE BARE PARAMETER
    assertThat(scalar("SELECT :p.size() AS r", Map.of("p", new int[] { 3, 1, 2 }))).isEqualTo(3);
    assertThat(scalar("SELECT :p.join('-') AS r", Map.of("p", new int[] { 3, 1, 2 }))).isEqualTo("3-1-2");
    assertThat(scalar("SELECT :p.sort() AS r", Map.of("p", new int[] { 3, 1, 2 }))).isEqualTo(List.of(1, 2, 3));
    assertThat(scalar("SELECT :p.join('-') AS r", Map.of("p", new String[] { "x", "y" }))).isEqualTo("x-y");
  }

  private Object scalar(final String query) {
    return scalar(query, Map.of());
  }

  private Object scalar(final String query, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("sql", query, params)) {
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      assertThat(rs.hasNext()).isFalse();
      return result.getProperty("r");
    }
  }
}
