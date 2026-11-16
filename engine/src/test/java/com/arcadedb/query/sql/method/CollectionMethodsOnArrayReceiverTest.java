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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class CollectionMethodsOnArrayReceiverTest {

  /**
   * Regression for #7027: split() answers a List and the collection methods honour array receivers.
   */
  @SuppressWarnings("unchecked")
  @Nested
  class SplitAndCollectionMethods extends TestHelper {

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

  /**
   * Regression for #7114: keys(), values(), ifempty(), remove() and removeAll() honour array receivers (follow-up to #7027).
   */
  @SuppressWarnings("unchecked")
  @Nested
  class KeysValuesRemoveIfEmpty extends TestHelper {

    @Test
    void removeHonoursAnArrayReceiver() {
      assertThat(scalar("SELECT :p.remove(2) AS r", Map.of("p", new int[] { 3, 2, 1, 2 }))).isEqualTo(List.of(3, 1, 2));
      assertThat(scalar("SELECT :p.remove('y') AS r", Map.of("p", new String[] { "x", "y", "z" }))).isEqualTo(List.of("x", "z"));
      assertThat(scalar("SELECT :p.remove('y', 'z') AS r", Map.of("p", new String[] { "x", "y", "z" }))).isEqualTo(List.of("x"));
      // THE LIST CONTROL ANSWERS THE SAME, AND A MAP RECEIVER KEEPS ITS OWN KIND
      assertThat(scalar("SELECT [3,2,1,2].remove(2) AS r")).isEqualTo(List.of(3, 1, 2));
      assertThat(scalar("SELECT {'a': 1, 'b': 2}.remove('a') AS r")).isEqualTo(Map.of("b", 2));
      final Set<Integer> set = new LinkedHashSet<>(List.of(3, 2, 1));
      assertThat(scalar("SELECT :p.remove(2) AS r", Map.of("p", set))).isInstanceOf(Set.class).isEqualTo(Set.of(3, 1));
      assertThat(scalar("SELECT :p.removeAll(2) AS r", Map.of("p", set))).isInstanceOf(Set.class).isEqualTo(Set.of(3, 1));
      assertThat(set).as("the source set is not mutated").containsExactly(3, 2, 1);
    }

    @Test
    void removeAllHonoursAnArrayReceiver() {
      assertThat(scalar("SELECT :p.removeAll(2) AS r", Map.of("p", new int[] { 3, 2, 1, 2 }))).isEqualTo(List.of(3, 1));
      assertThat(scalar("SELECT :p.removeAll('y') AS r", Map.of("p", new String[] { "x", "y", "y" }))).isEqualTo(List.of("x"));
      assertThat(scalar("SELECT [3,2,1,2].removeAll(2) AS r")).isEqualTo(List.of(3, 1));
    }

    @Test
    void removeDoesNotMutateTheArrayParameter() {
      final int[] source = { 3, 2, 1 };
      assertThat(scalar("SELECT :p.remove(2) AS r", Map.of("p", source))).isEqualTo(List.of(3, 1));
      assertThat(source).containsExactly(3, 2, 1);
    }

    @Test
    void ifEmptyRecognisesAnEmptyArray() {
      assertThat(scalar("SELECT :p.ifempty('none') AS r", Map.of("p", new String[0]))).isEqualTo("none");
      assertThat(scalar("SELECT :p.ifempty('none') AS r", Map.of("p", new int[0]))).isEqualTo("none");
      assertThat(scalar("SELECT :p.ifempty('none') AS r", Map.of("p", new int[] { 1 }))).isEqualTo(new int[] { 1 });
      assertThat(scalar("SELECT [].ifempty('none') AS r")).isEqualTo("none");
      assertThat(scalar("SELECT ''.ifempty('none') AS r")).isEqualTo("none");
      assertThat(scalar("SELECT 'a'.ifempty('none') AS r")).isEqualTo("a");
      // AN ITERATOR RECEIVER IS CONSUMED BY THE TEST FOR EMPTINESS, SO THE ANSWER IS WHAT IT HELD, NOT THE EXHAUSTED ITERATOR
      assertThat(scalar("SELECT :p.ifempty('none') AS r", Map.of("p", List.of(1, 2).iterator()))).isEqualTo(List.of(1, 2));
      assertThat(scalar("SELECT :p.ifempty('none') AS r", Map.of("p", List.of().iterator()))).isEqualTo("none");
    }

    @Test
    void keysAndValuesSeeTheElementsOfAnArrayReceiver() {
      final Object[] maps = { Map.of("a", 1), Map.of("b", 2) };
      assertThat((List<Object>) scalar("SELECT :p.keys() AS r", Map.of("p", maps))).containsExactly("a", "b");
      assertThat((List<Object>) scalar("SELECT :p.values() AS r", Map.of("p", maps))).containsExactly(1, 2);
      // THE LIST CONTROL ANSWERS THE SAME
      assertThat((List<Object>) scalar("SELECT :p.keys() AS r", Map.of("p", List.of(maps)))).containsExactly("a", "b");
      assertThat((List<Object>) scalar("SELECT :p.values() AS r", Map.of("p", List.of(maps)))).containsExactly(1, 2);
    }

    @Test
    void keysAndValuesOnAnArrayOfScalarsAnswerAnEmptyListLikeTheListForm() {
      assertThat((List<Object>) scalar("SELECT :p.keys() AS r", Map.of("p", new int[] { 1, 2 }))).isEmpty();
      assertThat((List<Object>) scalar("SELECT :p.values() AS r", Map.of("p", new int[] { 1, 2 }))).isEmpty();
      assertThat((List<Object>) scalar("SELECT [1,2].keys() AS r")).isEmpty();
      assertThat((List<Object>) scalar("SELECT [1,2].values() AS r")).isEmpty();
    }

    @Test
    void removeOnAStoredArrayPropertyAnswersTheReducedList() {
      database.transaction(() -> {
        database.command("sql", "CREATE DOCUMENT TYPE Issue7114");
        database.command("sql", "CREATE PROPERTY Issue7114.tags LIST");
        database.command("sql", "INSERT INTO Issue7114 SET tags = ['x','y','z']");
      });
      assertThat(scalar("SELECT tags.remove('y') AS r FROM Issue7114")).isEqualTo(List.of("x", "z"));
      assertThat(scalar("SELECT tags.removeAll('y') AS r FROM Issue7114")).isEqualTo(List.of("x", "z"));
      assertThat(scalar("SELECT tags.ifempty('none') AS r FROM Issue7114")).isEqualTo(List.of("x", "y", "z"));
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
}
