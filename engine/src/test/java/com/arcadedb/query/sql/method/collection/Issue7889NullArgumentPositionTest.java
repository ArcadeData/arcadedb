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
package com.arcadedb.query.sql.method.collection;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7889: {@code remove()} and {@code removeAll()} are declared varargs but gated the WHOLE
 * operation on the FIRST argument being non-null, so a leading null silently discarded every argument after it while
 * the same null one position later was acted on. The statement still reported success, so an
 * {@code UPDATE ... SET tags = tags.remove($a, $b)} whose first bound parameter happened to be null left the property
 * exactly as it was.
 * <p>
 * The sibling {@code append()} lost the same positional contradiction under #7028. These two keep the ability to
 * remove a null ELEMENT - a collection really can hold one - so the guard is now only "there is an argument", and
 * {@code MultiValue.removeFromCollection} was made null-safe, which also closes the
 * {@code removeAll(x, null)} NPE that came from binding {@code iToRemove::equals} on a null receiver.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7889NullArgumentPositionTest extends TestHelper {

  @Test
  void aLeadingNullNoLongerDiscardsTheRemainingRemoveArguments() {
    assertThat(list("SELECT [1,2,3].remove(2) AS r")).containsExactly(1, 3);
    assertThat(list("SELECT [1,2,3].remove(null,2) AS r")).as("the 2 must be removed whatever precedes it")
        .containsExactly(1, 3);
    assertThat(list("SELECT [1,2,3].remove(2,null) AS r")).containsExactly(1, 3);
  }

  @Test
  void aLeadingNullNoLongerDiscardsTheRemainingRemoveAllArguments() {
    assertThat(list("SELECT [1,2,2,3].removeAll(2) AS r")).containsExactly(1, 3);
    assertThat(list("SELECT [1,2,2,3].removeAll(null,2) AS r")).as("the 2s must be removed whatever precedes them")
        .containsExactly(1, 3);
  }

  @Test
  void aNullArgumentToRemoveAllNoLongerThrows() {
    // MultiValue.removeFromCollection used to bind iToRemove::equals, which throws from Objects.requireNonNull
    // before the first element is even looked at.
    assertThat(list("SELECT [1,2,3].removeAll(2,null) AS r")).containsExactly(1, 3);
    assertThat(list("SELECT [1,2,3].removeAll(null) AS r")).containsExactly(1, 2, 3);
  }

  @Test
  void positionNoLongerChangesWhichElementsAreRemoved() {
    // A null element IS removable, and it is removed the same way from either position - which is the contradiction
    // the issue is about: the two calls below used to answer differently.
    assertThat(sameAnswerFromBothOrders("remove")).isTrue();
    assertThat(sameAnswerFromBothOrders("removeAll")).isTrue();
  }

  @Test
  void aNullElementIsRemovedFromTheFirstPositionToo() {
    final List<Object> withNull = Arrays.asList(1, null, 3);
    assertThat(list("SELECT :v.remove(null) AS r", Map.of("v", withNull))).containsExactly(1, 3);
    assertThat(list("SELECT :v.removeAll(null) AS r", Map.of("v", withNull))).containsExactly(1, 3);
  }

  @Test
  void anUpdateWithALeadingNullParameterStillWritesTheRemainingRemovals() {
    database.getSchema().createDocumentType("T7889");
    database.transaction(() -> database.command("sql", "INSERT INTO T7889 SET tags = ['a','b','c']"));
    database.transaction(() -> database.command("sql", "UPDATE T7889 SET tags = tags.remove(:a, :b)",
        Map.of("b", "b")));

    try (final ResultSet rs = database.query("sql", "SELECT tags FROM T7889")) {
      assertThat(rs.next().<List<Object>>getProperty("tags")).containsExactly("a", "c");
    }
  }

  private boolean sameAnswerFromBothOrders(final String methodName) {
    final List<Object> withNull = Arrays.asList(1, null, 3);
    final List<Object> nullFirst = list("SELECT :v." + methodName + "(null, 3) AS r", Map.of("v", withNull));
    final List<Object> nullLast = list("SELECT :v." + methodName + "(3, null) AS r", Map.of("v", withNull));
    return nullFirst.equals(nullLast);
  }

  private List<Object> list(final String query) {
    return list(query, Map.of());
  }

  private List<Object> list(final String query, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("sql", query, params)) {
      assertThat(rs.hasNext()).isTrue();
      return rs.next().getProperty("r");
    }
  }
}
