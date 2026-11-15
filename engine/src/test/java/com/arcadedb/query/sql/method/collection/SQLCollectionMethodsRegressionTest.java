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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for the SQL collection methods ({@code remove()}, {@code removeAll()}, {@code keys()}, {@code values()}).
 */
class SQLCollectionMethodsRegressionTest {

  /**
   * Regression for #7889: {@code remove()} and {@code removeAll()} are declared varargs but gated the WHOLE
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
  @Nested
  class NullArgumentPosition extends TestHelper {

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
      // :a is bound to an explicit null: a parameter with no key at all is refused since issue #11098
      final Map<String, Object> parameters = new HashMap<>();
      parameters.put("a", null);
      parameters.put("b", "b");
      database.transaction(() -> database.command("sql", "UPDATE T7889 SET tags = tags.remove(:a, :b)", parameters));

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

  /**
   * Regression for #9843: {@code keys()} and {@code values()} are an index-aligned pair, and a {@code Result}
   * receiver (a sub-query row, a {@code LET} variable) broke it twice. {@code values()} went through
   * {@code ResultInternal.toMap()}, which is the metadata overload and element-first, so the record's {@code @cat},
   * {@code @type} and {@code @rid} (and an edge's {@code @in}/{@code @out}) came back as unlabelled values, and a row
   * that overrode a property of its element reported the element's value in that slot.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class KeysValuesOnResultReceiver extends TestHelper {

    @Override
    protected void beginTest() {
      database.getSchema().createDocumentType("D1");
      database.getSchema().createVertexType("V1");
      database.getSchema().createEdgeType("R1");

      database.transaction(() -> {
        database.command("sql", "insert into D1 set n = 1, s = 'x'");
        final Result from = database.command("sql", "create vertex V1 set n = 1").next();
        final Result to = database.command("sql", "create vertex V1 set n = 2").next();
        database.command("sql", "create edge R1 from " + from.getIdentity().get() + " to " + to.getIdentity().get() + " set w = 7");
      });
    }

    @Test
    void documentReceiverIsTheControl() {
      try (final ResultSet rs = database.query("sql", "select @this.keys() as k, @this.values() as v from D1")) {
        final Result row = rs.next();
        assertThat(row.<Collection<String>>getProperty("k")).containsExactly("n", "s");
        assertThat(row.<Collection<Object>>getProperty("v")).containsExactly(1, "x");
      }
    }

    @Test
    void documentRowOfASubQueryHasNoMetadataValues() {
      try (final ResultSet rs = database.query("sql",
          "select $a[0].keys() as k, $a[0].values() as v from D1 let $a = (select from D1)")) {
        final Result row = rs.next();
        assertThat(row.<Collection<String>>getProperty("k")).containsExactly("n", "s");
        assertThat(row.<Collection<Object>>getProperty("v")).containsExactly(1, "x");
      }
    }

    @Test
    void sizesAgreeOnADocumentRow() {
      try (final ResultSet rs = database.query("sql",
          "select $a[0].keys().size() as nk, $a[0].values().size() as nv from D1 let $a = (select from D1)")) {
        final Result row = rs.next();
        assertThat(row.<Integer>getProperty("nk")).isEqualTo(2);
        assertThat(row.<Integer>getProperty("nv")).isEqualTo(2);
      }
    }

    @Test
    void vertexRowOfASubQueryHasNoMetadataValues() {
      try (final ResultSet rs = database.query("sql",
          "select $a[0].keys() as k, $a[0].values() as v from D1 let $a = (select from V1 where n = 1)")) {
        final Result row = rs.next();
        assertThat(row.<Collection<String>>getProperty("k")).containsExactly("n");
        assertThat(row.<Collection<Object>>getProperty("v")).containsExactly(1);
      }
    }

    @Test
    void edgeRowOfASubQueryHasNoMetadataValues() {
      try (final ResultSet rs = database.query("sql",
          "select $a[0].keys() as k, $a[0].values() as v from D1 let $a = (select from R1)")) {
        final Result row = rs.next();
        assertThat(row.<Collection<String>>getProperty("k")).containsExactly("w");
        assertThat(row.<Collection<Object>>getProperty("v")).containsExactly(7);
      }
    }

    @Test
    void rowWithNoElementBehindItIsUnchanged() {
      try (final ResultSet rs = database.query("sql",
          "select $a[0].keys() as k, $a[0].values() as v from D1 let $a = (select n from D1)")) {
        final Result row = rs.next();
        assertThat(row.<Collection<String>>getProperty("k")).containsExactly("n");
        assertThat(row.<Collection<Object>>getProperty("v")).containsExactly(1);
      }
    }

    /**
     * The precedence half of the defect: the row overrides {@code s} of its element, and {@code getProperty("s")} says
     * {@code OVERRIDE}. The pair has to report the value the row has, in the slot of the name it lists.
     */
    @Test
    void overridingRowReportsItsOwnValueInTheSlotOfItsOwnName() {
      try (final ResultSet rs = database.query("sql",
          "select $a[0].s as s, $a[0].keys() as k, $a[0].values() as v from D1 let $a = (select *, 'OVERRIDE' as s from D1)")) {
        final Result row = rs.next();
        assertThat(row.<String>getProperty("s")).isEqualTo("OVERRIDE");

        final List<String> keys = new ArrayList<>(row.<Collection<String>>getProperty("k"));
        final List<Object> values = new ArrayList<>(row.<Collection<Object>>getProperty("v"));
        assertThat(values).hasSameSizeAs(keys);
        assertThat(keys).contains("n", "s");
        assertThat(values.get(keys.indexOf("s"))).as("the value in the slot of 's'").isEqualTo("OVERRIDE");
        assertThat(values.get(keys.indexOf("n"))).as("the value in the slot of 'n'").isEqualTo(1);
      }
    }

    /**
     * A document whose values are not scalars: the values are read by name off the converted view, so a list and an embedded
     * document must come back in the slot of their own name, as they do for the same document without the row in between.
     */
    @Test
    void documentWithListAndEmbeddedValuesKeepsEveryValueInTheSlotOfItsName() {
      database.getSchema().createDocumentType("D3");
      database.transaction(() -> database.command("sql", "insert into D3 set n = 1, tags = ['a', 'b'], emb = {'k': 'v'}"));

      for (final String receiver : new String[] { "@this", "$a[0]" }) {
        try (final ResultSet rs = database.query("sql",
            "select " + receiver + ".keys() as k, " + receiver + ".values() as v, " + receiver + ".emb as e, " + receiver
                + ".tags as t from D3 let $a = (select from D3)")) {
          final Result row = rs.next();
          final List<String> keys = new ArrayList<>(row.<Collection<String>>getProperty("k"));
          final List<Object> values = new ArrayList<>(row.<Collection<Object>>getProperty("v"));

          assertThat(keys).as(receiver).containsExactlyInAnyOrder("n", "tags", "emb");
          assertThat(values).as(receiver).hasSameSizeAs(keys);
          assertThat(values.get(keys.indexOf("n"))).as(receiver + " n").isEqualTo(1);
          assertThat(values.get(keys.indexOf("tags"))).as(receiver + " tags").isEqualTo(List.of("a", "b"));
          assertThat(values.get(keys.indexOf("emb"))).as(receiver + " emb").isNotNull();
          // and the nested values are the ones the receiver answers when asked for them by name
          assertThat(values.get(keys.indexOf("emb"))).as(receiver + " emb by name").isEqualTo(row.getProperty("e"));
          assertThat(values.get(keys.indexOf("tags"))).as(receiver + " tags by name").isEqualTo(row.getProperty("t"));
        }
      }
    }

    /** A property that holds null is still a property of the record: keys() and values() both count it, or neither does. */
    @Test
    void aNullValuedPropertyKeepsTheTwoSidesTheSameSize() {
      database.getSchema().createDocumentType("D4");
      database.transaction(() -> database.command("sql", "insert into D4 set n = 1, nothing = null"));

      for (final String receiver : new String[] { "@this", "$a[0]" }) {
        try (final ResultSet rs = database.query("sql",
            "select " + receiver + ".keys() as k, " + receiver + ".values() as v from D4 let $a = (select from D4)")) {
          final Result row = rs.next();
          final List<String> keys = new ArrayList<>(row.<Collection<String>>getProperty("k"));
          final List<Object> values = new ArrayList<>(row.<Collection<Object>>getProperty("v"));

          assertThat(values).as(receiver).hasSameSizeAs(keys);
          assertThat(values.get(keys.indexOf("n"))).as(receiver + " n").isEqualTo(1);
        }
      }
    }

    /** A collection of rows flattens each receiver through the same branch, so the aligned pair holds for it too. */
    @Test
    void collectionOfRowsFlattensAlignedPairs() {
      try (final ResultSet rs = database.query("sql",
          "select $a.keys() as k, $a.values() as v from D1 let $a = (select from D1)")) {
        final Result row = rs.next();
        final Collection<Object> keys = row.getProperty("k");
        final Collection<Object> values = row.getProperty("v");
        assertThat(keys).containsExactly("n", "s");
        assertThat(values).containsExactly(1, "x");
      }
    }
  }
}
