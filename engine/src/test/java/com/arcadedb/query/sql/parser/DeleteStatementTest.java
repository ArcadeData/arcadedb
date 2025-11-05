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
package com.arcadedb.query.sql.parser;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.CollectionUtils;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class DeleteStatementTest extends TestHelper {

  public DeleteStatementTest() {
    autoStartTx = true;
  }

  @Test
  public void deleteFromSubqueryWithWhereTest() {
    database.command("sql", "create document type Foo");
    database.command("sql", "create document type Bar");
    final MutableDocument doc1 = database.newDocument("Foo").set("k", "key1");
    final MutableDocument doc2 = database.newDocument("Foo").set("k", "key2");
    final MutableDocument doc3 = database.newDocument("Foo").set("k", "key3");

    doc1.save();
    doc2.save();
    doc3.save();

    final List<Document> list = new ArrayList<>();
    list.add(doc1);
    list.add(doc2);
    list.add(doc3);
    final MutableDocument bar = database.newDocument("Bar").set("arr", list);
    bar.save();

    database.command("sql", "delete from (select expand(arr) from Bar) where k = 'key2'");

    final ResultSet result = database.query("sql", "select from Foo");
    assertThat(Optional.ofNullable(result)).isNotNull();
    assertThat(CollectionUtils.countEntries(result)).isEqualTo(2);
    for (final ResultSet it = result; it.hasNext(); ) {
      final Document doc = it.next().toElement();
      assertThat(doc.getString("k")).isNotEqualTo("key2");
    }
    database.commit();
  }

  @Test
  public void testDeleteWithMultipleOrConditions() {
    // Reproduction test for issue #2695
    // DELETE with multiple OR conditions should delete all matching records
    database.command("sql", "create vertex type Duct");
    database.command("sql", "create edge type HierarchyDuctDuct");
    database.command("sql", "create property HierarchyDuctDuct.internal_from STRING");
    database.command("sql", "create property HierarchyDuctDuct.internal_to STRING");
    database.command("sql", "create property HierarchyDuctDuct.swap BOOLEAN");
    database.command("sql", "create property HierarchyDuctDuct.order_number INTEGER");

    // Create parent and child vertices
    final var parent = database.newVertex("Duct").set("name", "parent").save();
    final var child1 = database.newVertex("Duct").set("name", "child1").save();
    final var child2 = database.newVertex("Duct").set("name", "child2").save();
    final var child3 = database.newVertex("Duct").set("name", "child3").save();
    final var child4 = database.newVertex("Duct").set("name", "child4").save();
    final var child5 = database.newVertex("Duct").set("name", "child5").save();
    final var child6 = database.newVertex("Duct").set("name", "child6").save();

    // Create 6 edges with different property combinations
    parent.newEdge("HierarchyDuctDuct", child1)
        .set("internal_from", "A").set("internal_to", "B").set("swap", false).set("order_number", 1).save();
    parent.newEdge("HierarchyDuctDuct", child2)
        .set("internal_from", "A").set("internal_to", "C").set("swap", false).set("order_number", 2).save();
    parent.newEdge("HierarchyDuctDuct", child3)
        .set("internal_from", "A").set("internal_to", "D").set("swap", false).set("order_number", 3).save();
    parent.newEdge("HierarchyDuctDuct", child4)
        .set("internal_from", "A").set("internal_to", "E").set("swap", false).set("order_number", 4).save();
    parent.newEdge("HierarchyDuctDuct", child5)
        .set("internal_from", "A").set("internal_to", "F").set("swap", false).set("order_number", 5).save();
    parent.newEdge("HierarchyDuctDuct", child6)
        .set("internal_from", "A").set("internal_to", "G").set("swap", false).set("order_number", 6).save();

    // Verify we have 6 edges
    ResultSet result = database.query("sql", "select from HierarchyDuctDuct");
    assertThat(CollectionUtils.countEntries(result)).isEqualTo(6);

    // Delete using multiple OR conditions (this should delete all 6 edges)
    final String deleteQuery = "delete from HierarchyDuctDuct where " +
        "((internal_from = 'A') AND (internal_to = 'B') AND (swap = false) AND (order_number = 1)) OR " +
        "((internal_from = 'A') AND (internal_to = 'C') AND (swap = false) AND (order_number = 2)) OR " +
        "((internal_from = 'A') AND (internal_to = 'D') AND (swap = false) AND (order_number = 3)) OR " +
        "((internal_from = 'A') AND (internal_to = 'E') AND (swap = false) AND (order_number = 4)) OR " +
        "((internal_from = 'A') AND (internal_to = 'F') AND (swap = false) AND (order_number = 5)) OR " +
        "((internal_from = 'A') AND (internal_to = 'G') AND (swap = false) AND (order_number = 6))";

    final ResultSet deleteResult = database.command("sql", deleteQuery);
    final long deletedCount = deleteResult.next().getProperty("count");

    // Should delete all 6 edges, not just 1
    assertThat(deletedCount).isEqualTo(6);

    // Verify all edges are deleted
    result = database.query("sql", "select from HierarchyDuctDuct");
    assertThat(CollectionUtils.countEntries(result)).isEqualTo(0);

    database.commit();
  }

  @Test
  public void testDeleteWithMultipleOrConditionsAndIndex() {
    // Test for issue #2695 with indexed property
    // Ensures the fix works when some OR branches can use indexes
    database.command("sql", "create document type Product");
    database.command("sql", "create property Product.id INTEGER");
    database.command("sql", "create property Product.category STRING");
    database.command("sql", "create index on Product (id) UNIQUE");

    // Create test documents
    for (int i = 1; i <= 6; i++) {
      database.newDocument("Product")
          .set("id", i)
          .set("category", "cat" + i)
          .save();
    }

    // Verify we have 6 documents
    ResultSet result = database.query("sql", "select from Product");
    assertThat(CollectionUtils.countEntries(result)).isEqualTo(6);

    // Delete using OR with indexed field - all branches should be processed
    final String deleteQuery = "delete from Product where " +
        "(id = 1 AND category = 'cat1') OR " +
        "(id = 2 AND category = 'cat2') OR " +
        "(id = 3 AND category = 'cat3') OR " +
        "(id = 4 AND category = 'cat4') OR " +
        "(id = 5 AND category = 'cat5') OR " +
        "(id = 6 AND category = 'cat6')";

    final ResultSet deleteResult = database.command("sql", deleteQuery);
    final long deletedCount = deleteResult.next().getProperty("count");

    // Should delete all 6 documents, not just 1
    assertThat(deletedCount).isEqualTo(6);

    // Verify all documents are deleted
    result = database.query("sql", "select from Product");
    assertThat(CollectionUtils.countEntries(result)).isEqualTo(0);

    database.commit();
  }

  protected SqlParser getParserFor(final String string) {
    final InputStream is = new ByteArrayInputStream(string.getBytes());
    final SqlParser osql = new SqlParser(null, is);
    return osql;
  }

  protected SimpleNode checkRightSyntax(final String query) {
    return checkSyntax(query, true);
  }

  protected SimpleNode checkWrongSyntax(final String query) {
    return checkSyntax(query, false);
  }

  protected SimpleNode checkSyntax(final String query, final boolean isCorrect) {
    final SqlParser osql = getParserFor(query);
    try {
      final SimpleNode result = osql.Parse();
      if (!isCorrect) {
        fail("");
      }
      return result;
    } catch (final Exception e) {
      if (isCorrect) {
        e.printStackTrace();
        fail("");
      }
    }
    return null;
  }

}
