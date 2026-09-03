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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.engine.Bucket;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6935: {@code removeSuperType} withdrew only the type's OWN buckets from the former
 * ancestor's polymorphic cache, while linking had contributed the whole polymorphic subtree. A grandchild's buckets
 * therefore stayed in the ancestor after the link was severed, and {@code SELECT FROM <ancestor>} kept returning the
 * grandchild's records. A two-level hierarchy hid it because for a leaf type the two lists coincide.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6935RemoveSuperTypeGrandchildTest extends TestHelper {

  @Test
  void removingTheMiddleLinkWithdrawsTheGrandchildFromTheAncestor() {
    database.command("sql", "CREATE DOCUMENT TYPE A2");
    database.command("sql", "CREATE DOCUMENT TYPE B2 EXTENDS A2");
    database.command("sql", "CREATE DOCUMENT TYPE C2 EXTENDS B2");
    database.transaction(() -> database.command("sql", "INSERT INTO C2 SET n = 1"));

    final DocumentType a2 = database.getSchema().getType("A2");
    final DocumentType b2 = database.getSchema().getType("B2");
    final DocumentType c2 = database.getSchema().getType("C2");
    final List<Bucket> c2Buckets = c2.getBuckets(false);

    assertThat(count("A2")).isEqualTo(1);
    assertThat(a2.getBuckets(true)).containsAll(c2Buckets);

    b2.removeSuperType("A2");

    assertThat(a2.getSubTypes()).isEmpty();
    assertThat(b2.getSuperTypes()).isEmpty();
    assertThat(a2.getBuckets(true)).as("A2 has no relationship with C2 any more").doesNotContainAnyElementsOf(c2Buckets);
    assertThat(a2.getBucketIds(true)).doesNotContainAnyElementsOf(c2.getBucketIds(false));
    assertThat(count("A2")).as("a polymorphic scan of A2 must not read C2's bucket").isZero();

    // B2 STILL SEES ITS OWN SUBTREE
    assertThat(b2.getBuckets(true)).containsAll(c2Buckets);
    assertThat(count("B2")).isEqualTo(1);
    assertThat(count("C2")).isEqualTo(1);

    // AND RE-LINKING BRINGS THE WHOLE SUBTREE BACK, ONCE
    b2.addSuperType("A2");
    assertThat(a2.getBuckets(true)).containsAll(c2Buckets).doesNotHaveDuplicates();
    assertThat(count("A2")).isEqualTo(1);
  }

  @Test
  void removingOneLinkOfADiamondKeepsTheBucketsStillReachableThroughTheOther() {
    // C3 EXTENDS BOTH B3 AND A3, AND B3 EXTENDS A3: SEVERING B3 -> A3 MUST NOT BLIND A3 TO C3, WHICH IS STILL ITS SUBTYPE
    database.command("sql", "CREATE DOCUMENT TYPE A3");
    database.command("sql", "CREATE DOCUMENT TYPE B3 EXTENDS A3");
    database.command("sql", "CREATE DOCUMENT TYPE C3 EXTENDS B3, A3");
    database.transaction(() -> database.command("sql", "INSERT INTO C3 SET n = 1"));

    final DocumentType a3 = database.getSchema().getType("A3");
    final DocumentType b3 = database.getSchema().getType("B3");
    final DocumentType c3 = database.getSchema().getType("C3");

    assertThat(a3.getBuckets(true)).containsAll(c3.getBuckets(false)).doesNotHaveDuplicates();
    assertThat(count("A3")).isEqualTo(1);

    b3.removeSuperType("A3");

    assertThat(a3.getSubTypes()).containsExactly(c3);
    assertThat(a3.getBuckets(true)).as("C3 still extends A3 directly").containsAll(c3.getBuckets(false));
    assertThat(a3.getBuckets(true)).doesNotContainAnyElementsOf(b3.getBuckets(false));
    assertThat(count("A3")).isEqualTo(1);

    c3.removeSuperType("A3");
    assertThat(a3.getBuckets(true)).doesNotContainAnyElementsOf(c3.getBuckets(false));
    assertThat(count("A3")).isZero();
  }

  @Test
  void theCacheSurvivesAReopen() {
    database.command("sql", "CREATE DOCUMENT TYPE A4");
    database.command("sql", "CREATE DOCUMENT TYPE B4 EXTENDS A4");
    database.command("sql", "CREATE DOCUMENT TYPE C4 EXTENDS B4");
    database.transaction(() -> database.command("sql", "INSERT INTO C4 SET n = 1"));

    database.getSchema().getType("B4").removeSuperType("A4");
    assertThat(count("A4")).isZero();

    reopenDatabase();
    assertThat(count("A4")).isZero();
    assertThat(count("B4")).isEqualTo(1);
  }

  private long count(final String typeName) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + typeName)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }
}
