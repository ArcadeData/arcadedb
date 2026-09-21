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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * https://github.com/ArcadeData/arcadedb/issues/8042
 * <p>
 * {@code TruncateTypeStatement.executeDDL} refuses to truncate a non-empty graph type unless {@code UNSAFE} is
 * given, and that refusal is the only thing between {@code TRUNCATE TYPE Person} and the silent emptying of a live
 * vertex type. It could not fire: the test was {@code typez.isSubTypeOf("V")}/{@code isSubTypeOf("E")}, which
 * matches by NAME - this type's own, then recursively its super types' - and ArcadeDB has no implicit root types
 * called {@code V} and {@code E} (that is an OrientDB inheritance). {@code CREATE VERTEX TYPE Person} builds a
 * {@code VertexType} with an EMPTY super-type list, so for every vertex and edge type a user actually creates both
 * tests answered false and the truncate proceeded with the documented {@code UNSAFE} escape hatch never demanded.
 * <p>
 * The hierarchies below deliberately contain NO type called {@code V} or {@code E}, which is what the #7919
 * regression test's own hierarchy relies on and why it went on passing over a dead check.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue8042TruncateTypeGraphGuardTest extends TestHelper {

  @Test
  void aNonEmptyVertexTypeIsRefusedWithoutUnsafe() {
    database.command("sql", "CREATE VERTEX TYPE Person").close();
    database.transaction(() -> database.command("sql", "INSERT INTO Person SET name = 'a'").close());

    assertThatThrownBy(() -> database.command("sql", "TRUNCATE TYPE Person").close())
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("not empty vertex classes")
        .hasMessageContaining("UNSAFE");

    assertThat(count("Person")).isEqualTo(1);
  }

  @Test
  void aNonEmptyEdgeTypeIsRefusedWithoutUnsafe() {
    database.command("sql", "CREATE VERTEX TYPE Person").close();
    database.command("sql", "CREATE EDGE TYPE Knows").close();
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Person SET name = 'a'").close();
      database.command("sql", "INSERT INTO Person SET name = 'b'").close();
      database.command("sql",
          "CREATE EDGE Knows FROM (SELECT FROM Person WHERE name = 'a') TO (SELECT FROM Person WHERE name = 'b')").close();
    });

    assertThatThrownBy(() -> database.command("sql", "TRUNCATE TYPE Knows").close())
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("not empty edge classes")
        .hasMessageContaining("UNSAFE");

    assertThat(count("Knows")).isEqualTo(1);
    assertThat(count("Person")).isEqualTo(2);
  }

  @Test
  void unsafeStillTruncatesAVertexType() {
    // THE ESCAPE HATCH THE MESSAGE NAMES HAS TO KEEP WORKING - THAT IS WHAT MAKES THE REFUSAL ACCEPTABLE
    database.command("sql", "CREATE VERTEX TYPE Person").close();
    database.transaction(() -> database.command("sql", "INSERT INTO Person SET name = 'a'").close());

    database.command("sql", "TRUNCATE TYPE Person UNSAFE").close();

    assertThat(count("Person")).isZero();
  }

  @Test
  void anEmptyVertexTypeIsStillTruncatedWithoutUnsafe() {
    // THE GUARD IS KEYED ON recs > 0: AN EMPTY TYPE HAS NOTHING TO LOSE AND MUST NOT START BEING REFUSED
    database.command("sql", "CREATE VERTEX TYPE Person").close();

    database.command("sql", "TRUNCATE TYPE Person").close();

    assertThat(count("Person")).isZero();
  }

  @Test
  void aNonEmptyDocumentTypeIsStillTruncatedWithoutUnsafe() {
    // THE GUARD IS ABOUT GRAPH TYPES ONLY: A PLAIN DOCUMENT TYPE HAS NO EDGES TO STRAND
    database.command("sql", "CREATE DOCUMENT TYPE Doc").close();
    database.transaction(() -> database.command("sql", "INSERT INTO Doc SET name = 'a'").close());

    database.command("sql", "TRUNCATE TYPE Doc").close();

    assertThat(count("Doc")).isZero();
  }

  @Test
  void aPolymorphicTruncateIsRefusedForANonEmptyVertexSubtype() {
    // THE HALF #7919 REWROTE: THE COUNT IT CORRECTED FEEDS THIS CHECK, AND THE CHECK WAS DEAD. NO TYPE IN THIS
    // HIERARCHY IS CALLED V, WHICH IS EXACTLY WHAT #7919's OWN TEST RELIED ON
    database.command("sql", "CREATE VERTEX TYPE VBase").close();
    database.command("sql", "CREATE VERTEX TYPE VSub EXTENDS VBase").close();
    database.command("sql", "CREATE VERTEX TYPE VGrandSub EXTENDS VSub").close();
    database.transaction(() -> database.command("sql", "INSERT INTO VGrandSub SET x = 1").close());

    // THE ROOT'S OWN GUARD ANSWERS FIRST HERE, BECAUSE A POLYMORPHIC countType() ALREADY SEES THE GRANDCHILD'S
    // RECORD. THAT IS THE POINT: BEFORE THE FIX NEITHER THIS GUARD NOR THE PER-SUBTYPE LOOP BELOW IT COULD FIRE AT
    // ALL AND THE WHOLE SUBTREE WAS EMPTIED IN SILENCE
    assertThatThrownBy(() -> database.command("sql", "TRUNCATE TYPE VBase POLYMORPHIC").close())
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("not empty vertex classes");

    assertThat(count("VGrandSub")).isEqualTo(1);

    database.command("sql", "TRUNCATE TYPE VBase POLYMORPHIC UNSAFE").close();
    assertThat(count("VGrandSub")).isZero();
  }

  @Test
  void theSubtypeLoopNamesTheNonEmptyVertexSubtypeUnderADocumentRoot() {
    // THE ONE SHAPE THAT REACHES THE PER-SUBTYPE LOOP #7919 REWROTE: A DOCUMENT ROOT PASSES ITS OWN GUARD (IT IS
    // NEITHER A VertexType NOR AN EdgeType, WHATEVER ITS POLYMORPHIC COUNT), SO THE LOOP IS WHAT HAS TO REFUSE ON
    // BEHALF OF THE GRAPH SUBTYPE UNDER IT - AND IT NAMES IT
    database.command("sql", "CREATE DOCUMENT TYPE DocRoot").close();
    database.command("sql", "CREATE VERTEX TYPE VChild EXTENDS DocRoot").close();
    database.transaction(() -> database.command("sql", "INSERT INTO VChild SET x = 1").close());

    assertThatThrownBy(() -> database.command("sql", "TRUNCATE TYPE DocRoot POLYMORPHIC").close())
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("not empty vertex classes")
        .hasMessageContaining("VChild");

    assertThat(count("VChild")).isEqualTo(1);
  }

  @Test
  void aGraphLeafIsFoundBehindADocumentTypeIntermediateNode() {
    // getSubTypes() answers DIRECT children only, so a guard that walks one level down is blind to
    // DocRoot -> DocMiddle -> VLeaf: neither the root nor DocMiddle is a graph type, so neither names a reason to
    // refuse, while the polymorphic scanType() below happily deletes VLeaf's records. The scope of a POLYMORPHIC
    // truncate is the whole subtree, so the guard has to read the whole subtree (CodeRabbit on PR #8094).
    database.command("sql", "CREATE DOCUMENT TYPE DocRoot").close();
    database.command("sql", "CREATE DOCUMENT TYPE DocMiddle EXTENDS DocRoot").close();
    database.command("sql", "CREATE VERTEX TYPE VLeaf EXTENDS DocMiddle").close();
    database.transaction(() -> database.command("sql", "INSERT INTO VLeaf SET x = 1").close());

    assertThatThrownBy(() -> database.command("sql", "TRUNCATE TYPE DocRoot POLYMORPHIC").close())
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("not empty vertex classes")
        .hasMessageContaining("VLeaf");

    assertThat(count("VLeaf")).isEqualTo(1);

    database.command("sql", "TRUNCATE TYPE DocRoot POLYMORPHIC UNSAFE").close();
    assertThat(count("VLeaf")).isZero();
  }

  @Test
  void anEdgeLeafIsFoundBehindTwoDocumentTypeIntermediateNodes() {
    // The same shape one level deeper, and on the edge arm, so the recursion is pinned rather than a single extra
    // getSubTypes() hop.
    database.command("sql", "CREATE DOCUMENT TYPE DocRoot").close();
    database.command("sql", "CREATE DOCUMENT TYPE DocMiddle EXTENDS DocRoot").close();
    database.command("sql", "CREATE DOCUMENT TYPE DocLower EXTENDS DocMiddle").close();
    database.command("sql", "CREATE EDGE TYPE ELeaf EXTENDS DocLower").close();
    database.command("sql", "CREATE VERTEX TYPE Person").close();
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Person SET name = 'a'").close();
      database.command("sql", "INSERT INTO Person SET name = 'b'").close();
      database.command("sql",
          "CREATE EDGE ELeaf FROM (SELECT FROM Person WHERE name = 'a') TO (SELECT FROM Person WHERE name = 'b')").close();
    });

    assertThatThrownBy(() -> database.command("sql", "TRUNCATE TYPE DocRoot POLYMORPHIC").close())
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("not empty edge classes")
        .hasMessageContaining("ELeaf");

    assertThat(count("ELeaf")).isEqualTo(1);
  }

  @Test
  void anEmptyGraphAncestorIsNotBlamedForItsDescendantsRecords() {
    // A chain of TWO graph levels under a non-graph root. Counting each descendant polymorphically charged VLeaf's
    // record to the empty VMiddle above it, and the walk reaches VMiddle first, so the refusal named the one type
    // in the subtree that holds nothing (CodeRabbit on PR #8094). Refusing was right; naming VMiddle was not.
    database.command("sql", "CREATE DOCUMENT TYPE DocRoot").close();
    database.command("sql", "CREATE VERTEX TYPE VMiddle EXTENDS DocRoot").close();
    database.command("sql", "CREATE VERTEX TYPE VLeaf EXTENDS VMiddle").close();
    database.transaction(() -> database.command("sql", "INSERT INTO VLeaf SET x = 1").close());

    assertThat(count("VMiddle")).as("the intermediate holds nothing of its own").isEqualTo(1);
    assertThat(database.countType("VMiddle", false)).isZero();

    assertThatThrownBy(() -> database.command("sql", "TRUNCATE TYPE DocRoot POLYMORPHIC").close())
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("not empty vertex classes")
        .hasMessageContaining("VLeaf")
        .hasMessageNotContaining("VMiddle");
  }

  @Test
  void anEmptyLightweightAncestorIsNotBlamedForItsSubtypesEdges() {
    // The lightweight twin of the case above. A LIGHTWEIGHT edge allocates no record, so countType() answers 0 for
    // it however many edges it holds (#7477) and only a count(*) walk can see them - but count(*) is
    // unconditionally polymorphic, so the empty LWAnc absorbed LWSub's edge and, being reached first, was named
    // (CodeRabbit on PR #8094). A record-backed sibling is in the same subtree to pin that the two count paths
    // agree on which type to blame.
    database.command("sql", "CREATE DOCUMENT TYPE DocRoot").close();
    database.command("sql", "CREATE VERTEX TYPE Person").close();
    database.command("sql", "CREATE EDGE TYPE LWAnc EXTENDS DocRoot LIGHTWEIGHT").close();
    database.command("sql", "CREATE EDGE TYPE LWSub EXTENDS LWAnc LIGHTWEIGHT").close();
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Person SET name = 'a'").close();
      database.command("sql", "INSERT INTO Person SET name = 'b'").close();
      database.command("sql",
          "CREATE EDGE LWSub FROM (SELECT FROM Person WHERE name = 'a') TO (SELECT FROM Person WHERE name = 'b')").close();
    });

    assertThat(count("LWAnc")).as("polymorphically the ancestor sees its subtype's edge").isEqualTo(1);

    assertThatThrownBy(() -> database.command("sql", "TRUNCATE TYPE DocRoot POLYMORPHIC").close())
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("not empty edge classes")
        .hasMessageContaining("LWSub")
        .hasMessageNotContaining("LWAnc");
  }

  @Test
  void anEmptyLightweightAncestorWithARecordBackedSubtypeNamesTheRecordBackedOne() {
    // Same shape, but the non-empty subtype is RECORD-BACKED under a lightweight ancestor: a type's own LIGHTWEIGHT
    // flag is independent of its parent's, so this is a legal hierarchy and the two count paths have to agree on
    // which type to name.
    database.command("sql", "CREATE DOCUMENT TYPE DocRoot").close();
    database.command("sql", "CREATE VERTEX TYPE Person").close();
    database.command("sql", "CREATE EDGE TYPE LWAnc EXTENDS DocRoot LIGHTWEIGHT").close();
    database.command("sql", "CREATE EDGE TYPE HeavySub EXTENDS LWAnc").close();
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Person SET name = 'a'").close();
      database.command("sql", "INSERT INTO Person SET name = 'b'").close();
      database.command("sql",
          "CREATE EDGE HeavySub FROM (SELECT FROM Person WHERE name = 'a') TO (SELECT FROM Person WHERE name = 'b')").close();
    });

    assertThatThrownBy(() -> database.command("sql", "TRUNCATE TYPE DocRoot POLYMORPHIC").close())
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("not empty edge classes")
        .hasMessageContaining("HeavySub")
        .hasMessageNotContaining("LWAnc");
  }

  @Test
  void anEmptyGraphLeafBehindADocumentIntermediateIsNotBlamed() {
    // The recursion must not start refusing a subtree that holds nothing: only a non-empty graph descendant is a
    // reason to demand UNSAFE.
    database.command("sql", "CREATE DOCUMENT TYPE DocRoot").close();
    database.command("sql", "CREATE DOCUMENT TYPE DocMiddle EXTENDS DocRoot").close();
    database.command("sql", "CREATE VERTEX TYPE VLeaf EXTENDS DocMiddle").close();
    database.transaction(() -> database.command("sql", "INSERT INTO DocRoot SET x = 1").close());

    database.command("sql", "TRUNCATE TYPE DocRoot POLYMORPHIC").close();

    assertThat(count("DocRoot")).isZero();
  }

  @Test
  void truncateTypeAndTruncateBucketNowAgreeOnTheSameVertexType() {
    // THE SHORTEST STATEMENT OF THE WHOLE FINDING: TRUNCATE BUCKET REFUSED WHERE TRUNCATE TYPE DID NOT, ON THE SAME
    // DATA, WITH THE SAME MESSAGE TEMPLATE
    database.command("sql", "CREATE VERTEX TYPE Person BUCKETS 1").close();
    database.transaction(() -> database.command("sql", "INSERT INTO Person SET name = 'a'").close());

    final String bucketName = database.getSchema().getType("Person").getBuckets(false).getFirst().getName();

    assertThatThrownBy(() -> database.command("sql", "TRUNCATE BUCKET `" + bucketName + "`").close())
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("not empty vertex bucket");
    assertThatThrownBy(() -> database.command("sql", "TRUNCATE TYPE Person").close())
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("not empty vertex classes");

    assertThat(count("Person")).isEqualTo(1);
  }

  private long count(final String typeName) {
    return ((Number) database.query("sql", "SELECT count(*) AS c FROM `" + typeName + "`").nextIfAvailable()
        .getProperty("c")).longValue();
  }
}
