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
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.graph.Vertex;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #7919: {@code TRUNCATE TYPE}'s non-POLYMORPHIC guard read the CONFLATED
 * {@code EdgeType.holdsLightweightEdges()}, which answers true when the type <b>or anything below it</b> is
 * lightweight. So a heavyweight edge type that merely has a lightweight descendant had a plain
 * {@code TRUNCATE TYPE Follows} refused, with a message that is false for that shape - it claimed the type has "no
 * bucket of its own", when a record-backed root has exactly that, and a non-polymorphic truncate would clear it and
 * never touch the lightweight subtype.
 * <p>
 * Two more lines in the same method are pinned here: the safety count keyed on the same conflated flag (which would
 * have run an unconditionally-polymorphic {@code count(*)} for a non-polymorphic truncate and refused on records
 * outside the scope), and the per-subtype loop, which counted the ROOT on every iteration instead of the subtype,
 * so the check meant to refuse a polymorphic truncate over a non-empty subtype never looked at a subtype at all.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7919TruncateNonPolymorphicGuardTest extends TestHelper {

  @Test
  void nonPolymorphicTruncateOfAHeavyweightRootWithALightweightSubtypeIsAllowed() {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE EDGE TYPE Follows");
    database.command("sql", "CREATE EDGE TYPE FollowsLite EXTENDS Follows LIGHTWEIGHT");

    final RID p1, p2, p3, p4;
    database.begin();
    try {
      p1 = database.newVertex("Person").set("name", "p1").save().getIdentity();
      p2 = database.newVertex("Person").set("name", "p2").save().getIdentity();
      p3 = database.newVertex("Person").set("name", "p3").save().getIdentity();
      p4 = database.newVertex("Person").set("name", "p4").save().getIdentity();

      database.lookupByRID(p1, true).asVertex().modify().newEdge("Follows", p2).set("since", "2020").save();
      database.lookupByRID(p3, true).asVertex().modify().newEdge("FollowsLite", p4);
    } finally {
      database.commit();
    }

    assertThat(database.countType("Follows", false)).isEqualTo(1);
    assertThat(database.lookupByRID(p3, true).asVertex().countEdges(Vertex.DIRECTION.OUT, "FollowsLite")).isEqualTo(1);

    // Used to be refused outright: Follows is record-backed, so it has a bucket of its own to scope this to.
    database.command("sql", "TRUNCATE TYPE Follows UNSAFE").close();

    assertThat(database.countType("Follows", false)).as("the root's own bucket is cleared").isEqualTo(0);
    assertThat(database.lookupByRID(p3, true).asVertex().countEdges(Vertex.DIRECTION.OUT, "FollowsLite"))
        .as("a non-polymorphic truncate must not reach into the lightweight subtype")
        .isEqualTo(1);
  }

  /**
   * The count at the heart of the same guard. A non-polymorphic truncate of a heavyweight root must be gated on the
   * root's own records: routing it through the {@code count(*)} push-down, which is unconditionally polymorphic,
   * would refuse an empty root because a subtype below it holds edges the statement is not going to touch.
   */
  @Test
  void nonPolymorphicTruncateOfAnEmptyHeavyweightRootIsNotRefusedForItsLightweightSubtypesEdges() {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE EDGE TYPE Follows");
    database.command("sql", "CREATE EDGE TYPE FollowsLite EXTENDS Follows LIGHTWEIGHT");

    final RID p1, p2;
    database.begin();
    try {
      p1 = database.newVertex("Person").set("name", "p1").save().getIdentity();
      p2 = database.newVertex("Person").set("name", "p2").save().getIdentity();
      database.lookupByRID(p1, true).asVertex().modify().newEdge("FollowsLite", p2);
    } finally {
      database.commit();
    }

    // No UNSAFE: the safety count must see the root's own zero records, not the subtree's one edge.
    database.command("sql", "TRUNCATE TYPE Follows").close();

    assertThat(database.lookupByRID(p1, true).asVertex().countEdges(Vertex.DIRECTION.OUT, "FollowsLite"))
        .as("out of scope for a non-polymorphic truncate, so still there")
        .isEqualTo(1);
  }

  /**
   * The shape the guard exists for is still refused: a LIGHTWEIGHT root has no bucket, so a non-polymorphic
   * TRUNCATE could only be served by the unconditionally-polymorphic {@code DELETE FROM}, which would reach into
   * the subtype the caller did not ask for.
   */
  @Test
  void nonPolymorphicTruncateOfALightweightRootWithASubtypeIsStillRefused() {
    database.command("sql", "CREATE EDGE TYPE Knows LIGHTWEIGHT");
    database.command("sql", "CREATE EDGE TYPE StrongKnows EXTENDS Knows");

    assertThatThrownBy(() -> database.command("sql", "TRUNCATE TYPE Knows"))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("without POLYMORPHIC because it is a LIGHTWEIGHT edge type with a subtype")
        .hasMessageContaining("TRUNCATE TYPE Knows POLYMORPHIC");
  }

  /**
   * A lightweight type with no subtype at all has no ambiguity to refuse, whichever way POLYMORPHIC was spelled.
   */
  @Test
  void nonPolymorphicTruncateOfALightweightLeafStillWorks() {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE EDGE TYPE Knows LIGHTWEIGHT");

    final RID p1, p2;
    database.begin();
    try {
      p1 = database.newVertex("Person").set("name", "p1").save().getIdentity();
      p2 = database.newVertex("Person").set("name", "p2").save().getIdentity();
      database.lookupByRID(p1, true).asVertex().modify().newEdge("Knows", p2);
    } finally {
      database.commit();
    }

    database.command("sql", "TRUNCATE TYPE Knows UNSAFE").close();

    assertThat(database.lookupByRID(p1, true).asVertex().countEdges(Vertex.DIRECTION.OUT, "Knows")).isEqualTo(0);
  }

  /**
   * The per-subtype loop (line :122 of the issue). Reaching it needs a root that is NOT itself under {@code E} while
   * a subtype is - the "multiple inheritance" case the loop's own comment names - because otherwise the whole-subtree
   * check above it throws first. With the root counted instead of the subtype, the loop saw the root's zero and let a
   * polymorphic truncate of a non-empty subtype through without the UNSAFE keyword it was written to demand.
   */
  @Test
  void thePerSubtypeSafetyCountLooksAtTheSubtypeNotTheRoot() {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE EDGE TYPE E");
    database.command("sql", "CREATE EDGE TYPE Base");
    database.command("sql", "CREATE EDGE TYPE Sub EXTENDS Base, E");

    final RID p1, p2;
    database.begin();
    try {
      p1 = database.newVertex("Person").set("name", "p1").save().getIdentity();
      p2 = database.newVertex("Person").set("name", "p2").save().getIdentity();
      database.lookupByRID(p1, true).asVertex().modify().newEdge("Sub", p2).save();
    } finally {
      database.commit();
    }

    assertThat(database.countType("Base", false)).as("the root itself holds nothing").isZero();
    assertThat(database.countType("Sub", false)).isEqualTo(1);

    assertThatThrownBy(() -> database.command("sql", "TRUNCATE TYPE Base POLYMORPHIC"))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("not empty edge classes (Sub)");

    // still there: the refusal must have stopped the truncate, not merely reported on it afterwards
    assertThat(database.countType("Sub", false)).isEqualTo(1);

    database.command("sql", "TRUNCATE TYPE Base POLYMORPHIC UNSAFE").close();
    assertThat(database.countType("Sub", false)).isZero();
  }

  /**
   * The other half of the same typo: an EMPTY subtype under a non-empty root was reported BY NAME as the reason for
   * a refusal, because the count that triggered it was the root's.
   */
  @Test
  void anEmptySubtypeIsNotBlamedForTheRootsRecords() {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE EDGE TYPE E");
    database.command("sql", "CREATE EDGE TYPE Base");
    database.command("sql", "CREATE EDGE TYPE Sub EXTENDS Base, E");

    final RID p1, p2;
    database.begin();
    try {
      p1 = database.newVertex("Person").set("name", "p1").save().getIdentity();
      p2 = database.newVertex("Person").set("name", "p2").save().getIdentity();
      database.lookupByRID(p1, true).asVertex().modify().newEdge("Base", p2).save();
    } finally {
      database.commit();
    }

    assertThat(database.countType("Base", false)).isEqualTo(1);
    assertThat(database.countType("Sub", false)).isZero();

    // Base is not under E, so the whole-subtree check does not refuse; the loop must not refuse on Sub's behalf
    // either, because Sub is empty.
    database.command("sql", "TRUNCATE TYPE Base POLYMORPHIC").close();

    assertThat(database.countType("Base", true)).isZero();
  }
}
