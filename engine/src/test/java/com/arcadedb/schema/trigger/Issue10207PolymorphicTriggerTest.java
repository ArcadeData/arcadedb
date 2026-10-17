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
package com.arcadedb.schema.trigger;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.event.AfterRecordCreateListener;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10207: a trigger declared {@code ON TYPE P} fired only for records whose type was exactly {@code P}, so a record of
 * {@code S EXTENDS P} - which {@code SELECT FROM P} returns - skipped it, and a BEFORE trigger validating writes to {@code P}
 * could be bypassed by writing to a subtype. A trigger now covers its type's subtypes, Cypher composite types included, while a
 * listener registered from Java on {@code type.getEvents()} keeps firing for its exact type only.
 * <p>
 * Two more defects from the same report: {@code INSERT INTO P FROM SELECT ...} saved each row twice, so it fired the UPDATE
 * triggers of every row it created; and the write of a vertex's edge list head pointer, done when the vertex gets its first edge
 * of a direction or a new head segment, was announced as an update of the vertex - firing its UPDATE triggers now and then on an
 * edge creation, and letting a BEFORE UPDATE veto drop the pointer, which left the new edge an orphan.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10207PolymorphicTriggerTest extends TestHelper {
  static final AtomicInteger BEFORE_READS = new AtomicInteger();
  static final AtomicInteger AFTER_READS  = new AtomicInteger();

  public static class CountingBeforeRead implements JavaTrigger {
    @Override
    public boolean execute(final Database database, final Record record, final Record oldRecord) {
      return true;
    }

    @Override
    public boolean executeBeforeRead(final Database database, final RID rid) {
      BEFORE_READS.incrementAndGet();
      return true;
    }
  }

  public static class CountingAfterRead implements JavaTrigger {
    @Override
    public boolean execute(final Database database, final Record record, final Record oldRecord) {
      AFTER_READS.incrementAndGet();
      return true;
    }
  }

  @BeforeEach
  void createSchema() {
    BEFORE_READS.set(0);
    AFTER_READS.set(0);
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE VERTEX TYPE S EXTENDS P");
    database.command("sql", "CREATE DOCUMENT TYPE Audit");
    database.command("sql", "CREATE DOCUMENT TYPE Src");
    database.command("sql", "CREATE EDGE TYPE E");
  }

  @Test
  void afterCreateOnSuperTypeFiresForSubtypeRecordsThroughEveryApi() {
    auditTrigger("ta", "AFTER CREATE", "P", "create");

    database.transaction(() -> database.command("sql", "INSERT INTO P SET x = 1"));
    database.transaction(() -> database.command("sql", "INSERT INTO S SET x = 2"));
    database.transaction(() -> database.newVertex("S").set("x", 3).save());
    database.transaction(() -> database.command("opencypher", "CREATE (:S {x: 4})"));

    assertThat(count("SELECT count() AS c FROM P")).isEqualTo(4);
    assertThat(audits("create")).isEqualTo(4);
  }

  @Test
  void beforeCreateVetoOnSuperTypeCannotBeBypassedThroughASubtype() {
    database.command("sql",
        "CREATE TRIGGER guard BEFORE CREATE ON TYPE P EXECUTE JAVASCRIPT 'record.get(\"x\") == null || record.get(\"x\") >= 0'");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO S SET x = -1");
      database.newVertex("S").set("x", -2).save();
      database.command("sql", "INSERT INTO S SET x = 5");
    });

    assertThat(count("SELECT count() AS c FROM S")).isEqualTo(1);
  }

  @Test
  void updateAndDeleteOnSuperTypeFireForSubtypeRecords() {
    auditTrigger("tu", "AFTER UPDATE", "P", "update");
    auditTrigger("td", "BEFORE DELETE", "P", "delete");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO P SET x = 1");
      database.command("sql", "INSERT INTO S SET x = 2");
    });
    database.transaction(() -> database.command("sql", "UPDATE P SET y = 1"));
    assertThat(audits("update")).isEqualTo(2);

    database.transaction(() -> database.command("sql", "DELETE FROM P"));
    assertThat(audits("delete")).isEqualTo(2);
    assertThat(count("SELECT count() AS c FROM P")).isZero();
  }

  @Test
  void readTriggersOnSuperTypeFireForSubtypeRecords() {
    database.transaction(() -> {
      database.command("sql", "INSERT INTO P SET x = 1");
      database.command("sql", "INSERT INTO S SET x = 2");
    });
    database.command("sql", "CREATE TRIGGER br BEFORE READ ON TYPE P EXECUTE JAVA '" + CountingBeforeRead.class.getName() + "'");
    database.command("sql", "CREATE TRIGGER ar AFTER READ ON TYPE P EXECUTE JAVA '" + CountingAfterRead.class.getName() + "'");

    try (final ResultSet rs = database.query("sql", "SELECT FROM S")) {
      assertThat(rs.stream().count()).isEqualTo(1);
    }
    assertThat(BEFORE_READS.get()).isEqualTo(1);
    assertThat(AFTER_READS.get()).isEqualTo(1);
  }

  @Test
  void triggerOnSubtypeDoesNotFireForSuperTypeRecordsAndEachTriggerFiresOnce() {
    auditTrigger("onP", "AFTER CREATE", "P", "p");
    auditTrigger("onS", "AFTER CREATE", "S", "s");

    database.transaction(() -> database.command("sql", "INSERT INTO P SET x = 1"));
    assertThat(audits("p")).isEqualTo(1);
    assertThat(audits("s")).isZero();

    database.transaction(() -> database.command("sql", "INSERT INTO S SET x = 1"));
    assertThat(audits("p")).isEqualTo(2);
    assertThat(audits("s")).isEqualTo(1);
  }

  @Test
  void cypherCompositeTypeFiresTheTriggersOfEveryLabelOnceEvenThroughASharedParent() {
    database.command("sql", "CREATE VERTEX TYPE X");
    database.command("sql", "CREATE VERTEX TYPE A EXTENDS X");
    database.command("sql", "CREATE VERTEX TYPE B EXTENDS X");
    auditTrigger("onA", "AFTER CREATE", "A", "a");
    auditTrigger("onB", "AFTER CREATE", "B", "b");
    auditTrigger("onX", "AFTER CREATE", "X", "x");

    // Type A~B extends both A and B, and reaches X through each of them
    database.transaction(() -> database.command("opencypher", "CREATE (:A:B {v: 1})"));

    assertThat(audits("a")).isEqualTo(1);
    assertThat(audits("b")).isEqualTo(1);
    assertThat(audits("x")).isEqualTo(1);
  }

  @Test
  void theLineageFollowsTheTypeGraphAsItChanges() {
    auditTrigger("ta", "AFTER CREATE", "P", "create");

    // A subtype created after the trigger is covered by it
    database.command("sql", "CREATE VERTEX TYPE Late EXTENDS P");
    database.transaction(() -> database.command("sql", "INSERT INTO Late SET x = 1"));
    assertThat(audits("create")).isEqualTo(1);

    // ...and is no longer once it stops extending P
    database.command("sql", "ALTER TYPE Late SUPERTYPE -P");
    database.transaction(() -> database.command("sql", "INSERT INTO Late SET x = 2"));
    assertThat(audits("create")).isEqualTo(1);

    // A type that starts extending P is covered from then on
    database.command("sql", "ALTER TYPE Late SUPERTYPE +P");
    database.transaction(() -> database.command("sql", "INSERT INTO Late SET x = 3"));
    assertThat(audits("create")).isEqualTo(2);
  }

  @Test
  void droppingATypeInTheMiddleOfTheHierarchyMovesTheLineageWithIt() {
    database.command("sql", "CREATE VERTEX TYPE Middle EXTENDS P");
    database.command("sql", "CREATE VERTEX TYPE Leaf EXTENDS Middle");
    auditTrigger("onP", "AFTER CREATE", "P", "p");
    auditTrigger("onMiddle", "AFTER CREATE", "Middle", "middle");

    database.transaction(() -> database.command("sql", "INSERT INTO Leaf SET x = 1"));
    assertThat(audits("p")).isEqualTo(1);
    assertThat(audits("middle")).isEqualTo(1);

    // The subtypes of a dropped type are linked to its super types: Leaf keeps firing P's trigger, and no longer Middle's
    database.command("sql", "DROP TYPE Middle UNSAFE");
    assertThat(database.getSchema().getType("Leaf").instanceOf("P")).isTrue();
    database.transaction(() -> database.command("sql", "INSERT INTO Leaf SET x = 2"));
    assertThat(audits("p")).isEqualTo(2);
    assertThat(audits("middle")).isEqualTo(1);
  }

  @Test
  void droppingATypeDropsItsTriggers() {
    database.command("sql", "CREATE VERTEX TYPE Gone");
    auditTrigger("onGone", "AFTER CREATE", "Gone", "gone");
    auditTrigger("onP", "AFTER CREATE", "P", "p");

    database.command("sql", "DROP TYPE Gone");
    assertThat(database.getSchema().existsTrigger("onGone")).isFalse();
    assertThat(database.getSchema().existsTrigger("onP")).isTrue();

    // A type created later under the same name does not inherit the trigger, in this session or after a reopen
    database.command("sql", "CREATE VERTEX TYPE Gone");
    database.transaction(() -> database.command("sql", "INSERT INTO Gone SET x = 1"));
    reopenDatabase();
    assertThat(database.getSchema().existsTrigger("onGone")).isFalse();
    assertThat(database.getSchema().existsTrigger("onP")).isTrue();
    database.transaction(() -> database.command("sql", "INSERT INTO Gone SET x = 2"));
    assertThat(audits("gone")).isZero();
  }

  @Test
  void triggersStayPolymorphicAfterReopeningTheDatabase() {
    auditTrigger("ta", "AFTER CREATE", "P", "create");
    reopenDatabase();

    database.transaction(() -> database.command("sql", "INSERT INTO S SET x = 1"));
    assertThat(audits("create")).isEqualTo(1);
  }

  @Test
  void droppingTheTriggerStopsItForSubtypesToo() {
    auditTrigger("ta", "AFTER CREATE", "P", "create");
    database.command("sql", "DROP TRIGGER ta");

    database.transaction(() -> database.command("sql", "INSERT INTO S SET x = 1"));
    assertThat(audits("create")).isZero();
  }

  @Test
  void aJavaListenerOnTheTypeKeepsFiringForItsExactTypeOnly() {
    final AtomicInteger heard = new AtomicInteger();
    final AfterRecordCreateListener listener = record -> heard.incrementAndGet();
    database.getSchema().getType("P").getEvents().registerListener(listener);
    try {
      database.transaction(() -> {
        database.newVertex("P").save();
        database.newVertex("S").save();
      });
      assertThat(heard.get()).isEqualTo(1);
    } finally {
      database.getSchema().getType("P").getEvents().unregisterListener(listener);
    }
  }

  @Test
  void insertFromSelectFiresCreateOnlyOncePerRow() {
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        database.newDocument("Src").set("x", i).save();
    });
    auditTrigger("tc", "AFTER CREATE", "P", "create");
    auditTrigger("tu", "AFTER UPDATE", "P", "update");

    database.transaction(() -> database.command("sql", "INSERT INTO P FROM SELECT x FROM Src"));
    assertThat(audits("create")).isEqualTo(3);
    assertThat(audits("update")).isZero();

    // The rows went where the statement said, with what the select projected
    assertThat(count("SELECT count(*) AS c FROM P WHERE x IS NOT NULL")).isEqualTo(3);
    final String bucket = database.getSchema().getType("S").getBuckets(false).getLast().getName();
    database.transaction(() -> database.command("sql", "INSERT INTO bucket:" + bucket + " FROM SELECT x FROM Src"));
    assertThat(count("SELECT count() AS c FROM S")).isEqualTo(3);
    assertThat(count("SELECT count() AS c FROM bucket:" + bucket)).isEqualTo(3);
    assertThat(audits("create")).isEqualTo(6);
    assertThat(audits("update")).isZero();
  }

  @Test
  void creatingEdgesFiresNoUpdateTriggerOnTheirVertices() {
    database.transaction(() -> {
      database.command("sql", "INSERT INTO P SET x = 1");
      database.command("sql", "INSERT INTO S SET x = 2");
    });
    auditTrigger("tu", "AFTER UPDATE", "P", "update");

    // Enough edges to grow the edge lists past their first head segment, which rewrites the head pointer again
    for (int i = 0; i < 300; i++)
      database.transaction(
          () -> database.command("sql", "CREATE EDGE E FROM (SELECT FROM P WHERE x = 1) TO (SELECT FROM P WHERE x = 2)"));

    assertThat(audits("update")).isZero();
    assertThat(count("SELECT count() AS c FROM E")).isEqualTo(300);
  }

  @Test
  void aBeforeUpdateVetoOnTheVertexTypeDoesNotOrphanTheEdgesCreatedOnIt() {
    database.command("sql", "CREATE TRIGGER frozen BEFORE UPDATE ON TYPE P EXECUTE JAVASCRIPT 'false'");
    final RID[] rids = new RID[2];
    database.transaction(() -> {
      rids[0] = database.newVertex("P").set("x", 1).save().getIdentity();
      rids[1] = database.newVertex("S").set("x", 2).save().getIdentity();
    });

    for (int i = 0; i < 300; i++)
      database.transaction(
          () -> database.command("sql", "CREATE EDGE E FROM (SELECT FROM P WHERE x = 1) TO (SELECT FROM P WHERE x = 2)"));

    final Vertex from = rids[0].asVertex();
    final Vertex to = rids[1].asVertex();
    assertThat(from.countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(300);
    assertThat(to.countEdges(Vertex.DIRECTION.IN, "E")).isEqualTo(300);

    // The veto still holds for a real update of the vertex
    database.transaction(() -> {
      final MutableVertex v = from.modify();
      v.set("x", 99);
      v.save();
    });
    assertThat(rids[0].asVertex().getInteger("x")).isEqualTo(1);
    // The integrity check of TestHelper.afterTest() is what reported the orphan edges before the fix
  }

  @Test
  void unsavedEditsOfAVertexGettingItsFirstEdgeAreStillValidatedAndAnnounced() {
    database.command("sql", "CREATE TRIGGER guard BEFORE UPDATE ON TYPE P EXECUTE JAVASCRIPT 'record.get(\"x\") >= 0'");
    auditTrigger("tu", "AFTER UPDATE", "P", "update");
    final RID[] rids = new RID[2];
    database.transaction(() -> {
      rids[0] = database.newVertex("P").set("x", 1).save().getIdentity();
      rids[1] = database.newVertex("S").set("x", 2).save().getIdentity();
    });

    // An edit nobody saved, on an instance the transaction does not hold, then the vertex's first outgoing edge: the write of
    // the edge list head brings the instance into the transaction, and the edit with it - announced, as a save would
    database.transaction(() -> {
      final MutableVertex v = rids[0].asVertex().modify();
      v.set("x", 10);
      v.newEdge("E", rids[1].asVertex());
    });
    assertThat(rids[0].asVertex().getInteger("x")).isEqualTo(10);
    assertThat(audits("update")).isEqualTo(1);

    // The same with an edit the BEFORE UPDATE trigger refuses: the edit is dropped, as a vetoed save drops it, the edge stays
    database.transaction(() -> {
      final MutableVertex v = rids[1].asVertex().modify();
      v.set("x", -5);
      v.newEdge("E", rids[0].asVertex());
    });
    assertThat(rids[1].asVertex().getInteger("x")).isEqualTo(2);
    assertThat(rids[1].asVertex().countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(1);
    assertThat(rids[0].asVertex().countEdges(Vertex.DIRECTION.IN, "E")).isEqualTo(1);
    assertThat(audits("update")).isEqualTo(1);
  }

  @Test
  void aCypherSetVetoedOnTheVertexThenAnEdgeCreatedOnItKeepsTheEdgeAndNotTheValue() {
    database.command("sql", "CREATE TRIGGER guard BEFORE UPDATE ON TYPE P EXECUTE JAVASCRIPT 'record.get(\"x\") >= 0'");
    database.transaction(() -> {
      database.newVertex("P").set("x", 1).save();
      database.newVertex("S").set("x", 2).save();
    });

    database.transaction(() -> database.command("opencypher",
        "MATCH (a:P {x: 1}), (b:S {x: 2}) SET a.x = -1 CREATE (a)-[:E]->(b)").close());

    assertThat(count("SELECT count() AS c FROM P WHERE x = -1")).isZero();
    assertThat(count("SELECT count() AS c FROM E")).isEqualTo(1);
    try (final ResultSet rs = database.query("opencypher", "MATCH (a:P {x: 1})-[:E]->(b:S {x: 2}) RETURN count(*) AS c")) {
      assertThat(((Number) rs.next().getProperty("c")).longValue()).isEqualTo(1);
    }
    // The integrity check of TestHelper.afterTest() reports an edge its vertices do not reach
  }

  private void auditTrigger(final String name, final String timing, final String type, final String tag) {
    database.command("sql",
        "CREATE TRIGGER " + name + " " + timing + " ON TYPE " + type + " EXECUTE SQL 'INSERT INTO Audit SET tag = \"" + tag
            + "\"'");
  }

  private long audits(final String tag) {
    return count("SELECT count(*) AS c FROM Audit WHERE tag = '" + tag + "'");
  }

  private long count(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }
}
