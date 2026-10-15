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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.Index;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10096: {@code LocalDocumentType.rename} moved the buckets, re-keyed the schema map and renamed the indexes, and left
 * everything else that holds the type's name on the old one - a materialized view's source type names and defining query, a
 * continuous aggregate's source type, the backing type of either. Those are plain strings, persisted in the schema, so:
 * <ul>
 *   <li>the rename alone broke the refresh, which re-runs a stored query that still names the old type;</li>
 *   <li>{@code DROP TYPE} resolved the stale string to nothing, matched nothing, and let the source type go, whose guard
 *   exists to refuse exactly that;</li>
 *   <li>{@code DROP MATERIALIZED VIEW} after the backing type was renamed reported success while dropping no backing type.</li>
 * </ul>
 * A rename now carries the SOURCE type of a view or an aggregate (its names AND the query text), and is refused before a file
 * moves for the BACKING type (a view is named after its backing type) or for a query it cannot re-point with certainty. The
 * invariant is asserted over the whole schema, as the issue asks: after a rename no persisted schema object names a type that
 * {@code getTypeOrNull} cannot resolve.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10096TypeRenameCarriesDependantsTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Src");
    database.command("sql", "CREATE TIMESERIES TYPE Readings TIMESTAMP ts TAGS (sensor STRING) FIELDS (temperature DOUBLE)");
    database.transaction(() -> {
      database.newDocument("Src").set("name", "a").save();
      database.newDocument("Src").set("name", "b").save();
      database.command("sql", "INSERT INTO Readings SET ts = 3600000, sensor = 'A', temperature = 20.0");
      database.command("sql", "INSERT INTO Readings SET ts = 3700000, sensor = 'A', temperature = 22.0");
    });
  }

  @Test
  void aMaterializedViewFollowsTheRenameOfItsSourceType() {
    database.getSchema().buildMaterializedView().withName("MV").withQuery("SELECT FROM Src").create();
    assertThat(count("MV")).isEqualTo(2);

    database.command("sql", "ALTER TYPE Src NAME Renamed");

    final MaterializedView view = database.getSchema().getMaterializedView("MV");
    assertThat(view.getSourceTypeNames()).containsExactly("Renamed");
    assertThat(view.getQuery()).contains("Renamed").doesNotContain("Src");
    assertNoSchemaObjectNamesAnUnresolvableType();

    // THE REFRESH RE-RUNS THE STORED QUERY: IT USED TO FAIL WITH "Type with name 'Src' was not found"
    database.transaction(() -> database.newDocument("Renamed").set("name", "c").save());
    view.refresh();
    assertThat(count("MV")).isEqualTo(3);

    // AND THE GUARD OF DROP TYPE STILL SEES THE DEPENDANT UNDER THE NEW NAME
    assertThatThrownBy(() -> database.getSchema().dropType("Renamed")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("source type for materialized view 'MV'");
    assertThat(database.getSchema().existsType("Renamed")).isTrue();
  }

  @Test
  void aViewWithAWhereAndAGroupByKeepsTheRestOfItsQueryWhenItFollowsTheRename() {
    database.getSchema().buildMaterializedView().withName("MV")
        .withQuery("SELECT name, count(*) AS c FROM Src WHERE name <> 'z' GROUP BY name").create();
    assertThat(count("MV")).isEqualTo(2);

    database.command("sql", "ALTER TYPE Src NAME Renamed");

    final MaterializedView view = database.getSchema().getMaterializedView("MV");
    assertThat(view.getQuery()).isEqualTo("SELECT name, count(*) AS c FROM Renamed WHERE name <> 'z' GROUP BY name");
    database.transaction(() -> database.newDocument("Renamed").set("name", "a").save());
    view.refresh();
    assertThat(count("MV")).as("two groups, 'a' counted twice now").isEqualTo(2);
    assertNoSchemaObjectNamesAnUnresolvableType();
  }

  @Test
  void anIncrementalViewKeepsFollowingItsRenamedSource() {
    database.getSchema().buildMaterializedView().withName("MV").withQuery("SELECT FROM Src")
        .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL).create();

    database.command("sql", "ALTER TYPE Src NAME Renamed");

    // THE LISTENER IS ON THE TYPE, WHICH IS THE SAME OBJECT UNDER ITS NEW NAME
    database.transaction(() -> database.newDocument("Renamed").set("name", "c").save());
    assertThat(count("MV")).isEqualTo(3);
    assertNoSchemaObjectNamesAnUnresolvableType();
  }

  @Test
  void theCarriedViewSurvivesAReopen() {
    database.getSchema().buildMaterializedView().withName("MV").withQuery("SELECT FROM Src").create();
    database.command("sql", "ALTER TYPE Src NAME Renamed");

    reopenDatabase();

    final MaterializedView view = database.getSchema().getMaterializedView("MV");
    assertThat(view.getSourceTypeNames()).containsExactly("Renamed");
    assertThat(view.getQuery()).contains("Renamed");
    assertNoSchemaObjectNamesAnUnresolvableType();
    view.refresh();
    assertThat(count("MV")).isEqualTo(2);
  }

  @Test
  void aContinuousAggregateFollowsTheRenameOfItsSourceType() {
    database.getSchema().buildContinuousAggregate().withName("Hourly").withQuery(
        "SELECT sensor, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp FROM Readings GROUP BY sensor, hour").create();
    final long before = count("Hourly");
    assertThat(before).isGreaterThan(0);

    database.command("sql", "ALTER TYPE Readings NAME Readings2");

    final ContinuousAggregate aggregate = database.getSchema().getContinuousAggregate("Hourly");
    assertThat(aggregate.getSourceTypeName()).isEqualTo("Readings2");
    assertThat(aggregate.getQuery()).contains("Readings2").doesNotContain("FROM Readings ");
    assertNoSchemaObjectNamesAnUnresolvableType();

    database.transaction(() -> database.command("sql", "INSERT INTO Readings2 SET ts = 10800000, sensor = 'A', temperature = 30.0"));
    aggregate.refresh();
    assertThat(count("Hourly")).isGreaterThan(before);

    assertThatThrownBy(() -> database.getSchema().dropType("Readings2")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("source type for continuous aggregate 'Hourly'");
    assertThat(database.getSchema().existsType("Readings2")).isTrue();
  }

  @Test
  void theBackingTypeOfAViewIsNotRenamedAndNothingMoves() {
    database.getSchema().buildMaterializedView().withName("MV").withQuery("SELECT FROM Src").create();

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE MV NAME BackingRenamed")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("backing type for materialized view 'MV'");

    // THE REFUSAL CAME BEFORE A FILE MOVED: THE VIEW READS AS IT DID AND DROPS CLEANLY, BACKING TYPE INCLUDED
    assertThat(database.getSchema().existsType("MV")).isTrue();
    assertThat(database.getSchema().existsType("BackingRenamed")).isFalse();
    assertThat(count("MV")).isEqualTo(2);
    database.getSchema().dropMaterializedView("MV");
    assertThat(database.getSchema().existsType("MV")).isFalse();
  }

  @Test
  void theBackingTypeOfAnAggregateIsNotRenamed() {
    database.getSchema().buildContinuousAggregate().withName("Hourly").withQuery(
        "SELECT sensor, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp FROM Readings GROUP BY sensor, hour").create();

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE Hourly NAME BackingRenamed")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("backing type for continuous aggregate 'Hourly'");

    assertThat(database.getSchema().existsType("Hourly")).isTrue();
    assertThat(database.getSchema().existsType("BackingRenamed")).isFalse();
  }

  @Test
  void aQueryThatCannotBeRepointedWithCertaintyRefusesTheRenameBeforeAnythingMoves() {
    // THE SOURCE TYPE IS NAMED TWICE: RE-POINTING THE FROM CLAUSE ALONE WOULD LEAVE THE SUBQUERY ON A TYPE THAT NO LONGER EXISTS
    database.getSchema().buildMaterializedView().withName("MV")
        .withQuery("SELECT FROM Src WHERE name IN (SELECT name FROM Src WHERE name <> 'z')").create();

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE Src NAME Renamed")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("materialized view 'MV'").hasMessageContaining("Src");

    assertThat(database.getSchema().existsType("Src")).isTrue();
    assertThat(database.getSchema().existsType("Renamed")).isFalse();
    // NO FILE MOVED: THE BUCKETS STILL CARRY THE NAME OF THE TYPE
    assertThat(database.getSchema().getType("Src").getBuckets(false)).isNotEmpty().allMatch(b -> b.getName().startsWith("Src_"));
    assertThat(database.getSchema().getMaterializedView("MV").getSourceTypeNames()).containsExactly("Src");
    database.getSchema().getMaterializedView("MV").refresh();
    assertNoSchemaObjectNamesAnUnresolvableType();
  }

  /** A new name that is not a plain identifier is spelled between back-ticks in the query, which has to parse back to the same statement. */
  @Test
  void aNewNameWithLettersOutsideAsciiIsQuotedInTheRewrittenQuery() {
    database.getSchema().buildMaterializedView().withName("MV").withQuery("SELECT FROM Src WHERE name <> 'z'").create();

    database.command("sql", "ALTER TYPE Src NAME `Città`");

    final MaterializedView view = database.getSchema().getMaterializedView("MV");
    assertThat(view.getSourceTypeNames()).containsExactly("Città");
    assertThat(view.getQuery()).isEqualTo("SELECT FROM `Città` WHERE name <> 'z'");
    view.refresh();
    assertThat(count("MV")).isEqualTo(2);
    assertNoSchemaObjectNamesAnUnresolvableType();

    // AND BACK: A NAME WITH A NON-ASCII LETTER IS FOUND AS ONE TOKEN, NOT AS THE PREFIX OF A LONGER ONE
    database.command("sql", "ALTER TYPE `Città` NAME Src");
    assertThat(database.getSchema().getMaterializedView("MV").getQuery()).isEqualTo("SELECT FROM Src WHERE name <> 'z'");
    assertNoSchemaObjectNamesAnUnresolvableType();
  }

  /** A string literal that holds the name is not told apart from a second reference without a parser: refused, and the message says so. */
  @Test
  void aStringLiteralHoldingTheTypeNameRefusesTheRenameAndTheMessageSaysSo() {
    database.getSchema().buildMaterializedView().withName("MV").withQuery("SELECT FROM Src WHERE name <> 'Src'").create();

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE Src NAME Renamed")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("string literal or a comment");

    assertThat(database.getSchema().existsType("Src")).isTrue();
    assertThat(database.getSchema().existsType("Renamed")).isFalse();
  }

  @Test
  void aViewDefinedThroughAnAliasKeepsResolvingAfterTheRename() {
    database.command("sql", "ALTER TYPE Src ALIASES SrcAlias");
    database.getSchema().buildMaterializedView().withName("MV").withQuery("SELECT FROM SrcAlias").create();

    database.command("sql", "ALTER TYPE Src NAME Renamed");

    // THE ALIAS STAYS IN THE TYPE MAP, SO THE STORED NAME AND THE QUERY STILL RESOLVE WITHOUT BEING REWRITTEN
    assertThat(database.getSchema().getMaterializedView("MV").getSourceTypeNames()).containsExactly("SrcAlias");
    database.getSchema().getMaterializedView("MV").refresh();
    assertThat(count("MV")).isEqualTo(2);
    assertNoSchemaObjectNamesAnUnresolvableType();
    assertThatThrownBy(() -> database.getSchema().dropType("Renamed")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("materialized view 'MV'");
  }

  /**
   * A trigger is bound to its type by name as well: persisted with it, bound by it on reload, and unregistered through it. Left
   * on the old name, the renamed type's trigger came back unbound after a reopen, and DROP TRIGGER could not find the type to
   * take the listener off.
   */
  @Test
  void aTriggerFollowsTheRenameOfItsType() {
    database.getSchema().createDocumentType("Audit");
    database.command("sql", "CREATE TRIGGER TrgSrc AFTER CREATE ON TYPE Src EXECUTE SQL \"INSERT INTO Audit SET what = 'created'\"");

    database.command("sql", "ALTER TYPE Src NAME Renamed");

    assertThat(database.getSchema().getTrigger("TrgSrc").getTypeName()).isEqualTo("Renamed");
    assertThat(database.getSchema().getTriggersForType("Renamed")).hasSize(1);
    assertThat(database.getSchema().getTriggersForType("Src")).isEmpty();
    assertNoSchemaObjectNamesAnUnresolvableType();
    database.transaction(() -> database.newDocument("Renamed").set("name", "x").save());
    assertThat(count("Audit")).isEqualTo(1);

    reopenDatabase();

    assertThat(database.getSchema().getTrigger("TrgSrc").getTypeName()).isEqualTo("Renamed");
    database.transaction(() -> database.newDocument("Renamed").set("name", "y").save());
    assertThat(count("Audit")).as("the trigger is bound again after the reopen").isEqualTo(2);

    // DROP TRIGGER FINDS THE TYPE BY ITS NEW NAME AND TAKES THE LISTENER OFF
    database.getSchema().dropTrigger("TrgSrc");
    database.transaction(() -> database.newDocument("Renamed").set("name", "z").save());
    assertThat(count("Audit")).as("a dropped trigger does not fire").isEqualTo(2);
  }

  /**
   * The check of the rename runs before any lock, so a view can be registered between it and the carry. The window is made
   * deterministic by holding the database write lock on a helper thread: the rename passes its check, moves the files and parks
   * on the lock that reserves the new name, and only then does the helper - still holding the lock - register a view on the type.
   * The carry, which runs under that lock too, takes the late view along.
   */
  @Test
  void aViewRegisteredAfterTheCheckOfTheRenameIsCarriedToo() throws Exception {
    final LocalDatabase embedded = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
    final CountDownLatch lockHeld = new CountDownLatch(1);
    final CountDownLatch renameParked = new CountDownLatch(1);
    final AtomicReference<Throwable> holderFailure = new AtomicReference<>();
    final AtomicReference<Throwable> renameFailure = new AtomicReference<>();

    final Thread holder = new Thread(() -> {
      try {
        embedded.executeInWriteLock(() -> {
          lockHeld.countDown();
          assertThat(renameParked.await(30, TimeUnit.SECONDS)).isTrue();
          database.getSchema().buildMaterializedView().withName("LateView").withQuery("SELECT FROM Src").create();
          return null;
        });
      } catch (final Throwable t) {
        holderFailure.set(t);
      }
    }, "Issue10096-lock-holder");

    final Thread renamer = new Thread(() -> {
      try {
        database.getSchema().getType("Src").rename("Renamed");
      } catch (final Throwable t) {
        renameFailure.set(t);
      }
    }, "Issue10096-renamer");

    holder.start();
    try {
      assertThat(lockHeld.await(30, TimeUnit.SECONDS)).as("the helper thread took the database write lock").isTrue();
      renamer.start();
      awaitParkedOnTheWriteLock(renamer);
    } finally {
      renameParked.countDown();
      holder.join(60_000);
      renamer.join(60_000);
    }

    assertThat(holder.isAlive()).isFalse();
    assertThat(renamer.isAlive()).isFalse();
    assertThat(holderFailure.get()).as("the view registered under the lock").isNull();
    assertThat(renameFailure.get()).as("the rename").isNull();

    final MaterializedView late = database.getSchema().getMaterializedView("LateView");
    assertThat(late.getSourceTypeNames()).containsExactly("Renamed");
    assertThat(late.getQuery()).contains("Renamed").doesNotContain("Src");
    assertNoSchemaObjectNamesAnUnresolvableType();
    late.refresh();
    assertThat(count("LateView")).isEqualTo(2);
  }

  /** The renamer is waiting inside the write lock acquisition, so the check of the rename is behind it. */
  private static void awaitParkedOnTheWriteLock(final Thread thread) throws InterruptedException {
    for (int i = 0; i < 3_000; i++) {
      final Thread.State state = thread.getState();
      if (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING)
        for (final StackTraceElement frame : thread.getStackTrace())
          if (frame.getClassName().equals(LocalDatabase.class.getName()) && frame.getMethodName().equals("executeInWriteLock"))
            return;
      assertThat(thread.isAlive()).as("the renamer ended before it reached the write lock").isTrue();
      Thread.sleep(10);
    }
    throw new AssertionError("The renamer never parked on the database write lock");
  }

  @Test
  void aTypeWithNoDependantRenamesAsBefore() {
    database.getSchema().createDocumentType("Free");
    database.getSchema().buildMaterializedView().withName("MV").withQuery("SELECT FROM Src").create();

    database.command("sql", "ALTER TYPE Free NAME FreeRenamed");

    assertThat(database.getSchema().existsType("FreeRenamed")).isTrue();
    assertThat(database.getSchema().existsType("Free")).isFalse();
    assertThat(database.getSchema().getMaterializedView("MV").getSourceTypeNames()).containsExactly("Src");
  }

  /**
   * A database whose view was left naming a type that no longer exists - what a rename did before this fix, and what the
   * schema keeps persisting - must fail safe: {@code DROP TYPE} used to compare the unresolvable string with nothing and let
   * any type go. It now refuses and says which view is out of date, so the operator drops it (or recreates it) first.
   */
  @Test
  void aDependantNamingAnUnresolvableTypeMakesDropTypeFailSafe() {
    database.getSchema().createDocumentType("Other");
    database.getSchema().buildMaterializedView().withName("MV").withQuery("SELECT FROM Src").create();
    // WHAT A RENAME BEFORE THE FIX LEFT BEHIND: THE VIEW STILL NAMES A TYPE THAT IS GONE
    ((MaterializedViewImpl) database.getSchema().getMaterializedView("MV")).retargetSourceType("Src", "Gone",
        "SELECT FROM Gone");

    assertThatThrownBy(() -> database.getSchema().dropType("Other")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("materialized view 'MV'").hasMessageContaining("'Gone'").hasMessageContaining("does not exist");
    assertThat(database.getSchema().existsType("Other")).isTrue();

    // AND THE WAY OUT IS THE ONE THE MESSAGE NAMES
    database.getSchema().dropMaterializedView("MV");
    database.getSchema().dropType("Other");
    assertThat(database.getSchema().existsType("Other")).isFalse();
  }

  /**
   * The invariant of the issue, over the whole schema: no persisted object names a type {@code getTypeOrNull} cannot resolve.
   * Views and aggregates (source and backing), the type every index claims, and the type every bucket belongs to.
   */
  private void assertNoSchemaObjectNamesAnUnresolvableType() {
    final Schema schema = database.getSchema();
    for (final MaterializedView view : schema.getMaterializedViews()) {
      assertThat(schema.getTypeOrNull(((MaterializedViewImpl) view).getBackingTypeName()))
          .as("backing type of view %s", view.getName()).isNotNull();
      for (final String source : view.getSourceTypeNames())
        assertThat(schema.getTypeOrNull(source)).as("source type '%s' of view %s", source, view.getName()).isNotNull();
    }
    for (final ContinuousAggregate aggregate : schema.getContinuousAggregates()) {
      assertThat(schema.getTypeOrNull(((ContinuousAggregateImpl) aggregate).getBackingTypeName()))
          .as("backing type of aggregate %s", aggregate.getName()).isNotNull();
      assertThat(schema.getTypeOrNull(aggregate.getSourceTypeName())).as("source type of aggregate %s", aggregate.getName())
          .isNotNull();
    }
    for (final Trigger trigger : schema.getTriggers())
      assertThat(schema.getTypeOrNull(trigger.getTypeName())).as("type of trigger %s", trigger.getName()).isNotNull();
    for (final Index index : schema.getIndexes())
      if (index.getTypeName() != null)
        assertThat(schema.getTypeOrNull(index.getTypeName())).as("type of index %s", index.getName()).isNotNull();
    for (final DocumentType type : schema.getTypes())
      for (final Bucket bucket : type.getBuckets(false))
        assertThat(schema.getTypeByBucketName(bucket.getName())).as("type of bucket %s", bucket.getName()).isNotNull();
  }

  private long count(final String type) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + type)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }
}
