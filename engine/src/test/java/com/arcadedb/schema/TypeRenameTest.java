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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.FileManager;
import com.arcadedb.engine.timeseries.TimeSeriesBucket;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.engine.timeseries.TimeSeriesShard;
import com.arcadedb.exception.AlreadyExistsException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/**
 * Regression tests, one nested class per fixed issue.
 */
class TypeRenameTest {

  /**
   * Regression for #10096: {@code LocalDocumentType.rename} moved the buckets, re-keyed the schema map and renamed the indexes, and left
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
  @Nested
  class TypeRenameCarriesDependants extends TestHelper {

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

  /**
   * Regression for #10317 (case B): an index a type inherits has one sub-index per bucket of the type, and that sub-index names the type
   * (and so does its file). {@code ALTER TYPE ... NAME} renamed only the indexes the type declares itself, so renaming a sub type
   * left the sub-index of an inherited index naming the old type, and a later {@code DROP TYPE} of the super type could not drop
   * it ("Type with name 'T10' was not found"). The opposite direction was wrong too: renaming a super type re-labelled the
   * sub-indexes that sit on the buckets of its sub types.
   * <p>
   * The invariant pinned here: a sub-index names the type that owns its bucket, and its file is named after that type.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RenamedTypeInheritedIndex extends TestHelper {

    @Test
    void renamingASubTypeCarriesTheSubIndexOfAnInheritedIndex() {
      createSuperAndSubType();
      insert("T10", "x");
      insert("T5", "y");

      database.getSchema().getType("T10").rename("T11");

      assertEverySubIndexNamesTheTypeOwningItsBucket();
      assertThat(count("T5", "x")).isEqualTo(1L);
      assertThat(count("T5", "y")).isEqualTo(1L);

      reopenDatabase();

      assertEverySubIndexNamesTheTypeOwningItsBucket();
      assertThat(count("T5", "x")).isEqualTo(1L);
      insert("T11", "z");
      assertThat(count("T5", "z")).isEqualTo(1L);
      assertThat(count("T11", "z")).isEqualTo(1L);
    }

    @Test
    void renamingASuperTypeLeavesTheSubIndexesOfItsSubTypesAlone() {
      createSuperAndSubType();
      insert("T10", "x");
      insert("T5", "y");

      database.getSchema().getType("T5").rename("T6");

      assertEverySubIndexNamesTheTypeOwningItsBucket();
      assertThat(count("T6", "x")).isEqualTo(1L);
      assertThat(count("T6", "y")).isEqualTo(1L);

      reopenDatabase();

      assertEverySubIndexNamesTheTypeOwningItsBucket();
      assertThat(count("T6", "x")).isEqualTo(1L);
      assertThat(count("T6", "y")).isEqualTo(1L);
    }

    @Test
    void dropTypeOfTheSuperTypeAfterRenamingASubTypeBehavesAsWithoutTheRename() {
      createSuperAndSubType();
      insert("T10", "x");
      database.getSchema().getType("T10").rename("T11");

      database.command("sql", "DROP TYPE T5");

      assertThat(database.getSchema().existsType("T5")).isFalse();
      assertThat(database.getSchema().getType("T11").getSuperTypes()).isEmpty();
      assertThat(database.getSchema().getType("T11").getAllIndexes(true)).isEmpty();
      assertThat(database.countType("T11", true)).isEqualTo(1L);

      reopenDatabase();

      assertThat(database.getSchema().existsType("T5")).isFalse();
      assertThat(database.getSchema().getType("T11").getSuperTypes()).isEmpty();
      assertThat(database.countType("T11", true)).isEqualTo(1L);
    }

    @Test
    void renamingATypeThatDeclaresAnIndexAndInheritsAnotherCarriesBoth() {
      database.command("sql", "CREATE DOCUMENT TYPE T1");
      database.command("sql", "CREATE PROPERTY T1.p STRING");
      database.command("sql", "CREATE INDEX ON T1 (p) NOTUNIQUE");
      database.command("sql", "CREATE DOCUMENT TYPE T2 EXTENDS T1");
      database.command("sql", "CREATE PROPERTY T2.q STRING");
      database.command("sql", "CREATE INDEX ON T2 (q) NOTUNIQUE");
      database.command("sql", "CREATE DOCUMENT TYPE T3 EXTENDS T2");
      database.transaction(() -> {
        database.newDocument("T2").set("p", "p2").set("q", "q2").save();
        database.newDocument("T3").set("p", "p3").set("q", "q3").save();
      });

      database.getSchema().getType("T2").rename("T2b");

      assertEverySubIndexNamesTheTypeOwningItsBucket();
      assertThat(database.query("sql", "SELECT FROM T1 WHERE p = 'p3'").stream().count()).isEqualTo(1L);
      assertThat(database.query("sql", "SELECT FROM T2b WHERE q = 'q3'").stream().count()).isEqualTo(1L);

      reopenDatabase();

      assertEverySubIndexNamesTheTypeOwningItsBucket();
      assertThat(database.query("sql", "SELECT FROM T1 WHERE p = 'p2'").stream().count()).isEqualTo(1L);
      assertThat(database.query("sql", "SELECT FROM T2b WHERE q = 'q2'").stream().count()).isEqualTo(1L);
    }

    /**
     * The inherited sub-index is renamed with the type, so a failure on it has to put the type back like a failure on an index the
     * type declares: the failure is injected by parking a directory on the path its file is about to move to.
     */
    @Test
    void aFailedRenameOfTheInheritedSubIndexPutsTheTypeBack() {
      createSuperAndSubType();
      insert("T10", "x");

      final IndexInternal inherited = subIndexOn("T10");
      final File databaseDirectory = new File(database.getDatabasePath());
      final File blockedFile = fileStartingWith(databaseDirectory, inherited.getName() + ".");
      final File parked = new File(databaseDirectory, blockedFile.getName().replace("T10_0_", "T11_0_"));
      assertThat(parked.mkdir()).as("fault injection: park a directory on the target path").isTrue();

      try {
        assertThatThrownBy(() -> database.getSchema().getType("T10").rename("T11")).isInstanceOf(SchemaException.class);

        assertThat(database.getSchema().existsType("T10")).isTrue();
        assertThat(database.getSchema().existsType("T11")).isFalse();
        assertEverySubIndexNamesTheTypeOwningItsBucket();
      } finally {
        parked.delete();
      }

      assertThat(count("T5", "x")).isEqualTo(1L);

      reopenDatabase();

      assertEverySubIndexNamesTheTypeOwningItsBucket();
      assertThat(count("T5", "x")).isEqualTo(1L);
    }

    /**
     * A database renamed by a version before #10317 holds the sub-index of an inherited index under the OLD type name, in its
     * metadata and in its file name. Nothing has to repair it by hand (issue #10394): the schema load binds every sub-index to the
     * type that owns its bucket (#10093, #10412) and names the type in its metadata, so opening the database with this version is
     * the repair, and the DROP TYPE of the super type that used to fail finds the sub-index. The file keeps the older prefix, which
     * nothing reads: a later rename leaves a file that does not carry the type's name where it is.
     */
    @Test
    void aSchemaRenamedByAnOlderVersionIsRepairedByTheLoad() {
      createSuperAndSubType();
      insert("T10", "x");
      database.getSchema().getType("T10").rename("T11");

      // What the older version left behind: the sub-index naming the type as it was before the rename
      subIndexOn("T11").updateTypeName("T10");
      database.getSchema().getEmbedded().saveConfiguration();
      assertThat(subIndexOn("T11").getTypeName()).isEqualTo("T10");
      // updateTypeName moves the file with the name: the stale layout is on disk too, not only in the metadata
      assertThat(componentNameOf(subIndexOn("T11"))).startsWith("T10_");

      reopenDatabase();

      assertThat(subIndexOn("T11").getTypeName()).isEqualTo("T11");
      assertThat(componentNameOf(subIndexOn("T11"))).as("the file keeps the older prefix, which nothing reads").startsWith("T10_");
      assertThat(count("T5", "x")).isEqualTo(1L);

      database.command("sql", "DROP TYPE T5");

      assertThat(database.getSchema().existsType("T5")).isFalse();
      assertThat(database.getSchema().getType("T11").getAllIndexes(true)).isEmpty();
      assertThat(database.countType("T11", true)).isEqualTo(1L);

      reopenDatabase();

      assertThat(database.getSchema().existsType("T5")).isFalse();
      assertThat(database.countType("T11", true)).isEqualTo(1L);
    }

    private void createSuperAndSubType() {
      database.command("sql", "CREATE DOCUMENT TYPE T5");
      database.command("sql", "CREATE PROPERTY T5.p STRING");
      database.command("sql", "CREATE INDEX ON T5 (p) NOTUNIQUE");
      database.command("sql", "CREATE DOCUMENT TYPE T10 EXTENDS T5");
    }

    private void insert(final String typeName, final String value) {
      database.transaction(() -> database.newDocument(typeName).set("p", value).save());
    }

    private long count(final String typeName, final String value) {
      return database.query("sql", "SELECT FROM " + typeName + " WHERE p = ?", value).stream().count();
    }

    private IndexInternal subIndexOn(final String typeName) {
      final int bucketId = database.getSchema().getType(typeName).getBuckets(false).getFirst().getFileId();
      for (final TypeIndex index : database.getSchema().getType("T5").getAllIndexes(false))
        for (final IndexInternal subIndex : index.getIndexesOnBuckets())
          if (subIndex.getAssociatedBucketId() == bucketId)
            return subIndex;
      throw new AssertionError("no sub-index on a bucket of " + typeName);
    }

    /** A sub-index names the type that owns its bucket, and its file is named after that type. */
    private void assertEverySubIndexNamesTheTypeOwningItsBucket() {
      for (final DocumentType type : database.getSchema().getTypes())
        for (final TypeIndex index : type.getAllIndexes(false))
          for (final IndexInternal subIndex : index.getIndexesOnBuckets()) {
            final String owner = database.getSchema().getTypeByBucketId(subIndex.getAssociatedBucketId()).getName();
            assertThat(subIndex.getTypeName()).as("type named by the sub-index " + subIndex.getName()).isEqualTo(owner);
            assertThat(((DatabaseInternal) database).getFileManager().getFile(subIndex.getFileId()).getComponentName())
                .as("file of the sub-index " + subIndex.getName()).startsWith(owner + "_");
          }
    }

    private String componentNameOf(final IndexInternal subIndex) {
      return ((DatabaseInternal) database).getFileManager().getFile(subIndex.getFileId()).getComponentName();
    }

    private static File fileStartingWith(final File directory, final String prefix) {
      final File[] files = directory.listFiles((dir, name) -> name.startsWith(prefix));
      assertThat(files).as("a file starting with " + prefix).isNotNull().hasSize(1);
      return files[0];
    }
  }

  /**
   * Regression for #10412: after #10348 (a rename relabels only the sub-indexes sitting on the buckets of the renamed type), an index created
   * with {@code CREATE INDEX} on a type that already had a sub type was left with the sub-index of the sub type's bucket naming the
   * type that DECLARES the index, not the type that owns the bucket. The rename of the declaring type then never reached it, and
   * {@code DROP TYPE} / {@code ALTER TYPE ... BUCKET -} of the sub type could not find the type the sub-index named ("Type with name
   * 'T22' was not found") and left the schema half changed. A schema reload binds every sub-index to the type that owns its bucket,
   * so the layout in memory differed from the one after a reopen.
   * <p>
   * The invariant pinned here: a sub-index names the type that owns its bucket, also right after {@code CREATE INDEX}, so that
   * the layout does not change at the next open and every sequence of schema statements ends the same before and after a reopen.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class RenamedIndexedSuperType extends TestHelper {

    @Test
    void dropTypeOfASubTypeAfterItsIndexedSuperTypeWasRenamed() {
      run("CREATE DOCUMENT TYPE T22 BUCKETS 3", "CREATE DOCUMENT TYPE T25 EXTENDS T22", "CREATE PROPERTY T22.p STRING",
          "CREATE INDEX ON T22 (p) NOTUNIQUE", "ALTER TYPE T22 NAME T28");

      run("DROP TYPE T25");

      assertThat(database.getSchema().existsType("T25")).isFalse();
      final String expected = "T28{super [], buckets [T28_0, T28_1, T28_2], indexes [T28[p]]}";
      assertThat(schema()).isEqualTo(expected);
      assertEverySubIndexNamesTheTypeOwningItsBucket();

      reopenDatabase();

      assertThat(schema()).isEqualTo(expected);
      assertEverySubIndexNamesTheTypeOwningItsBucket();
    }

    @Test
    void removingTheBucketOfARenamedSubTypeAfterItsIndexedSuperTypeWasRenamed() {
      run("CREATE EDGE TYPE T1", "CREATE EDGE TYPE T2 EXTENDS T1", "ALTER TYPE T2 NAME T4", "CREATE PROPERTY T1.p STRING",
          "CREATE INDEX ON T1 (p) NOTUNIQUE", "ALTER TYPE T1 NAME T10");

      run("ALTER TYPE T4 BUCKET -T4_0");

      final String expected = "T10{super [], buckets [T10_0], indexes [T10[p]]} T4{super [T10], buckets [], indexes []}";
      assertThat(schema()).isEqualTo(expected);
      assertEverySubIndexNamesTheTypeOwningItsBucket();

      reopenDatabase();

      assertThat(schema()).isEqualTo(expected);
      assertEverySubIndexNamesTheTypeOwningItsBucket();
    }

    @Test
    void anIndexedTypeRenamedTwiceLosesItsLastBucketThenItsSubTypeIsDropped() {
      run("CREATE DOCUMENT TYPE T1", "ALTER TYPE T1 NAME T13", "CREATE DOCUMENT TYPE T15 EXTENDS T13", "CREATE PROPERTY T15.p STRING",
          "CREATE INDEX ON T15 (p) NOTUNIQUE", "CREATE DOCUMENT TYPE T64 EXTENDS T15 BUCKETS 4", "ALTER TYPE T15 NAME T65",
          "ALTER TYPE T65 NAME T66", "ALTER TYPE T66 BUCKET -T66_0");

      run("DROP TYPE T64");

      final String expected = "T13{super [], buckets [T13_0], indexes []} T66{super [T13], buckets [], indexes []}";
      assertThat(schema()).isEqualTo(expected);

      reopenDatabase();

      assertThat(schema()).isEqualTo(expected);
    }

    /**
     * The same drop without a rename: the indexed super type has lost its own buckets, so the sub-index of its last sub type is the
     * only one left under the wrapper, and dropping that sub type must take the wrapper with it instead of leaving an empty one that
     * the next schema save fails on.
     */
    @Test
    void droppingTheLastSubTypeOfAnIndexedSuperTypeWithoutBucketsTakesTheEmptyIndexWithIt() {
      run("CREATE DOCUMENT TYPE A", "CREATE DOCUMENT TYPE B EXTENDS A BUCKETS 2", "CREATE PROPERTY A.p STRING",
          "CREATE INDEX ON A (p) NOTUNIQUE", "ALTER TYPE A BUCKET -A_0");

      run("DROP TYPE B");

      final String expected = "A{super [], buckets [], indexes []}";
      assertThat(schema()).isEqualTo(expected);

      reopenDatabase();

      assertThat(schema()).isEqualTo(expected);
    }

    /** The layout the sub-indexes have right after CREATE INDEX is the one the next open gives them. */
    @Test
    void aSubIndexNamesTheTypeOwningItsBucketRightAfterCreateIndex() {
      run("CREATE DOCUMENT TYPE A", "CREATE DOCUMENT TYPE B EXTENDS A", "CREATE DOCUMENT TYPE C EXTENDS B",
          "CREATE PROPERTY A.p STRING", "CREATE INDEX ON A (p) NOTUNIQUE");
      database.transaction(() -> {
        database.newDocument("A").set("p", "a").save();
        database.newDocument("B").set("p", "b").save();
        database.newDocument("C").set("p", "c").save();
      });

      assertEverySubIndexNamesTheTypeOwningItsBucket();
      final String live = layout();

      reopenDatabase();

      assertEverySubIndexNamesTheTypeOwningItsBucket();
      assertThat(layout()).isEqualTo(live);
      assertThat(database.query("sql", "SELECT FROM A WHERE p = 'c'").stream().count()).isEqualTo(1L);
    }

    /** Renaming the indexed super type leaves the files and the names of the sub types' sub-indexes alone, and the data stays indexed. */
    @Test
    void renamingTheIndexedSuperTypeKeepsTheSubTypesIndexed() {
      run("CREATE DOCUMENT TYPE A", "CREATE DOCUMENT TYPE B EXTENDS A", "CREATE PROPERTY A.p STRING",
          "CREATE INDEX ON A (p) NOTUNIQUE");
      database.transaction(() -> {
        database.newDocument("A").set("p", "a").save();
        database.newDocument("B").set("p", "b").save();
      });

      run("ALTER TYPE A NAME A2");

      assertEverySubIndexNamesTheTypeOwningItsBucket();
      assertThat(database.query("sql", "SELECT FROM A2 WHERE p = 'b'").stream().count()).isEqualTo(1L);

      reopenDatabase();

      assertEverySubIndexNamesTheTypeOwningItsBucket();
      assertThat(database.query("sql", "SELECT FROM A2 WHERE p = 'b'").stream().count()).isEqualTo(1L);
      assertThat(database.query("sql", "SELECT FROM B WHERE p = 'b'").stream().count()).isEqualTo(1L);
    }

    private void run(final String... statements) {
      for (final String statement : statements)
        database.command("sql", statement).close();
    }

    /** Types, supertypes, buckets and indexes, in the shape of the report. */
    private String schema() {
      final StringBuilder out = new StringBuilder();
      final List<DocumentType> types = new ArrayList<>(database.getSchema().getTypes());
      types.sort(Comparator.comparing(DocumentType::getName));
      for (final DocumentType type : types) {
        final List<String> superTypes = new ArrayList<>();
        for (final DocumentType superType : type.getSuperTypes())
          superTypes.add(superType.getName());
        final List<String> buckets = new ArrayList<>();
        for (final Bucket bucket : type.getBuckets(false))
          buckets.add(bucket.getName());
        final List<String> indexes = new ArrayList<>();
        for (final TypeIndex index : type.getAllIndexes(false))
          indexes.add(index.getName());
        if (!out.isEmpty())
          out.append(' ');
        out.append(type.getName()).append("{super ").append(superTypes).append(", buckets ").append(buckets).append(", indexes ")
            .append(indexes).append('}');
      }
      return out.toString();
    }

    /** Which type each sub-index names and which file carries it, independent of the generated index names. */
    private String layout() {
      final List<String> rows = new ArrayList<>();
      for (final DocumentType type : database.getSchema().getTypes())
        for (final TypeIndex index : type.getAllIndexes(false))
          for (final IndexInternal subIndex : index.getIndexesOnBuckets())
            rows.add(index.getName() + " bucket " + subIndex.getAssociatedBucketId() + " names " + subIndex.getTypeName());
      rows.sort(Comparator.naturalOrder());
      return String.join("\n", rows);
    }

    /** A sub-index names the type that owns its bucket, and its file is named after the bucket. */
    private void assertEverySubIndexNamesTheTypeOwningItsBucket() {
      for (final DocumentType type : database.getSchema().getTypes())
        for (final TypeIndex index : type.getAllIndexes(false))
          for (final IndexInternal subIndex : index.getIndexesOnBuckets()) {
            final String owner = database.getSchema().getTypeByBucketId(subIndex.getAssociatedBucketId()).getName();
            assertThat(subIndex.getTypeName()).as("type named by the sub-index " + subIndex.getName()).isEqualTo(owner);
            assertThat(((DatabaseInternal) database).getFileManager().getFile(subIndex.getFileId()).getComponentName())
                .as("file of the sub-index " + subIndex.getName()).startsWith(owner + "_");
          }
    }
  }

  /**
   * Regression for #10749: {@code ALTER TYPE ... NAME} on a TIMESERIES type moved none of the files its samples live in. The samples are kept
   * by the type's {@code TimeSeriesEngine}: a mutable bucket and a sealed store per shard and one tag dictionary, named after the type
   * but neither the type's buckets nor an index file, which were the only two things the rename walked. The statement was accepted,
   * the session kept reading the engine still bound to the old files, and the next open built a fresh, empty engine under the new
   * name, stranding every sample in files no component pointed at. Under HA the session that ships a rename recorded none of those
   * files either.
   * <p>
   * The census is over every concrete kind of type, enumerated from the classes of the schema package rather than from a list here:
   * write a record, rename, reopen, and the record is still read under the new name AND no file of the database still carries the old
   * one. Reading back in the same session, or checking only that the type answers to the new name, passes without the fix.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class TypeRenameMovesEveryOwnedFile extends TestHelper {
    private static final String OLD = "Old10749";
    private static final String NEW = "New10749";

    @Test
    void everyKindOfTypeKeepsItsRecordsAndLeavesNoFileUnderTheOldName() throws Exception {
      final Map<Class<?>, Kind> kinds = new LinkedHashMap<>();
      kinds.put(LocalDocumentType.class, new Kind(
          name -> database.command("sql", "CREATE DOCUMENT TYPE " + name),
          name -> database.transaction(() -> database.newDocument(name).set("id", 1).save())));
      kinds.put(LocalVertexType.class, new Kind(
          name -> database.command("sql", "CREATE VERTEX TYPE " + name),
          name -> database.transaction(() -> {
            final MutableVertex a = database.newVertex(name).set("id", 1).save();
            a.newEdge("Link10749", database.newVertex(name).set("id", 2).save()).save();
          })));
      kinds.put(LocalEdgeType.class, new Kind(
          name -> database.command("sql", "CREATE EDGE TYPE " + name),
          name -> database.transaction(() -> database.newVertex("Node10749").save().newEdge(name, database.newVertex("Node10749").save())
              .set("id", 1).save())));
      kinds.put(LocalTimeSeriesType.class, new Kind(
          name -> database.command("sql",
              "CREATE TIMESERIES TYPE " + name + " TIMESTAMP ts PRECISION MILLISECOND TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 2"),
          name -> database.command("sql", "INSERT INTO " + name + " SET ts = 1000, host = 'h1', v = 1.5")));

      // a new kind of type has to say here how it is created and written, or this fails: its files are what the census is about
      for (final Class<?> concrete : concreteTypeClasses())
        assertThat(kinds).as("a census entry for %s", concrete.getSimpleName()).containsKey(concrete);

      database.command("sql", "CREATE VERTEX TYPE Node10749");
      database.command("sql", "CREATE EDGE TYPE Link10749");

      int round = 0;
      for (final Map.Entry<Class<?>, Kind> kind : kinds.entrySet()) {
        final String oldName = OLD + "K" + round;
        final String newName = NEW + "K" + round++;
        kind.getValue().create().accept(oldName);
        kind.getValue().write().accept(oldName);
        final long records = count(oldName);
        assertThat(records).as("%s written", kind.getKey().getSimpleName()).isPositive();

        database.command("sql", "ALTER TYPE " + oldName + " NAME " + newName);
        assertThat(count(newName)).as("%s in the session of the rename", kind.getKey().getSimpleName()).isEqualTo(records);

        reopenDatabase();

        assertThat(database.getSchema().getType(newName)).as("%s", kind.getKey().getSimpleName()).isInstanceOf(kind.getKey());
        assertThat(count(newName)).as("%s after a reopen", kind.getKey().getSimpleName()).isEqualTo(records);
        assertThat(filesNaming(oldName)).as("files of %s still named after '%s'", kind.getKey().getSimpleName(), oldName).isEmpty();
      }
    }

    /**
     * The samples of a TIMESERIES type live in two layers per shard, and the rename has to carry both: the sealed blocks a compaction
     * wrote, which are not a component at all but a file beside the shard's mutable bucket, and the samples still in the mutable bucket.
     * A sample written under the new name lands in the same files, and the tag dictionary keeps resolving the tags of the old ones.
     */
    @Test
    void aTimeSeriesTypeKeepsItsSealedAndMutableSamplesAcrossARenameAndAReopen() throws Exception {
      database.command("sql",
          "CREATE TIMESERIES TYPE " + OLD + " TIMESTAMP ts PRECISION MILLISECOND TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 2");
      for (int i = 0; i < 6; i++)
        insert(OLD, 1000L * (i + 1), "h" + (i % 3), i);
      engineOf(OLD).compactAll();
      insert(OLD, 10_000L, "h1", 100);
      assertThat(sealedBlocks(OLD)).as("some samples are sealed").isPositive();

      database.command("sql", "ALTER TYPE " + OLD + " NAME " + NEW);

      assertThat(filesNaming(OLD)).as("no file is left under the old name, in the session of the rename").isEmpty();
      insert(NEW, 11_000L, "h2", 200);
      assertThat(count(NEW)).isEqualTo(8);

      reopenDatabase();

      assertThat(count(NEW)).isEqualTo(8);
      assertThat(sealedBlocks(NEW)).as("the sealed blocks are found under the new name").isPositive();
      assertThat(hosts(NEW)).containsExactlyInAnyOrder("h0", "h1", "h2");
      assertThat(sumOfV(NEW)).isEqualTo(0 + 1 + 2 + 3 + 4 + 5 + 100 + 200);
      assertThat(filesNaming(OLD)).isEmpty();

      // and back: the files follow the name whichever way it goes
      database.command("sql", "ALTER TYPE " + NEW + " NAME " + OLD);
      insert(OLD, 12_000L, "h0", 300);
      reopenDatabase();
      assertThat(count(OLD)).isEqualTo(9);
      assertThat(filesNaming(NEW)).isEmpty();

      // a type created under the name the first one left takes none of its files
      database.command("sql",
          "CREATE TIMESERIES TYPE " + NEW + " TIMESTAMP ts PRECISION MILLISECOND TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 2");
      assertThat(count(NEW)).isZero();
      assertThat(count(OLD)).isEqualTo(9);

      database.command("sql", "DROP TYPE " + OLD);
      database.command("sql", "DROP TYPE " + NEW);
      assertThat(filesNaming(OLD)).as("the drop of a renamed type deletes every file it owns").isEmpty();
      assertThat(filesNaming(NEW)).isEmpty();
    }

    /**
     * Under HA the session that publishes a rename ships the renames it recorded, and a follower moves the same files. The engine's
     * components (the mutable bucket of each shard and the tag dictionary) are recorded with the type's buckets and index files.
     */
    @Test
    void theRenameRecordsTheComponentsOfTheEngine() throws Exception {
      database.command("sql",
          "CREATE TIMESERIES TYPE " + OLD + " TIMESTAMP ts PRECISION MILLISECOND TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 2");
      insert(OLD, 1000L, "h1", 1);
      final TimeSeriesEngine engine = engineOf(OLD);
      final List<Integer> engineFiles = new ArrayList<>();
      for (int i = 0; i < engine.getShardCount(); i++)
        engineFiles.add(engine.getShard(i).getMutableBucket().getFileId());
      engineFiles.add(engine.getTagDictionary().getFileId());

      final FileManager fileManager = ((DatabaseInternal) database).getFileManager();
      final Map<Integer, String> recorded;
      assertThat(fileManager.startRecordingChanges()).isTrue();
      try {
        database.getSchema().getType(OLD).rename(NEW);
        recorded = fileManager.getRecordedRenames();
      } finally {
        fileManager.stopRecordingChanges();
      }

      for (final Integer fileId : engineFiles)
        assertThat(recorded).as("engine file %d", fileId).containsEntry(fileId, fileManager.getFile(fileId).getComponentName());
      assertThat(recorded.values()).allMatch(name -> name.startsWith(NEW + "_"));
    }

    /**
     * A follower moves the files a rename recorded through the component of each, by file id, before it loads the schema that names
     * them: the sealed store is not a component, so the move of the mutable bucket it belongs to has to carry it, or the follower's
     * engine opens an empty sealed store under the new name and every sealed sample of the type is lost there.
     */
    @Test
    void movingAShardBucketByItsComponentCarriesItsSealedStore() throws Exception {
      database.command("sql",
          "CREATE TIMESERIES TYPE " + OLD + " TIMESTAMP ts PRECISION MILLISECOND TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1");
      for (int i = 0; i < 4; i++)
        insert(OLD, 1000L * (i + 1), "h1", i);
      engineOf(OLD).compactAll();

      final TimeSeriesShard shard = engineOf(OLD).getShard(0);
      final TimeSeriesBucket bucket = shard.getMutableBucket();
      final int blocks = shard.getSealedStore().getBlockCount();
      assertThat(blocks).isPositive();

      final File databaseDirectory = new File(database.getDatabasePath());
      bucket.rename(NEW + "_shard_0");

      assertThat(new File(databaseDirectory, OLD + "_shard_0.ts.sealed")).doesNotExist();
      assertThat(new File(databaseDirectory, NEW + "_shard_0.ts.sealed")).exists();
      // the live store follows its file: the samples are still read
      assertThat(shard.getSealedStore().getBlockCount()).isEqualTo(blocks);
      assertThat(shard.countSamples()).isEqualTo(4);

      bucket.rename(OLD + "_shard_0");
      assertThat(new File(databaseDirectory, OLD + "_shard_0.ts.sealed")).exists();
      assertThat(count(OLD)).isEqualTo(4);
    }

    /**
     * A type whose engine could not start (issue #6356) is still renamed with its files: they are moved through their components,
     * by the names the engine would look them up by, so the repair that brings the engine back finds the samples under the new name.
     */
    @Test
    void aTypeWhoseEngineCouldNotStartIsRenamedWithItsFiles() throws Exception {
      database.command("sql",
          "CREATE TIMESERIES TYPE " + OLD + " TIMESTAMP ts PRECISION MILLISECOND TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1");
      for (int i = 0; i < 5; i++)
        insert(OLD, 1000L * (i + 1), "h" + i, i);
      engineOf(OLD).compactAll();
      insert(OLD, 9000L, "h1", 9);

      final File directory = new File(database.getDatabasePath());
      final Path oldSealed = new File(directory, OLD + "_shard_0.ts.sealed").toPath();
      database.close();
      final byte[] goodSealed = Files.readAllBytes(oldSealed);
      final byte[] garbage = new byte[goodSealed.length];
      Arrays.fill(garbage, (byte) 0x7F);
      Files.write(oldSealed, garbage);
      database = factory.open();

      final LocalTimeSeriesType broken = (LocalTimeSeriesType) database.getSchema().getType(OLD);
      assertThat(broken.isEngineAvailable()).as("the engine must have failed to open, or this test proves nothing").isFalse();

      database.command("sql", "ALTER TYPE " + OLD + " NAME " + NEW);
      assertThat(filesNaming(OLD)).isEmpty();
      assertThat(new File(directory, NEW + "_shard_0.ts.sealed")).exists();

      // the repair, as the HA path installs a good sealed file: under the name the type now has
      Files.write(new File(directory, NEW + "_shard_0.ts.sealed").toPath(), goodSealed);
      broken.initEngine();
      assertThat(count(NEW)).isEqualTo(6);

      reopenDatabase();
      assertThat(count(NEW)).isEqualTo(6);
      assertThat(filesNaming(OLD)).isEmpty();
    }

    @Test
    void aRenameOntoTheFilesOfAnotherTypeIsRefusedBeforeAnythingMoves() throws Exception {
      database.command("sql",
          "CREATE TIMESERIES TYPE " + OLD + " TIMESTAMP ts PRECISION MILLISECOND TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1");
      insert(OLD, 1000L, "h1", 1);
      // a sealed file left in the directory under the target name, by nothing this database still knows
      final File stray = new File(database.getDatabasePath(), NEW + "_shard_0.ts.sealed");
      Files.write(stray.toPath(), new byte[] { 1, 2, 3 });

      assertThatThrownBy(() -> database.command("sql", "ALTER TYPE " + OLD + " NAME " + NEW)).isInstanceOf(SchemaException.class)
          .hasMessageContaining(stray.getName()).hasMessageContaining("Nothing was changed");

      assertThat(database.getSchema().existsType(OLD)).isTrue();
      assertThat(database.getSchema().existsType(NEW)).isFalse();
      assertThat(filesNaming(OLD)).isNotEmpty();
      assertThat(count(OLD)).isEqualTo(1);
      Files.delete(stray.toPath());
    }

    /**
     * The sealed file cannot move (a file already holds the target name): the component is moved back, the sealed file stays where it
     * was and the open store goes on reading it there.
     */
    @Test
    void aSealedFileThatCannotMoveTakesTheBucketBack() throws Exception {
      database.command("sql",
          "CREATE TIMESERIES TYPE " + OLD + " TIMESTAMP ts PRECISION MILLISECOND TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1");
      for (int i = 0; i < 4; i++)
        insert(OLD, 1000L * (i + 1), "h1", i);
      engineOf(OLD).compactAll();
      final TimeSeriesShard shard = engineOf(OLD).getShard(0);
      final File stray = new File(database.getDatabasePath(), NEW + "_shard_0.ts.sealed");
      Files.write(stray.toPath(), new byte[] { 1, 2, 3 });

      assertThatThrownBy(() -> shard.getMutableBucket().rename(NEW + "_shard_0")).isInstanceOf(IOException.class)
          .hasMessageContaining("already exists");

      assertThat(shard.getMutableBucket().getName()).isEqualTo(OLD + "_shard_0");
      assertThat(new File(database.getDatabasePath(), OLD + "_shard_0.ts.sealed")).exists();
      assertThat(shard.countSamples()).isEqualTo(4);
      assertThat(count(OLD)).isEqualTo(4);
      Files.delete(stray.toPath());
    }

    /**
     * The engine stays open through a rename: samples appended, compactions sealing them and reads of them while the files move go on,
     * and every sample is there, after a reopen too.
     */
    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS) // a hang detector for the waits on the appender, not a latency bound
    void samplesAppendedWhileTheTypeIsRenamedAreKept() throws Exception {
      database.command("sql",
          "CREATE TIMESERIES TYPE " + OLD + " TIMESTAMP ts PRECISION MILLISECOND TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 2");
      final TimeSeriesEngine engine = engineOf(OLD);
      for (int i = 0; i < 50; i++)
        engine.appendSamples(new long[] { 1L + i }, new Object[] { "h" + (i % 3) }, new Object[] { (double) i });
      engine.compactAll();

      final AtomicBoolean stop = new AtomicBoolean();
      final AtomicLong appended = new AtomicLong(50);
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final Thread appender = new Thread(() -> {
        long ts = 10_000;
        try {
          while (!stop.get()) {
            engine.appendSamples(new long[] { ts }, new Object[] { "h" + (ts % 3) }, new Object[] { (double) ts });
            ++ts;
            final long total = appended.incrementAndGet();
            if (total % 20 == 0) {
              engine.compactAll();
              assertThat(engine.countSamples()).isEqualTo(total);
            }
          }
        } catch (final Throwable t) {
          failure.set(t);
        }
      });
      appender.start();
      try {
        while (appended.get() < 100 && failure.get() == null)
          Thread.onSpinWait();
        database.command("sql", "ALTER TYPE " + OLD + " NAME " + NEW);
        final long atRename = appended.get();
        while (appended.get() < atRename + 50 && failure.get() == null)
          Thread.onSpinWait();
      } finally {
        stop.set(true);
        appender.join();
      }

      assertThat(failure.get()).as("an append failed while the type was renamed").isNull();
      assertThat(count(NEW)).isEqualTo(appended.get());
      reopenDatabase();
      assertThat(count(NEW)).isEqualTo(appended.get());
      assertThat(filesNaming(OLD)).isEmpty();
    }

    @Test
    void theMaintenanceOfARenamedTypeFollowsItsName() {
      database.command("sql",
          "CREATE TIMESERIES TYPE " + OLD + " TIMESTAMP ts PRECISION MILLISECOND TAGS (host STRING) FIELDS (v DOUBLE) RETENTION 1 DAYS");
      final var scheduler = ((LocalSchema) database.getSchema()).getTimeSeriesMaintenanceScheduler();
      assertThat(scheduler.isScheduled(OLD)).isTrue();

      database.command("sql", "ALTER TYPE " + OLD + " NAME " + NEW);

      assertThat(scheduler.isScheduled(NEW)).isTrue();
      assertThat(scheduler.isScheduled(OLD)).isFalse();

      // a type created under the old name schedules its own maintenance, and leaves the renamed type's alone
      database.command("sql",
          "CREATE TIMESERIES TYPE " + OLD + " TIMESTAMP ts PRECISION MILLISECOND TAGS (host STRING) FIELDS (v DOUBLE)");
      assertThat(scheduler.isScheduled(OLD)).isTrue();
      assertThat(scheduler.isScheduled(NEW)).isTrue();
    }

    private record Kind(Consumer<String> create, Consumer<String> write) {
    }

    /** The concrete {@link LocalDocumentType} classes of the schema package: every kind of type the schema can hold. */
    private static List<Class<?>> concreteTypeClasses() throws Exception {
      // the classes directory of the main code, not the first entry of the classpath holding the package: the tests share the package
      final URL url = LocalDocumentType.class.getProtectionDomain().getCodeSource().getLocation();
      if (!"file".equals(url.getProtocol()) || !new File(url.toURI()).isDirectory())
        fail("The schema classes are not in a directory of the classpath (" + url + "), so they cannot be listed");

      final List<Class<?>> classes = new ArrayList<>();
      final File packageDirectory = new File(new File(url.toURI()), LocalDocumentType.class.getPackageName().replace('.', File.separatorChar));
      final File[] files = packageDirectory.listFiles((dir, name) -> name.endsWith(".class") && !name.contains("$"));
      assertThat(files).isNotNull();
      for (final File file : files) {
        final Class<?> c = Class.forName(LocalDocumentType.class.getPackageName() + "." + file.getName().replace(".class", ""), false,
            LocalDocumentType.class.getClassLoader());
        if (LocalDocumentType.class.isAssignableFrom(c) && !Modifier.isAbstract(c.getModifiers()) && !c.isInterface())
          classes.add(c);
      }
      assertThat(classes).contains(LocalDocumentType.class, LocalTimeSeriesType.class);
      return classes;
    }

    /** The files of the database directory whose name starts with the type name, followed by a delimiter of a component name. */
    private List<String> filesNaming(final String typeName) {
      final List<String> names = new ArrayList<>();
      final File[] files = new File(database.getDatabasePath()).listFiles();
      if (files != null)
        for (final File file : files)
          if (file.getName().startsWith(typeName + "_") || file.getName().startsWith(typeName + "."))
            names.add(file.getName());
      return names;
    }

    private TimeSeriesEngine engineOf(final String typeName) {
      return ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine();
    }

    private int sealedBlocks(final String typeName) {
      final TimeSeriesEngine engine = engineOf(typeName);
      int blocks = 0;
      for (int i = 0; i < engine.getShardCount(); i++)
        blocks += engine.getShard(i).getSealedStore().getBlockCount();
      return blocks;
    }

    private void insert(final String typeName, final long ts, final String host, final double v) {
      database.command("sql", "INSERT INTO " + typeName + " SET ts = ?, host = ?, v = ?", ts, host, v);
    }

    private long count(final String typeName) {
      try (final ResultSet rs = database.query("sql", "SELECT count() AS c FROM " + typeName)) {
        return rs.next().<Number>getProperty("c").longValue();
      }
    }

    private List<String> hosts(final String typeName) {
      final List<String> hosts = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT DISTINCT(host) AS h FROM " + typeName)) {
        while (rs.hasNext())
          hosts.add(rs.next().getProperty("h"));
      }
      return hosts;
    }

    private double sumOfV(final String typeName) {
      try (final ResultSet rs = database.query("sql", "SELECT sum(v) AS s FROM " + typeName)) {
        return rs.next().<Number>getProperty("s").doubleValue();
      }
    }
  }

  /**
   * Regression for #10751: a type rename gave every {@link TypeIndex} of the type its new name and left {@code LocalSchema.indexMap} keyed by
   * the old one. The index answered to a name nothing resolved: {@code DROP INDEX} and {@code REBUILD INDEX} with the name the catalog
   * printed failed, the old name dropped the index of a type that no longer existed, a new type under the freed name could not index
   * the same property, a {@code DROP TYPE} of the renamed type left the wrapper behind and broke {@code schema:indexes}, and a manual
   * index name was replaced by the derived one and lost for good at the next reopen.
   * <p>
   * The guarantee the census pins: after a rename, every index the schema lists is reached through the name it answers to, and no
   * name it answered to before resolves any longer. The census walks the index kinds from the schema's own factory, so a kind added
   * there is covered without touching this test.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class TypeRenameRekeysIndexMap extends TestHelper {

    @Test
    void aDefaultNamedIndexAnswersToItsNewNameOnly() {
      database.command("sql", "CREATE DOCUMENT TYPE Foo");
      database.command("sql", "CREATE PROPERTY Foo.a STRING");
      database.command("sql", "CREATE INDEX ON Foo (a) UNIQUE");
      database.transaction(() -> database.newDocument("Foo").set("a", "x").save());
      final Index index = database.getSchema().getIndexByName("Foo[a]");

      database.command("sql", "ALTER TYPE Foo NAME Bar");

      assertThat(index.getName()).isEqualTo("Bar[a]");
      assertThat(database.getSchema().existsIndex("Bar[a]")).isTrue();
      assertThat(database.getSchema().getIndexByName("Bar[a]")).isSameAs(index);
      assertThat(database.getSchema().existsIndex("Foo[a]")).isFalse();
      assertEveryIndexResolvesToItself();

      // the old name is not a way to reach the index of a type that no longer exists under it
      assertThatThrownBy(() -> database.command("sql", "DROP INDEX `Foo[a]`")).isInstanceOf(SchemaException.class)
          .hasMessageContaining("Index not found");
      assertThat(database.getSchema().existsIndex("Bar[a]")).isTrue();

      database.command("sql", "REBUILD INDEX `Bar[a]`");
      assertThat(database.query("sql", "SELECT FROM Bar WHERE a = 'x'").stream().count()).isEqualTo(1L);

      database.command("sql", "DROP INDEX `Bar[a]`");
      assertThat(database.getSchema().existsIndex("Bar[a]")).isFalse();
      assertThat(database.getSchema().getType("Bar").getAllIndexes(true)).isEmpty();
    }

    @Test
    void theFreedNameCanBeIndexedAgainAndTheRenamedTypeDropsCleanly() {
      database.command("sql", "CREATE DOCUMENT TYPE Foo");
      database.command("sql", "CREATE PROPERTY Foo.a STRING");
      database.command("sql", "CREATE INDEX ON Foo (a) UNIQUE");
      database.command("sql", "ALTER TYPE Foo NAME Bar");

      database.command("sql", "CREATE DOCUMENT TYPE Foo");
      database.command("sql", "CREATE PROPERTY Foo.a STRING");
      database.command("sql", "CREATE INDEX ON Foo (a) UNIQUE");
      assertThat(database.getSchema().getIndexByName("Foo[a]").getTypeName()).isEqualTo("Foo");
      assertThat(database.getSchema().getIndexByName("Bar[a]").getTypeName()).isEqualTo("Bar");
      assertEveryIndexResolvesToItself();

      database.command("sql", "DROP TYPE Bar");

      assertThat(database.getSchema().existsIndex("Bar[a]")).isFalse();
      // the catalog lists the type index and its bucket sub-index, both of the new Foo, and answers at all
      assertThat(indexNamesOfTheCatalog()).contains("Foo[a]").allMatch(n -> n.startsWith("Foo"));
      assertEveryIndexResolvesToItself();

      reopenDatabase();
      assertThat(indexNamesOfTheCatalog()).contains("Foo[a]").allMatch(n -> n.startsWith("Foo"));
    }

    @Test
    void aManualIndexNameSurvivesTheRenameAndAReopen() {
      database.command("sql", "CREATE DOCUMENT TYPE Foo");
      database.command("sql", "CREATE PROPERTY Foo.a STRING");
      database.command("sql", "CREATE INDEX MyIdx ON Foo (a) UNIQUE");
      database.transaction(() -> database.newDocument("Foo").set("a", "x").save());

      database.command("sql", "ALTER TYPE Foo NAME Bar");

      assertThat(database.getSchema().existsIndex("MyIdx")).isTrue();
      assertThat(database.getSchema().getIndexByName("MyIdx").getName()).isEqualTo("MyIdx");
      assertThat(database.getSchema().existsIndex("Bar[a]")).isFalse();
      assertEveryIndexResolvesToItself();

      reopenDatabase();

      assertThat(database.getSchema().existsIndex("MyIdx")).isTrue();
      assertThat(database.getSchema().existsIndex("Bar[a]")).isFalse();
      assertThat(database.getSchema().getIndexByName("MyIdx").getTypeName()).isEqualTo("Bar");
      assertThat(database.query("sql", "SELECT FROM Bar WHERE a = 'x'").stream().count()).isEqualTo(1L);
      assertEveryIndexResolvesToItself();

      // and a second rename keeps it too
      database.command("sql", "ALTER TYPE Bar NAME Baz");
      assertThat(database.getSchema().getIndexByName("MyIdx").getTypeName()).isEqualTo("Baz");
      reopenDatabase();
      assertThat(database.getSchema().existsIndex("MyIdx")).isTrue();
      assertThat(database.getSchema().existsIndex("Baz[a]")).isFalse();
    }

    @Test
    void aRenameBackRestoresTheOriginalNames() {
      database.command("sql", "CREATE DOCUMENT TYPE Foo");
      database.command("sql", "CREATE PROPERTY Foo.a STRING");
      database.command("sql", "CREATE INDEX ON Foo (a) UNIQUE");

      database.command("sql", "ALTER TYPE Foo NAME Bar");
      database.command("sql", "ALTER TYPE Bar NAME Foo");

      assertThat(database.getSchema().existsIndex("Foo[a]")).isTrue();
      assertThat(database.getSchema().existsIndex("Bar[a]")).isFalse();
      assertEveryIndexResolvesToItself();
    }

    /**
     * The name a default-named index would take is held by a manually named index of another type: the rename is refused before it
     * changes anything, instead of taking the name over and leaving the other index unreachable by it.
     */
    @Test
    void aRenameWhoseIndexNameIsTakenIsRefusedAndChangesNothing() {
      database.command("sql", "CREATE DOCUMENT TYPE X");
      database.command("sql", "CREATE PROPERTY X.a STRING");
      database.command("sql", "CREATE INDEX `Bar[a]` ON X (a) UNIQUE");
      database.command("sql", "CREATE DOCUMENT TYPE Foo");
      database.command("sql", "CREATE PROPERTY Foo.a STRING");
      database.command("sql", "CREATE INDEX ON Foo (a) UNIQUE");
      final Index taken = database.getSchema().getIndexByName("Bar[a]");
      final Index fooIndex = database.getSchema().getIndexByName("Foo[a]");

      assertThatThrownBy(() -> database.command("sql", "ALTER TYPE Foo NAME Bar")).isInstanceOf(AlreadyExistsException.class)
          .hasMessageContaining("Bar[a]");

      assertThat(database.getSchema().existsType("Foo")).isTrue();
      assertThat(database.getSchema().existsType("Bar")).isFalse();
      assertThat(database.getSchema().getIndexByName("Bar[a]")).isSameAs(taken);
      assertThat(database.getSchema().getIndexByName("Foo[a]")).isSameAs(fooIndex);
      assertEveryIndexResolvesToItself();

      reopenDatabase();
      assertThat(database.getSchema().getIndexByName("Bar[a]").getTypeName()).isEqualTo("X");
      assertThat(database.getSchema().getIndexByName("Foo[a]").getTypeName()).isEqualTo("Foo");
    }

    /** A rename refused after the type index took its new name puts the map back under the old one (the rollback of the rename). */
    @Test
    void aRefusedRenameLeavesTheIndexUnderItsOldName() throws IOException {
      database.command("sql", "CREATE DOCUMENT TYPE Foo BUCKETS 2");
      database.command("sql", "CREATE PROPERTY Foo.a STRING");
      database.command("sql", "CREATE INDEX ON Foo (a) UNIQUE");
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Foo[a]");
      // the file of the LAST sub-index cannot take its new name: the type index and the first sub-index are renamed before the refusal
      final IndexInternal last = index.getIndexesOnBuckets()[1];
      final Path file = Path.of(((DatabaseInternal) database).getFileManager().getFile(last.getFileId()).getFilePath());
      final Path blocked = file.resolveSibling(file.getFileName().toString().replace("Foo_", "Bar_"));
      Files.createDirectory(blocked);
      try {
        assertThatThrownBy(() -> database.getSchema().getType("Foo").rename("Bar")).isInstanceOf(SchemaException.class);

        assertThat(database.getSchema().existsType("Foo")).isTrue();
        assertThat(index.getName()).isEqualTo("Foo[a]");
        assertThat(database.getSchema().getIndexByName("Foo[a]")).isSameAs(index);
        assertThat(database.getSchema().existsIndex("Bar[a]")).isFalse();
        assertEveryIndexResolvesToItself();
      } finally {
        Files.deleteIfExists(blocked);
      }

      reopenDatabase();
      assertThat(database.getSchema().existsIndex("Foo[a]")).isTrue();
      assertThat(database.getSchema().existsIndex("Bar[a]")).isFalse();
      assertEveryIndexResolvesToItself();
    }

    /**
     * Every index kind the schema can build, each on a type of its own, the types renamed: every index answers to a name that reaches
     * it, and none of the names the type indexes answered to before the rename does. Then again after a reopen.
     */
    @Test
    void censusOfTheRegisteredIndexKinds() {
      final Set<String> kinds = ((LocalSchema) database.getSchema().getEmbedded()).indexFactory.getRegisteredTypes();
      assertThat(kinds).as("the census reads the kinds from the factory").isNotEmpty();

      final List<String> oldNames = new ArrayList<>();
      for (final String kind : kinds) {
        final String typeName = "T_" + kind;
        createIndexOfKind(typeName, Schema.INDEX_TYPE.valueOf(kind));
        for (final TypeIndex index : database.getSchema().getType(typeName).getAllIndexes(false))
          oldNames.add(index.getName());
      }
      assertThat(oldNames).as("one type index per registered kind").hasSize(kinds.size());

      for (final String kind : kinds)
        database.command("sql", "ALTER TYPE `T_" + kind + "` NAME `R_" + kind + "`");

      for (final String kind : kinds) {
        final List<TypeIndex> indexes = new ArrayList<>(database.getSchema().getType("R_" + kind).getAllIndexes(false));
        assertThat(indexes).as("kind %s keeps its index", kind).hasSize(1);
        assertThat(indexes.getFirst().getName()).as("kind %s", kind).startsWith("R_" + kind + "[");
      }
      for (final String oldName : oldNames)
        assertThat(database.getSchema().existsIndex(oldName)).as("the retired name %s", oldName).isFalse();
      assertEveryIndexResolvesToItself();

      reopenDatabase();

      for (final String oldName : oldNames)
        assertThat(database.getSchema().existsIndex(oldName)).as("the retired name %s after a reopen", oldName).isFalse();
      for (final String kind : kinds)
        assertThat(database.getSchema().getType("R_" + kind).getAllIndexes(false)).as("kind %s after a reopen", kind).hasSize(1);
      assertEveryIndexResolvesToItself();
    }

    // ---- helpers

    private void createIndexOfKind(final String typeName, final Schema.INDEX_TYPE kind) {
      final String t = "`" + typeName + "`";
      database.command("sql", "CREATE DOCUMENT TYPE " + t);
      switch (kind) {
      case LSM_TREE -> {
        database.command("sql", "CREATE PROPERTY " + t + ".s STRING");
        database.command("sql", "CREATE INDEX ON " + t + " (s) UNIQUE");
      }
      case HASH -> {
        database.command("sql", "CREATE PROPERTY " + t + ".s STRING");
        database.command("sql", "CREATE INDEX ON " + t + " (s) UNIQUE_HASH");
      }
      case FULL_TEXT -> {
        database.command("sql", "CREATE PROPERTY " + t + ".s STRING");
        database.command("sql", "CREATE INDEX ON " + t + " (s) FULL_TEXT");
      }
      case GEOSPATIAL -> {
        database.command("sql", "CREATE PROPERTY " + t + ".s STRING");
        database.command("sql", "CREATE INDEX ON " + t + " (s) GEOSPATIAL");
      }
      case LSM_VECTOR -> {
        database.command("sql", "CREATE PROPERTY " + t + ".v ARRAY_OF_FLOATS");
        database.command("sql", "CREATE INDEX ON " + t + " (v) LSM_VECTOR METADATA { dimensions: 4, similarity: 'COSINE' }");
      }
      case LSM_SPARSE_VECTOR -> {
        database.command("sql", "CREATE PROPERTY " + t + ".tokens ARRAY_OF_INTEGERS");
        database.command("sql", "CREATE PROPERTY " + t + ".weights ARRAY_OF_FLOATS");
        database.command("sql", "CREATE INDEX ON " + t + " (tokens, weights) LSM_SPARSE_VECTOR METADATA { dimensions: 16 }");
      }
      case COLUMNAR -> {
        // its derived name carries the _columnar suffix, which the rename keeps: R_COLUMNAR[s]_columnar
        database.command("sql", "CREATE PROPERTY " + t + ".s STRING");
        database.command("sql", "CREATE INDEX ON " + t + " (s) COLUMNAR");
      }
      default -> throw new IllegalStateException("index kind " + kind + " registered in the factory and not covered by this census");
      }
    }

    /** Every index the schema lists - type indexes and their bucket sub-indexes - is the one its own name resolves to. */
    private void assertEveryIndexResolvesToItself() {
      final Map<IndexInternal, Boolean> seen = new IdentityHashMap<>();
      for (final Index index : database.getSchema().getIndexes()) {
        assertThat(database.getSchema().existsIndex(index.getName())).as("index %s", index.getName()).isTrue();
        assertThat(database.getSchema().getIndexByName(index.getName())).as("index %s", index.getName()).isSameAs(index);
        seen.put((IndexInternal) index, true);
      }
      for (final DocumentType type : database.getSchema().getTypes())
        for (final TypeIndex index : type.getAllIndexes(false)) {
          assertThat(seen).as("type index %s of %s is listed", index.getName(), type.getName()).containsKey(index);
          for (final IndexInternal subIndex : index.getIndexesOnBuckets())
            assertThat(seen).as("sub-index %s is listed", subIndex.getName()).containsKey(subIndex);
        }
    }

    private List<String> indexNamesOfTheCatalog() {
      final List<String> names = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT FROM schema:indexes")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          names.add(row.getProperty("name"));
        }
      }
      return names;
    }
  }

  /**
   * Regression for #6667: a vertex type rename must not corrupt the edge-chunk bucket names of the type.
   * <p>
   * Standalone reproduction of the exact scenario reported in https://github.com/ArcadeData/arcadedb/issues/6667:
   * a vertex type rename must not corrupt the type's edge-chunk bucket names, or an edge insert on the renamed
   * type afterward fails with SchemaException. Kept separate from {@link TypeRenameComponentNamingTest} because it
   * mirrors the issue's own reproducer verbatim, including the restart step the issue's suggested fix #4 calls
   * out ("insert an edge, restart, insert another edge, and assert the bucket names follow the convention").
   * <p>
   * Extends {@link TestHelper} so {@code afterTest()} runs {@code CHECK DATABASE} on teardown: the failure mode
   * this test guards against is bucket/file corruption, which a cached record count would not necessarily surface.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Repro extends TestHelper {

    @Test
    void renamedVertexTypeCanStillInsertEdges() {
      database.getSchema().createVertexType("Person");
      database.getSchema().createEdgeType("Knows");

      database.transaction(() -> {
        final MutableVertex v1 = database.newVertex("Person").set("uid", "a").save();
        final MutableVertex v2 = database.newVertex("Person").set("uid", "b").save();
        v1.newEdge("Knows", v2).save();
      });

      database.getSchema().getType("Person").rename("Human");

      database.transaction(() -> {
        final MutableVertex v3 = database.newVertex("Human").set("uid", "c").save();
        final MutableVertex v4 = database.newVertex("Human").set("uid", "d").save();
        v3.newEdge("Knows", v4).save();
      });

      assertThat(database.query("sql", "select from Human").stream().count()).isEqualTo(4L);

      final String databasePath = database.getDatabasePath();
      database.close();
      database = new DatabaseFactory(databasePath).open();

      database.transaction(() -> {
        final MutableVertex v5 = database.newVertex("Human").set("uid", "e").save();
        final MutableVertex v6 = database.newVertex("Human").set("uid", "f").save();
        v5.newEdge("Knows", v6).save();
      });

      assertThat(database.query("sql", "select from Human").stream().count()).isEqualTo(6L);

      final var edgeBuckets = database.getSchema().getType("Human").getInvolvedBuckets().stream()
          .map(b -> b.getName()).filter(n -> n.endsWith("_out_edges") || n.endsWith("_in_edges")).toList();
      assertThat(edgeBuckets).containsExactlyInAnyOrder("Human_0_out_edges", "Human_0_in_edges");
    }
  }
}
