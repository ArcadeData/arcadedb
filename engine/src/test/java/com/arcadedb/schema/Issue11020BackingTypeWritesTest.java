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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11020: {@code TRUNCATE TYPE} on the backing type of a continuous aggregate emptied it and left its watermark where it was,
 * so the next refresh recomputed only the buckets at or after the watermark: every bucket behind it was lost for good, with the
 * aggregate reporting VALID. A materialized view truncated the same way answered no rows while reporting VALID, and a row
 * INSERTed into a view was answered beside the rows of its query. {@code DELETE FROM} the aggregate lost the same buckets as the
 * truncate.
 * <p>
 * A backing type is now written by its owner's refresh and by nothing else, as PostgreSQL refuses to change a materialized view:
 * every record insert, update and delete, {@code TRUNCATE TYPE} (of the type, or reaching it POLYMORPHIC), {@code TRUNCATE BUCKET}
 * and the removal of one of its buckets is refused, naming the statement that does the job.
 * <p>
 * Issue #11204: a row can be in the backing type's answer without being written to it, by a type linked below the backing type
 * ({@code CREATE ... EXTENDS}, {@code ALTER TYPE ... SUPERTYPE +}): that link is refused.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11020BackingTypeWritesTest extends TestHelper {
  private static final long HOUR = 3_600_000L;
  /** 2023-11-14T22:13:20Z, the first sample of the issue. */
  private static final long T0   = 1_700_000_000_000L;

  // ---- continuous aggregate: the issue's reproduction

  @Test
  void truncateOfAnAggregateBackingTypeIsRefusedAndNoBucketIsLost() {
    createAggregate();
    assertThat(aggregateRows()).containsExactly(1.0, 2.0, 3.0);

    assertThatThrownBy(() -> database.command("sql", "TRUNCATE TYPE ca")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("backing type for continuous aggregate 'ca'").hasMessageContaining("REFRESH CONTINUOUS AGGREGATE ca")
        .hasMessageContaining("DROP CONTINUOUS AGGREGATE ca");
    assertThatThrownBy(() -> database.command("sql", "TRUNCATE TYPE ca UNSAFE")).isInstanceOf(SchemaException.class);

    assertThat(aggregateRows()).containsExactly(1.0, 2.0, 3.0);
    assertThat(status("continuousAggregates", "ca")).isEqualTo("VALID");

    // and the refreshes the operator runs next keep every bucket
    database.command("sql", "REFRESH CONTINUOUS AGGREGATE ca");
    database.command("sql", "REFRESH CONTINUOUS AGGREGATE ca");
    assertThat(aggregateRows()).containsExactly(1.0, 2.0, 3.0);
  }

  @Test
  void deleteFromAnAggregateBackingTypeIsRefusedAndNoBucketIsLost() {
    createAggregate();

    assertThatThrownBy(() -> database.command("sql", "DELETE FROM ca")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("Cannot delete from type 'ca'");
    assertThatThrownBy(() -> database.command("sql", "DELETE FROM ca WHERE av < 3")).isInstanceOf(SchemaException.class);
    assertThatThrownBy(() -> database.command("sql", "UPDATE ca SET av = 42")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("Cannot update a record of type 'ca'");
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO ca SET sid = 'x', av = 9")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("Cannot insert into type 'ca'");

    database.command("sql", "REFRESH CONTINUOUS AGGREGATE ca");
    assertThat(aggregateRows()).containsExactly(1.0, 2.0, 3.0);
  }

  @Test
  void anAggregateKeepsRefreshingFromItsSource() {
    createAggregate();

    database.command("sql", "INSERT INTO SR SET ts = ?, sid = 's1', v = 4.0", T0 + 3 * 2 * HOUR);
    database.command("sql", "REFRESH CONTINUOUS AGGREGATE ca");

    assertThat(aggregateRows()).containsExactly(1.0, 2.0, 3.0, 4.0);
    assertThat(status("continuousAggregates", "ca")).isEqualTo("VALID");
  }

  // ---- materialized view

  @Test
  void everyWriteOfAViewBackingTypeIsRefused() {
    createView("");
    final long sourceRows = count("Sale");
    final List<Integer> buckets = database.getSchema().getType("mv").getBucketIds(false);
    // indexing a backing type writes no record and stays allowed; UPSERT needs the unique index
    database.command("sql", "CREATE PROPERTY mv.region STRING");
    database.command("sql", "CREATE INDEX ON mv (region) UNIQUE");

    for (final String statement : List.of("TRUNCATE TYPE mv", "TRUNCATE TYPE mv UNSAFE", "INSERT INTO mv SET region = 'NEVER IN THE QUERY'",
        "UPDATE mv SET total = 0", "UPDATE mv SET total = 1 UPSERT WHERE region = 'NOWHERE'",
        "UPDATE mv SET total = 1 UPSERT WHERE region = 'EU'", "DELETE FROM mv", "DELETE FROM mv WHERE region = 'EU'",
        "INSERT INTO BUCKET:" + database.getSchema().getBucketById(buckets.getFirst()).getName() + " SET region = 'X'",
        "TRUNCATE BUCKET " + database.getSchema().getBucketById(buckets.getFirst()).getName(),
        "ALTER TYPE mv BUCKET -" + database.getSchema().getBucketById(buckets.getFirst()).getName()))
      assertThatThrownBy(() -> database.command("sql", statement)).as(statement).isInstanceOf(SchemaException.class)
          .hasMessageContaining("backing type for materialized view 'mv'").hasMessageContaining("REFRESH MATERIALIZED VIEW mv");

    // inside a transaction too, and the transaction is left usable
    database.transaction(() -> {
      assertThatThrownBy(() -> database.command("sql", "INSERT INTO mv SET region = 'NEVER IN THE QUERY'"))
          .isInstanceOf(SchemaException.class);
      database.newDocument("Sale").set("region", "APAC").set("amount", 5).save();
    });

    // and through the API
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("mv").set("region", "X").save())).isInstanceOf(
        SchemaException.class);
    assertThatThrownBy(() -> database.transaction(() -> {
      final Document row = database.iterateType("mv", false).next().asDocument();
      row.delete();
    })).isInstanceOf(SchemaException.class);
    assertThatThrownBy(() -> database.transaction(() -> {
      final MutableDocument row = database.iterateType("mv", false).next().asDocument().modify();
      row.set("total", -1).save();
    })).isInstanceOf(SchemaException.class);

    assertThat(database.getSchema().getType("mv").getBucketIds(false)).isEqualTo(buckets);
    assertThat(count("Sale")).isEqualTo(sourceRows + 1);
    database.command("sql", "REFRESH MATERIALIZED VIEW mv");
    assertThat(viewRows()).containsExactly("APAC=5", "EU=30", "US=5");
    assertThat(status("materializedViews", "mv")).isEqualTo("VALID");
  }

  @Test
  void anIncrementalViewIsStillMaintainedByItsRefresh() {
    createView(" REFRESH INCREMENTAL");
    database.transaction(() -> database.newDocument("Sale").set("region", "US").set("amount", 7).save());
    assertThat(viewRows()).containsExactly("EU=30", "US=12");
  }

  @Test
  void aViewOverAnAggregateIsRefreshedThroughTheAggregateRefresh() {
    createAggregate();
    // the aggregate's refresh writes its own backing type and, on the same thread, the view's refresh writes the view's
    database.command("sql", "CREATE MATERIALIZED VIEW over_ca AS SELECT hour, av FROM ca REFRESH INCREMENTAL");
    database.command("sql", "INSERT INTO SR SET ts = ?, sid = 's1', v = 4.0", T0 + 3 * 2 * HOUR);
    database.command("sql", "REFRESH CONTINUOUS AGGREGATE ca");

    assertThat(count("over_ca")).isEqualTo(4L);
    assertThatThrownBy(() -> database.command("sql", "DELETE FROM over_ca")).isInstanceOf(SchemaException.class);
  }

  @Test
  void aPolymorphicTruncateReachingABackingTypeIsRefusedWhole() {
    createView("");
    database.command("sql", "CREATE DOCUMENT TYPE Base");
    database.command("sql", "CREATE DOCUMENT TYPE Plain EXTENDS Base");
    database.command("sql", "ALTER TYPE mv SUPERTYPE +Base");
    database.transaction(() -> database.command("sql", "INSERT INTO Plain SET k = 1"));

    assertThatThrownBy(() -> database.command("sql", "TRUNCATE TYPE Base POLYMORPHIC")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("backing type for materialized view 'mv'");
    // refused before anything was deleted, the type that is not a backing type included
    assertThat(count("Plain")).isEqualTo(1L);
    assertThat(viewRows()).containsExactly("EU=30", "US=5");

    // and non-polymorphic, the backing type is out of scope
    database.command("sql", "TRUNCATE TYPE Plain");
    assertThat(count("Plain")).isZero();
  }

  @Test
  void theGuardSurvivesAReopenAndGoesWithTheView() {
    createView("");
    reopenDatabase();

    assertThatThrownBy(() -> database.command("sql", "TRUNCATE TYPE mv")).isInstanceOf(SchemaException.class);
    assertThat(viewRows()).containsExactly("EU=30", "US=5");

    database.command("sql", "DROP MATERIALIZED VIEW mv");
    // a type that takes the name afterwards is an ordinary one
    database.command("sql", "CREATE DOCUMENT TYPE mv");
    database.transaction(() -> database.command("sql", "INSERT INTO mv SET region = 'free'"));
    database.command("sql", "TRUNCATE TYPE mv");
    assertThat(count("mv")).isZero();
  }

  /** No mutator of the registries can leave the write guard behind: each refreshes it, and the views refuse a removal. */
  @Test
  void everyMutationOfTheRegistriesKeepsTheGuardInStep() {
    createView("");
    final LocalSchema schema = database.getSchema().getEmbedded();
    final MaterializedViewImpl view = schema.materializedViews.get("mv");

    assertThatThrownBy(() -> schema.materializedViews.keySet().remove("mv")).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> schema.materializedViews.values().clear()).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> schema.materializedViews.entrySet().clear()).isInstanceOf(UnsupportedOperationException.class);

    // the map's own mutators each refresh the guard: off with remove(key, value), back on with putIfAbsent
    assertThat(schema.materializedViews.remove("mv", view)).isTrue();
    schema.checkBackingTypeWrite("mv", "insert into");
    schema.materializedViews.putIfAbsent("mv", view);
    assertThatThrownBy(() -> schema.checkBackingTypeWrite("mv", "insert into")).isInstanceOf(SchemaException.class);
    schema.materializedViews.computeIfPresent("mv", (k, v) -> null);
    schema.checkBackingTypeWrite("mv", "insert into");
    schema.materializedViews.merge("mv", view, (a, b) -> b);
    assertThatThrownBy(() -> schema.checkBackingTypeWrite("mv", "insert into")).isInstanceOf(SchemaException.class);

    // and the SequencedMap mutators and views of LinkedHashMap
    final LinkedHashMap<String, MaterializedViewImpl> sequenced = (LinkedHashMap<String, MaterializedViewImpl>) schema.materializedViews;
    assertThat(sequenced.pollFirstEntry().getValue()).isSameAs(view);
    schema.checkBackingTypeWrite("mv", "insert into");
    sequenced.putLast("mv", view);
    assertThatThrownBy(() -> schema.checkBackingTypeWrite("mv", "insert into")).isInstanceOf(SchemaException.class);
    assertThatThrownBy(() -> sequenced.sequencedKeySet().removeFirst()).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> sequenced.reversed().pollFirstEntry()).isInstanceOf(UnsupportedOperationException.class);

    // issue #11203: so are the entries the views hand out, whose setValue replaced an owner behind the guard
    final MaterializedViewImpl other = new MaterializedViewImpl(database, "other", "SELECT FROM Sale", "other", List.of("Sale"),
        MaterializedViewRefreshMode.MANUAL, true, 0);
    final List<Runnable> entryWrites = List.of(//
        () -> schema.materializedViews.entrySet().iterator().next().setValue(other),
        () -> schema.materializedViews.entrySet().forEach(entry -> entry.setValue(other)),
        () -> schema.materializedViews.entrySet().stream().findFirst().orElseThrow().setValue(other),
        () -> schema.materializedViews.entrySet().removeIf(entry -> true),
        () -> sequenced.sequencedEntrySet().getFirst().setValue(other),
        () -> sequenced.sequencedEntrySet().reversed().iterator().next().setValue(other),
        () -> sequenced.sequencedEntrySet().removeFirst(),
        () -> sequenced.reversed().entrySet().iterator().next().setValue(other),
        () -> sequenced.firstEntry().setValue(other));
    for (final Runnable write : entryWrites)
      assertThatThrownBy(write::run).isInstanceOf(UnsupportedOperationException.class);
    assertThat(schema.materializedViews.entrySet()).extracting(Map.Entry::getKey).containsExactly("mv");
    assertThat(schema.materializedViews.get("mv")).isSameAs(view);
    assertThatThrownBy(() -> schema.checkBackingTypeWrite("mv", "insert into")).isInstanceOf(SchemaException.class);
  }

  /** A document of another type written into a bucket of the backing type by name is refused too (found by CodeRabbit). */
  @Test
  void aDocumentOfAnotherTypeIsNotWrittenIntoABackingBucket() {
    createView("");
    final String bucket = database.getSchema().getBucketById(database.getSchema().getType("mv").getBucketIds(false).getFirst())
        .getName();

    assertThatThrownBy(() -> database.transaction(() -> {
      database.newDocument("Sale").set("region", "X").set("amount", 1).save(bucket);
    })).isInstanceOf(SchemaException.class).hasMessageContaining("backing type for materialized view 'mv'");
    assertThat(viewRows()).containsExactly("EU=30", "US=5");
  }

  /** The refresh scope of one database authorizes no write in another one holding a view of the same name (found by CodeRabbit). */
  @Test
  void aRefreshScopeAuthorizesWritesInItsOwnDatabaseOnly() {
    createView("");
    final MaterializedViewImpl view = database.getSchema().getEmbedded().materializedViews.get("mv");

    final DatabaseFactory factory = new DatabaseFactory(getDatabasePath() + "_other");
    if (factory.exists())
      factory.open().drop();
    final Database other = factory.create();
    try {
      other.command("sql", "CREATE DOCUMENT TYPE Sale");
      other.command("sql", "CREATE MATERIALIZED VIEW mv AS SELECT region, sum(amount) AS total FROM Sale GROUP BY region");

      try (final BackingTypeWriteScope ignored = BackingTypeWriteScope.open(view)) {
        // the scope lets this database's refresh write its own backing type...
        database.transaction(() -> database.newDocument("mv").set("region", "scoped").save());
        // ...and nothing in the other database
        assertThatThrownBy(() -> other.transaction(() -> other.newDocument("mv").set("region", "X").save())).isInstanceOf(
            SchemaException.class);
      }
      assertThat(count("mv")).isEqualTo(3L);
    } finally {
      other.drop();
    }
  }

  // ---- issue #11204: a subtype of a backing type has every row answered by the view, so it is refused

  /** {@code CREATE ... TYPE Sub EXTENDS <backingType>} is refused, whatever kind of type, and nothing is created. */
  @Test
  void aNewTypeCannotExtendABackingType() {
    createView("");
    createAggregate();

    for (final String statement : List.of("CREATE DOCUMENT TYPE Sub EXTENDS mv", "CREATE DOCUMENT TYPE Sub IF NOT EXISTS EXTENDS mv",
        "CREATE DOCUMENT TYPE Sub EXTENDS Sale, mv", "CREATE VERTEX TYPE Sub EXTENDS mv", "CREATE DOCUMENT TYPE Sub EXTENDS ca"))
      assertThatThrownBy(() -> database.command("sql", statement)).as(statement).isInstanceOf(SchemaException.class)
          .hasMessageContaining("Cannot make type 'Sub' a subtype of").hasMessageContaining("only its refresh writes");
    assertThat(database.getSchema().existsType("Sub")).isFalse();

    // through the API too
    assertThatThrownBy(() -> database.getSchema().buildDocumentType().withName("Sub").withSuperType("mv").create()).isInstanceOf(
        SchemaException.class).hasMessageContaining("backing type for materialized view 'mv'");
    assertThat(database.getSchema().existsType("Sub")).isFalse();

    assertThat(viewRows()).containsExactly("EU=30", "US=5");
    assertThat(aggregateRows()).containsExactly(1.0, 2.0, 3.0);
  }

  /** {@code ALTER TYPE Legacy SUPERTYPE +<backingType>} would put a populated type's whole population into the view at once. */
  @Test
  void anExistingTypeCannotBeMadeASubtypeOfABackingType() {
    createView("");
    createAggregate();
    database.command("sql", "CREATE DOCUMENT TYPE Legacy");
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        database.newDocument("Legacy").set("region", "JUNK" + i).set("total", 100 + i).save();
    });

    for (final String target : List.of("mv", "ca"))
      assertThatThrownBy(() -> database.command("sql", "ALTER TYPE Legacy SUPERTYPE +" + target)).as(target)
          .isInstanceOf(SchemaException.class).hasMessageContaining("Cannot make type 'Legacy' a subtype of '" + target + "'");
    assertThatThrownBy(() -> database.getSchema().getType("Legacy").addSuperType("mv")).isInstanceOf(SchemaException.class);
    assertThatThrownBy(() -> database.getSchema().getType("Legacy").setSuperTypes(List.of(database.getSchema().getType("ca"))))
        .isInstanceOf(SchemaException.class);

    assertThat(database.getSchema().getType("Legacy").getSuperTypes()).isEmpty();
    assertThat(viewRows()).containsExactly("EU=30", "US=5");
    assertThat(count("mv")).isEqualTo(2L);
    assertThat(aggregateRows()).containsExactly(1.0, 2.0, 3.0);

    // a backing type can still be given a super type of its own: its rows join the super type's answer, the view's stay its own
    database.command("sql", "CREATE DOCUMENT TYPE Base");
    database.command("sql", "ALTER TYPE mv SUPERTYPE +Base");
    database.command("sql", "CREATE DOCUMENT TYPE Sibling EXTENDS Base");
    assertThat(viewRows()).containsExactly("EU=30", "US=5");
  }

  /**
   * A database written before issue #11204 can already hold a subtype of a backing type: it still opens, the link can be taken out,
   * and nothing new can be hung under that subtype either, since its rows are the view's rows too.
   */
  @Test
  void aSubtypeLinkedBeforeTheCheckStillOpensAndCanBeUnlinked() {
    createView("");
    final LocalDocumentType mv = (LocalDocumentType) database.getSchema().getType("mv");
    final LocalDocumentType legacy = (LocalDocumentType) database.getSchema().createDocumentType("Legacy");
    // the re-establishing linkage of the schema load, which a database written before the check went through
    legacy.addSuperType(mv, false);
    database.transaction(() -> database.newDocument("Legacy").set("region", "JUNK").set("total", 1).save());
    assertThat(count("mv")).isEqualTo(3L);

    reopenDatabase();
    assertThat(database.getSchema().getType("Legacy").getSuperTypes()).extracting(DocumentType::getName).containsExactly("mv");

    assertThatThrownBy(() -> database.command("sql", "CREATE DOCUMENT TYPE Deeper EXTENDS Legacy")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("'Legacy' extends 'mv', the backing type for materialized view 'mv'");

    database.command("sql", "ALTER TYPE Legacy SUPERTYPE -mv");
    assertThat(viewRows()).containsExactly("EU=30", "US=5");
    database.command("sql", "CREATE DOCUMENT TYPE Deeper EXTENDS Legacy");
  }

  // ---- helpers

  private void createAggregate() {
    database.command("sql", "CREATE TIMESERIES TYPE SR TIMESTAMP ts TAGS (sid STRING) FIELDS (v DOUBLE)");
    // three samples two hours apart, one per bucket
    for (int i = 0; i < 3; i++)
      database.command("sql", "INSERT INTO SR SET ts = ?, sid = 's1', v = ?", T0 + i * 2 * HOUR, i + 1.0);
    database.command("sql",
        "CREATE CONTINUOUS AGGREGATE ca AS SELECT sid, ts.timeBucket('1h', ts) AS hour, avg(v) AS av FROM SR GROUP BY sid, hour");
  }

  private void createView(final String refresh) {
    database.command("sql", "CREATE DOCUMENT TYPE Sale");
    database.transaction(() -> {
      database.newDocument("Sale").set("region", "EU").set("amount", 10).save();
      database.newDocument("Sale").set("region", "EU").set("amount", 20).save();
      database.newDocument("Sale").set("region", "US").set("amount", 5).save();
    });
    database.command("sql", "CREATE MATERIALIZED VIEW mv AS SELECT region, sum(amount) AS total FROM Sale GROUP BY region" + refresh);
    assertThat(viewRows()).containsExactly("EU=30", "US=5");
  }

  private List<Double> aggregateRows() {
    final List<Double> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT hour, av FROM ca ORDER BY hour")) {
      while (rs.hasNext())
        rows.add(((Number) rs.next().getProperty("av")).doubleValue());
    }
    return rows;
  }

  private List<String> viewRows() {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT region, total FROM mv ORDER BY region")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        rows.add(row.getProperty("region") + "=" + row.getProperty("total"));
      }
    }
    return rows;
  }

  private long count(final String type) {
    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS n FROM " + type)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }

  private String status(final String registry, final String name) {
    try (final ResultSet rs = database.query("sql", "SELECT status FROM schema:" + registry + " WHERE name = ?", name)) {
      return rs.next().getProperty("status");
    }
  }
}
