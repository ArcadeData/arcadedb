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
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.query.opencypher.Labels;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10537: the one schema publication per type creation is a property of creating a type, not of one SQL statement.
 * <p>
 * #10147 made {@code CREATE ... TYPE} one outermost schema frame, which is one {@code SCHEMA_ENTRY} under Raft, and put the boundary
 * in the statement. Every other route to the same schema change went through {@link TypeBuilder} or {@code Labels.ensureCompositeType}
 * and published as many entries as it made steps: the Java builder of a UNIQUE edge type four (the type, two properties and the
 * index, the constraint being built outside the type's own frame), a two-label Cypher create three (each label, then the composite),
 * three labels four. A follower that counts the entries a transaction has to tolerate reads every one of those as somebody else's schema
 * change, and the {@code engine} and {@code ha-raft} guidance states the one-entry rule as if it held for all of them.
 * <p>
 * The guarantee is the one {@link Issue10147CreateTypeOneSchemaEntryTest} makes for the statement, over every route that creates, grows
 * or links a type: one outermost session (what Raft publishes, see {@link SchemaSessionCounter}) and one schema version, and none
 * at all when the type is there already.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10537TypeCreationOneSchemaEntryTest extends TestHelper {

  /** The routes that create or change a type. Each one names what it does, which is what the failure message shows. */
  enum Route {
    BUILDER_VERTEX("buildVertexType().create()", db -> db.getSchema().buildVertexType().withName("BuilderVertex").create()),
    BUILDER_DOCUMENT("buildDocumentType().create()", db -> db.getSchema().buildDocumentType().withName("BuilderDocument").create()),
    BUILDER_EDGE("buildEdgeType().create()", db -> db.getSchema().buildEdgeType().withName("BuilderEdge").create()),
    BUILDER_UNIQUE_EDGE("buildEdgeType().withUnique(true).create()",
        db -> db.getSchema().buildEdgeType().withName("BuilderUniqueEdge").withUnique(true).create()),
    BUILDER_UNIQUE_EDGE_WITH_SUPER("buildEdgeType().withSuperType().withUnique(true).create()",
        db -> db.getSchema().buildEdgeType().withName("BuilderUniqueSubEdge").withSuperType("BaseRel").withUnique(true).create()),
    BUILDER_LIGHTWEIGHT_UNIQUE_EDGE("buildEdgeType().withLightweight(true).withUnique(true).create()",
        db -> db.getSchema().buildEdgeType().withName("BuilderLightEdge").withLightweight(true).withUnique(true).create()),
    BUILDER_TWO_SUPERS("buildVertexType().withSuperType(A).withSuperType(B).create()",
        db -> db.getSchema().buildVertexType().withName("BuilderComposite").withSuperType("A").withSuperType("B").create()),
    BUILDER_SUPER_WITH_INDEX("buildVertexType().withSuperType(Indexed).create()",
        db -> db.getSchema().buildVertexType().withName("BuilderSubIndexed").withSuperType("Indexed").create()),
    BUILDER_BUCKETS("buildVertexType().withTotalBuckets(4).create()",
        db -> db.getSchema().buildVertexType().withName("BuilderBuckets").withTotalBuckets(4).create()),
    GROWS_AN_EXISTING_TYPE("buildVertexType().withTotalBuckets(4) on a type with one bucket",
        db -> db.getSchema().buildVertexType().withName("OneBucket").withTotalBuckets(4).withIgnoreIfExists(true).create()),
    LINKS_AN_EXISTING_TYPE("buildVertexType().withSuperType(A).withSuperType(B) on an existing type",
        db -> db.getSchema().buildVertexType().withName("Standalone").withSuperType("A").withSuperType("B").withIgnoreIfExists(true)
            .create()),
    GROWS_AND_LINKS_AN_EXISTING_TYPE("buildVertexType().withTotalBuckets(3).withSuperType(A) on an existing type",
        db -> db.getSchema().buildVertexType().withName("OneBucketToo").withTotalBuckets(3).withSuperType("A").withIgnoreIfExists(true)
            .create()),
    GROWS_AN_EXISTING_UNIQUE_EDGE("buildEdgeType().withTotalBuckets(3) on a unique edge type with one bucket",
        db -> db.getSchema().buildEdgeType().withName("UniqueOneBucket").withTotalBuckets(3).withIgnoreIfExists(true).create()),
    GET_OR_CREATE_VERTEX("getOrCreateVertexType()", db -> db.getSchema().getOrCreateVertexType("GetOrCreateVertex")),
    GET_OR_CREATE_EDGE("getOrCreateEdgeType()", db -> db.getSchema().getOrCreateEdgeType("GetOrCreateEdge")),
    ENSURE_COMPOSITE_TWO("Labels.ensureCompositeType(2 labels)", db -> Labels.ensureCompositeType(db.getSchema(), List.of("L1", "L2"))),
    ENSURE_COMPOSITE_THREE("Labels.ensureCompositeType(3 labels)",
        db -> Labels.ensureCompositeType(db.getSchema(), List.of("M1", "M2", "M3"))),
    ENSURE_COMPOSITE_OVER_EXISTING("Labels.ensureCompositeType(1 existing + 1 new label)",
        db -> Labels.ensureCompositeType(db.getSchema(), List.of("A", "N2"))),
    CYPHER_CREATE_TWO_LABELS("CREATE (n:Pp:Qq)", db -> db.command("opencypher", "CREATE (n:Pp:Qq)").close()),
    CYPHER_CREATE_THREE_LABELS("CREATE (n:Rr:Ss:Tt)", db -> db.command("opencypher", "CREATE (n:Rr:Ss:Tt)").close()),
    CYPHER_MERGE_TWO_LABELS("MERGE (n:Uu:Vv)", db -> db.command("opencypher", "MERGE (n:Uu:Vv)").close()),
    CYPHER_SET_LABEL("MATCH (n:A) SET n:Ww", db -> db.command("opencypher", "MATCH (n:A) SET n:Ww").close()),
    ALTER_TYPE_UNIQUE("ALTER TYPE ... WITH unique = true on a populated edge type",
        db -> db.command("sql", "ALTER TYPE Populated WITH unique = true").close());

    private final String                 description;
    private final Consumer<LocalDatabase> action;

    Route(final String description, final Consumer<LocalDatabase> action) {
      this.description = description;
      this.action = action;
    }
  }

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE A");
    database.command("sql", "CREATE VERTEX TYPE B");
    database.command("sql", "CREATE EDGE TYPE BaseRel");
    // A super type that declares an index: linking a sub type propagates it over the sub type's buckets
    database.command("sql", "CREATE VERTEX TYPE Indexed");
    database.command("sql", "CREATE PROPERTY Indexed.key STRING");
    database.command("sql", "CREATE INDEX ON Indexed (key) UNIQUE");
    // Types the routes grow or link, and one the alter makes UNIQUE with records in it
    database.command("sql", "CREATE VERTEX TYPE OneBucket BUCKETS 1");
    database.command("sql", "CREATE VERTEX TYPE OneBucketToo BUCKETS 1");
    database.command("sql", "CREATE VERTEX TYPE Standalone");
    database.command("sql", "CREATE EDGE TYPE UniqueOneBucket UNIQUE BUCKETS 1");
    database.command("sql", "CREATE EDGE TYPE Populated");
    database.transaction(() -> {
      final var from = database.newVertex("A").save();
      final var to = database.newVertex("B").save();
      from.newEdge("Populated", to);
    });
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(Route.class)
  void everyRouteIsOneSchemaEntry(final Route route) {
    final long versionBefore = schema().getVersion();

    final int sessions = SchemaSessionCounter.count((LocalDatabase) database, () -> route.action.accept((LocalDatabase) database));

    assertThat(sessions).as("outermost schema sessions, i.e. SCHEMA_ENTRY under Raft, of: %s", route.description).isEqualTo(1);
    assertThat(schema().getVersion() - versionBefore).as("schema versions published by: %s", route.description).isEqualTo(1);
  }

  /** One entry is useless if it carries less than the four of them did: the type, its two properties and the index are all there. */
  @Test
  void theSingleEntryOfAUniqueEdgeCarriesTheTypeItsPropertiesAndItsIndex() {
    database.getSchema().buildEdgeType().withName("Linked").withUnique(true).create();

    assertUniqueEdge();

    reopenDatabase();

    assertUniqueEdge();
  }

  private void assertUniqueEdge() {
    final DocumentType linked = database.getSchema().getType("Linked");
    assertThat(linked.existsProperty("@out")).isTrue();
    assertThat(linked.existsProperty("@in")).isTrue();
    assertThat(database.getSchema().existsIndex(LocalEdgeType.uniqueIndexName("Linked"))).isTrue();
  }

  /** The composite a Cypher create makes has both labels as super types, the one entry carries them. */
  @Test
  void theSingleEntryOfACompositeLabelCarriesTheLabelsAndTheirLinks() {
    database.command("opencypher", "CREATE (n:Pp:Qq)").close();

    assertComposite();

    reopenDatabase();

    assertComposite();
  }

  private void assertComposite() {
    final DocumentType composite = database.getSchema().getType("Pp~Qq");
    assertThat(composite.getSuperTypes()).extracting(DocumentType::getName).containsExactlyInAnyOrder("Pp", "Qq");
    assertThat(database.getSchema().getType("Pp").getSubTypes()).extracting(DocumentType::getName).contains("Pp~Qq");
    assertThat(database.getSchema().getType("Qq").getSubTypes()).extracting(DocumentType::getName).contains("Pp~Qq");
  }

  /**
   * A route that finds everything in place asks nothing of the schema: no frame, so no entry, no version, and no write lock taken on
   * the hot path of every Cypher create on labels that exist.
   */
  @Test
  void aRouteThatFindsEveryTypeInPlaceOpensNoSession() {
    database.command("opencypher", "CREATE (n:Pp:Qq)").close();
    final long versionBefore = schema().getVersion();

    final int sessions = SchemaSessionCounter.count((LocalDatabase) database, () -> {
      Labels.ensureCompositeType(database.getSchema(), List.of("Pp", "Qq"));
      database.command("opencypher", "CREATE (n:Pp:Qq)").close();
      database.getSchema().buildVertexType().withName("Pp~Qq").withIgnoreIfExists(true).create();
      database.getSchema().getOrCreateEdgeType("BaseRel");
    });

    assertThat(sessions).isZero();
    assertThat(schema().getVersion()).isEqualTo(versionBefore);
  }

  /**
   * A create whose constraint is refused leaves nothing of it in the schema: the type is taken back with the properties it declared,
   * as a refused super type link takes it back. The super type declares {@code @in} as a MAP, which the index of the constraint cannot
   * be built over.
   */
  @Test
  void aRefusedUniqueEdgeCreateLeavesNothingBehind() {
    database.getSchema().getType("BaseRel").createProperty("@in", Type.MAP);

    assertThatThrownBy(() -> database.getSchema().buildEdgeType().withName("Refused").withSuperType("BaseRel").withUnique(true).create())
        .isInstanceOf(RuntimeException.class);

    assertThat(database.getSchema().existsType("Refused")).isFalse();
    assertThat(database.getSchema().existsIndex(LocalEdgeType.uniqueIndexName("Refused"))).isFalse();
    assertThat(database.getSchema().getBuckets()).extracting(bucket -> bucket.getName()).noneMatch(name -> name.startsWith("Refused_"));
  }

  /** Routes that wait for the async workers to drain (an index to build, a super type whose indexes are propagated). */
  enum AsyncRoute {
    BUILDER_UNIQUE_EDGE(db -> db.getSchema().buildEdgeType().withName("AsyncUnique").withUnique(true).create()),
    BUILDER_SUPER_WITH_INDEX(db -> db.getSchema().buildVertexType().withName("AsyncSub").withSuperType("Indexed").create()),
    BUILDER_COMPLETES_A_TYPE(
        db -> db.getSchema().buildVertexType().withName("Standalone").withSuperType("Indexed").withIgnoreIfExists(true).create()),
    ENSURE_COMPOSITE_WITH_INDEX(db -> Labels.ensureCompositeType(db.getSchema(), List.of("Indexed", "AsyncLabel"))),
    CYPHER_CREATE_WITH_INDEX(db -> db.command("opencypher", "CREATE (n:Indexed:AsyncCypher {key: 'k'})").close()),
    SQL_UNIQUE_EDGE(db -> db.command("sql", "CREATE EDGE TYPE SqlAsyncUnique UNIQUE").close()),
    SQL_SUPER_WITH_INDEX(db -> db.command("sql", "CREATE VERTEX TYPE SqlAsyncSub EXTENDS Indexed").close()),
    ALTER_TYPE_UNIQUE(db -> db.command("sql", "ALTER TYPE Populated WITH unique = true").close());

    private final Consumer<LocalDatabase> action;

    AsyncRoute(final Consumer<LocalDatabase> action) {
      this.action = action;
    }
  }

  /**
   * What waits for the async workers to drain has to be asked before the write lock of the frame is taken, and must not be asked again
   * inside it: the workers need that lock to commit their batch, and a barrier that waits behind workers a quiescence has parked never
   * ends. The builder used to build the index of a UNIQUE edge type outside the frame, so it did not meet it; inside the one frame it does,
   * as the statement does, and the two are measured here under an async executor that holds a batch. A hang detector, not a latency bound.
   */
  @ParameterizedTest(name = "{0}")
  @EnumSource(AsyncRoute.class)
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void noRouteStallsWhileTheAsyncExecutorHoldsABatch(final AsyncRoute route) {
    database.getSchema().createVertexType("Batch");
    for (int i = 0; i < 100; i++)
      database.async().createRecord(database.newVertex("Batch").set("i", i), null);

    route.action.accept((LocalDatabase) database);

    database.async().waitCompletion();
    assertThat(database.countType("Batch", false)).isEqualTo(100L);
  }

  private LocalSchema schema() {
    return ((LocalDatabase) database).getSchema().getEmbedded();
  }
}
