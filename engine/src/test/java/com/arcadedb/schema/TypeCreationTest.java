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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.TypeAlreadyExistsException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.CreateVertexTypeStatement;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.Identifier;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Regression tests, one nested class per fixed issue.
 */
class TypeCreationTest {

  /**
   * Regression for #10067: a replica that has to ask the leader to create a type renders the builder's description as the {@code CREATE ... TYPE}
   * statement the leader runs. The rendering has to describe the same type the builder would create, so every statement here is also
   * run, and the type it makes is compared with the one the builder makes.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class TypeBuilderToSQL extends TestHelper {

    @Test
    void aPlainVertexTypeRendersOnlyWhatWasChosen() {
      final TypeBuilder<VertexType> builder = database.getSchema().buildVertexType().withName("Person").withIgnoreIfExists(true);
      assertThat(builder.toSQL()).isEqualTo("CREATE VERTEX TYPE `Person` IF NOT EXISTS");
    }

    /** The statement a replica forwards for an implicit type is idempotent whatever the builder said, so a racing creation is no error. */
    @Test
    void ifNotExistsCanBeForcedEitherWay() {
      final TypeBuilder<VertexType> strict = database.getSchema().buildVertexType().withName("Racing");
      assertThat(strict.toSQL()).isEqualTo("CREATE VERTEX TYPE `Racing`");
      assertThat(strict.toSQL(true)).isEqualTo("CREATE VERTEX TYPE `Racing` IF NOT EXISTS");

      final TypeBuilder<VertexType> lenient = database.getSchema().buildVertexType().withName("Racing").withIgnoreIfExists(true);
      assertThat(lenient.toSQL(false)).isEqualTo("CREATE VERTEX TYPE `Racing`");

      database.command("sql", strict.toSQL(true));
      database.command("sql", strict.toSQL(true));
      assertThat(database.getSchema().existsType("Racing")).isTrue();
    }

    /**
     * {@link TypeBuilder#toSQL()} is what a replica sends the leader in place of creating the type itself, so a field of the builder it
     * does not render would make the leader create a different type than the caller asked for, silently. Adding a field to the builder
     * fails this test until the field is rendered by {@code toSQL()} (and covered above) and listed here.
     */
    @Test
    void everyFieldOfTheBuilderIsAccountedFor() {
      final Set<String> rendered = Set.of("type", "ignoreIfExists", "typeName", "superTypes", "bucketInstances", "buckets",
          "bucketsChosen", "pageSize", "pageSizeChosen", "edgeBidirectional", "edgeLightweight", "edgeUnique");
      final Set<String> notPartOfTheDescription = Set.of("database");

      final Set<String> declared = new TreeSet<>();
      for (final Field field : TypeBuilder.class.getDeclaredFields())
        if (!Modifier.isStatic(field.getModifiers()) && !field.isSynthetic())
          declared.add(field.getName());

      final Set<String> accountedFor = new TreeSet<>(rendered);
      accountedFor.addAll(notPartOfTheDescription);
      assertThat(declared).as("a field of TypeBuilder that toSQL() does not render makes a replica create a different type").isEqualTo(accountedFor);
    }

    /**
     * {@link TypeBuilder#create()} looks the type up in the schema's type map without the read lock, as a pre-check, which is sound only
     * because the map is a concurrent one whichever generation of the schema (published or staged) it answers for.
     */
    @Test
    void theTypeMapTheUnlockedPreCheckReadsIsAConcurrentMap() {
      assertThat(((LocalSchema) database.getSchema()).typeMap()).isInstanceOf(ConcurrentMap.class);
    }

    /** The kind of the type is the kind the builder was made for: vertex, edge and document each create what they were asked to. */
    @Test
    void theKindOfTheTypeRoundTrips() {
      database.command("sql", database.getSchema().buildVertexType().withName("AVertex").toSQL());
      database.command("sql", database.getSchema().buildEdgeType().withName("AnEdge").toSQL());
      database.command("sql", database.getSchema().buildDocumentType().withName("ADocument").toSQL());
      database.command("sql", database.getSchema().buildVertexType().withName("ASubVertex").withSuperType("AVertex").toSQL());

      assertThat(database.getSchema().getType("AVertex")).isInstanceOf(VertexType.class);
      assertThat(database.getSchema().getType("AnEdge")).isInstanceOf(EdgeType.class);
      assertThat(database.getSchema().getType("ADocument")).isNotInstanceOf(VertexType.class).isNotInstanceOf(EdgeType.class);
      assertThat(database.getSchema().getType("ASubVertex")).as("a type with a super type keeps its kind").isInstanceOf(VertexType.class);
      assertThat(database.getSchema().getType("ASubVertex").instanceOf("AVertex")).isTrue();
    }

    @Test
    void aTypeCreatedWithoutIgnoreIfExistsHasNoIfNotExists() {
      assertThat(database.getSchema().buildVertexType().withName("Person").toSQL()).isEqualTo("CREATE VERTEX TYPE `Person`");
      assertThat(database.getSchema().buildDocumentType().withName("Doc").toSQL()).isEqualTo("CREATE DOCUMENT TYPE `Doc`");
    }

    @Test
    void aCompositeLabelTypeNamesItsSuperTypes() {
      database.getSchema().createVertexType("A");
      database.getSchema().createVertexType("B");
      final TypeBuilder<VertexType> builder = database.getSchema().buildVertexType().withName("A~B").withSuperType("A").withSuperType("B");
      assertThat(builder.toSQL()).isEqualTo("CREATE VERTEX TYPE `A~B` EXTENDS `A`, `B`");

      database.command("sql", builder.toSQL());
      assertThat(database.getSchema().getType("A~B").instanceOf("A")).isTrue();
      assertThat(database.getSchema().getType("A~B").instanceOf("B")).isTrue();
    }

    @Test
    void anEdgeTypeRendersItsFlags() {
      final TypeBuilder<EdgeType> builder = database.getSchema().buildEdgeType().withName("Knows").withIgnoreIfExists(true)
          .withBidirectional(false).withLightweight(true).withUnique(true);
      assertThat(builder.toSQL()).isEqualTo("CREATE EDGE TYPE `Knows` IF NOT EXISTS UNIDIRECTIONAL LIGHTWEIGHT UNIQUE");

      database.command("sql", builder.toSQL());
      final EdgeType edge = (EdgeType) database.getSchema().getType("Knows");
      assertThat(edge.isBidirectional()).isFalse();
      assertThat(edge.isLightweight()).isTrue();
      assertThat(edge.isUnique()).isTrue();
    }

    @Test
    void theBucketCountAndThePageSizeAreRenderedWhenTheCallerChoseThem() {
      final int defaultBuckets = database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS);
      final int defaultPageSize = database.getConfiguration().getValueAsInteger(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE);

      assertThat(database.getSchema().buildVertexType().withName("Defaults").toSQL()).as("not chosen: left to the node that runs it")
          .isEqualTo("CREATE VERTEX TYPE `Defaults`");

      // Chosen, even when it equals the default of THIS node: the node that runs the statement may have another default
      assertThat(database.getSchema().buildVertexType().withName("Explicit").withTotalBuckets(defaultBuckets)
          .withPageSize(defaultPageSize).toSQL()).isEqualTo(
          "CREATE VERTEX TYPE `Explicit` BUCKETS " + defaultBuckets + " PAGESIZE " + defaultPageSize);
      assertThat(database.getSchema().buildVertexType().withName("OnlyBuckets").withTotalBuckets(defaultBuckets).toSQL())
          .isEqualTo("CREATE VERTEX TYPE `OnlyBuckets` BUCKETS " + defaultBuckets);
      assertThat(database.getSchema().buildVertexType().withName("OnlyPage").withPageSize(defaultPageSize).toSQL())
          .isEqualTo("CREATE VERTEX TYPE `OnlyPage` PAGESIZE " + defaultPageSize);

      final TypeBuilder<VertexType> custom = database.getSchema().buildVertexType().withName("Custom")
          .withTotalBuckets(defaultBuckets + 1).withPageSize(defaultPageSize * 2);
      assertThat(custom.toSQL()).isEqualTo("CREATE VERTEX TYPE `Custom` BUCKETS " + (defaultBuckets + 1) + " PAGESIZE " + defaultPageSize * 2);

      database.command("sql", custom.toSQL());
      assertThat(database.getSchema().getType("Custom").getBuckets(false)).hasSize(defaultBuckets + 1);
    }

    /** The statement is executed on another node, so what it creates has to be what the builder described - the page size included. */
    @Test
    void aCustomPageSizeReachesTheBucketsOfTheCreatedType() {
      final int pageSize = database.getConfiguration().getValueAsInteger(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE) * 2;
      final TypeBuilder<VertexType> builder = database.getSchema().buildVertexType().withName("Paged").withPageSize(pageSize);

      database.command("sql", builder.toSQL());

      final VertexType created = (VertexType) database.getSchema().getType("Paged");
      assertThat(created.getBuckets(false)).isNotEmpty().allSatisfy(bucket -> assertThat(((PaginatedComponent) bucket).getPageSize())
          .isEqualTo(pageSize));
    }

    /** A name with spaces, a keyword or quotes is quoted, and the statement creates the type of exactly that name. */
    @Test
    void namesWithSpacesKeywordsAndQuotesRoundTrip() {
      for (final String name : new String[] { "my type", "select", "FROM", "order by", "A B~C", "it's", "say \"hi\"", "semi;colon",
          "dash-and.dot", "ünïcödé" }) {
        final TypeBuilder<VertexType> builder = database.getSchema().buildVertexType().withName(name).withIgnoreIfExists(true);

        database.command("sql", builder.toSQL());

        assertThat(database.getSchema().existsType(name)).as("type '%s' created through %s", name, builder.toSQL()).isTrue();
        assertThat(database.getSchema().getType(name).getName()).isEqualTo(name);
      }
    }

    @Test
    void aNameThatNeedsQuotingIsEscaped() {
      final TypeBuilder<VertexType> builder = database.getSchema().buildVertexType().withName("we`ird\\name");
      assertThat(builder.toSQL()).isEqualTo("CREATE VERTEX TYPE `we\\`ird\\\\name`");

      database.command("sql", builder.toSQL());
      assertThat(database.getSchema().existsType("we`ird\\name")).isTrue();
    }

    @Test
    void aTypeBuiltOnBucketInstancesHasNoSqlForm() {
      database.getSchema().createBucket("Orphan_0");
      final TypeBuilder<VertexType> builder = database.getSchema().buildVertexType().withName("OnBuckets")
          .withBuckets(java.util.List.of(database.getSchema().getBucketByName("Orphan_0")));
      assertThat(builder.toSQL()).isNull();
    }
  }

  /**
   * Regression for #10146: {@link TypeBuilder#toSQL()} is what a replica sends the leader in place of creating the type itself (#10067), so the
   * statement has to create the type the builder would. A census of the builder's field NAMES cannot see a setter that writes a field
   * without the flag that renders it ({@code withBuckets(emptyList)} wrote {@code buckets = 0} without {@code bucketsChosen}, so the
   * statement carried no {@code BUCKETS} clause and the other node gave the abstract type its default buckets). The census here is by
   * VALUE: every setter is applied with a non-default argument, the rendered statement is run, and the type it makes is compared with
   * the type the same builder makes.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class TypeBuilderSetterRoundTrip extends TestHelper {

    @Test
    void anEmptyBucketInstanceListRendersZeroBuckets() {
      final TypeBuilder<VertexType> builder = database.getSchema().buildVertexType().withName("EmptyList").withBuckets(List.of());
      assertThat(builder.toSQL()).isEqualTo("CREATE VERTEX TYPE `EmptyList` BUCKETS 0");

      database.command("sql", builder.toSQL());
      assertThat(database.getSchema().getType("EmptyList").getBuckets(false)).isEmpty();

      assertThat(database.getSchema().buildVertexType().withName("EmptyListByBuilder").withBuckets(List.of()).create().getBuckets(false))
          .isEmpty();
    }

    /** The public overloads that take bucket instances reach the same setter, for every kind of type. */
    @Test
    void anEmptyBucketInstanceListRendersZeroBucketsForEveryKind() {
      assertThat(database.getSchema().buildEdgeType().withName("E").withBuckets(List.of()).toSQL()).isEqualTo("CREATE EDGE TYPE `E` BUCKETS 0");
      assertThat(database.getSchema().buildDocumentType().withName("D").withBuckets(List.of()).toSQL())
          .isEqualTo("CREATE DOCUMENT TYPE `D` BUCKETS 0");

      database.getSchema().createVertexType("ViaOverload", List.<Bucket>of());
      database.getSchema().createEdgeType("EdgeViaOverload", List.<Bucket>of());
      database.getSchema().createDocumentType("DocViaOverload", List.<Bucket>of());
      assertThat(database.getSchema().getType("ViaOverload").getBuckets(false)).isEmpty();
      assertThat(database.getSchema().getType("EdgeViaOverload").getBuckets(false)).isEmpty();
      assertThat(database.getSchema().getType("DocViaOverload").getBuckets(false)).isEmpty();
    }

    /** The last setter wins, as it does for create(): a count chosen after the instances replaces their zero, and the reverse. */
    @Test
    void theLastBucketSetterWins() {
      assertThat(database.getSchema().buildVertexType().withName("A").withBuckets(List.of()).withTotalBuckets(3).toSQL())
          .isEqualTo("CREATE VERTEX TYPE `A` BUCKETS 3");
      assertThat(database.getSchema().buildVertexType().withName("B").withTotalBuckets(3).withBuckets(List.of()).toSQL())
          .isEqualTo("CREATE VERTEX TYPE `B` BUCKETS 0");
    }

    /**
     * A count chosen after a NON-empty list does not replace the instances: withTotalBuckets does not clear them, create() still uses
     * them, so the statement must stay absent rather than render a count the builder would not honor. Pins the behavior before #10146.
     */
    @Test
    void aCountAfterBucketInstancesLeavesTheTypeWithoutSqlForm() {
      final Bucket orphan = database.getSchema().createBucket("Orphan_0");
      assertThat(database.getSchema().buildVertexType().withName("C").withBuckets(List.of(orphan)).withTotalBuckets(3).toSQL()).isNull();
    }

    /**
     * Every setter of the builder, applied with a value that differs from the default, makes the same type through {@code toSQL()} as
     * through {@code create()}. A setter added to the builder fails this test until it is given a case here.
     */
    @Test
    void everySetterRoundTripsThroughTheStatement() {
      final int defaultBuckets = database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS);
      final int defaultPageSize = database.getConfiguration().getValueAsInteger(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE);

      database.getSchema().createVertexType("BaseV");
      database.getSchema().createEdgeType("BaseE");
      database.getSchema().createDocumentType("BaseD");

      // withName is applied to every case: a builder without a name has nothing to create
      final Map<String, List<UnaryOperator<TypeBuilder<?>>>> anyKind = new LinkedHashMap<>();
      anyKind.put("withIgnoreIfExists", List.of(b -> b.withIgnoreIfExists(true)));
      // withTotalBuckets(0) is a regression pin: that setter already rendered BUCKETS 0 before #10146
      anyKind.put("withTotalBuckets", List.of(b -> b.withTotalBuckets(defaultBuckets + 1), b -> b.withTotalBuckets(0)));
      // only the EMPTY list: a non-empty one has no SQL form at all (toSQL() returns null, see
      // aCountAfterBucketInstancesLeavesTheTypeWithoutSqlForm), so it cannot round-trip and would fail the isNotNull check below
      anyKind.put("withBuckets", List.of(b -> b.withBuckets(List.of())));
      anyKind.put("withPageSize", List.of(b -> b.withPageSize(defaultPageSize * 2)));
      anyKind.put("withSuperType", List.of(b -> b.withSuperType(baseTypeOf(b))));

      final Map<String, List<UnaryOperator<TypeBuilder<?>>>> edgeOnly = new LinkedHashMap<>();
      edgeOnly.put("withBidirectional", List.of(b -> b.withBidirectional(false)));
      edgeOnly.put("withLightweight", List.of(b -> b.withLightweight(true)));
      edgeOnly.put("withUnique", List.of(b -> b.withUnique(true)));

      final Set<String> setters = new TreeSet<>();
      for (final Method m : TypeBuilder.class.getDeclaredMethods())
        if (Modifier.isPublic(m.getModifiers()) && !Modifier.isStatic(m.getModifiers()) && m.getName().startsWith("with"))
          setters.add(m.getName());
      final Set<String> covered = new TreeSet<>(anyKind.keySet());
      covered.addAll(edgeOnly.keySet());
      covered.add("withName");
      assertThat(setters).as("a setter of TypeBuilder without a round-trip case here").isEqualTo(covered);

      final List<Supplier<TypeBuilder<?>>> kinds = List.of(() -> database.getSchema().buildVertexType(),
          () -> database.getSchema().buildEdgeType(), () -> database.getSchema().buildDocumentType());

      final List<String> mismatches = new ArrayList<>();
      int n = 0;
      for (final Supplier<TypeBuilder<?>> kind : kinds) {
        final Map<String, List<UnaryOperator<TypeBuilder<?>>>> cases = new LinkedHashMap<>(anyKind);
        if (kind.get().type.equals(EdgeType.class))
          cases.putAll(edgeOnly);

        for (final Map.Entry<String, List<UnaryOperator<TypeBuilder<?>>>> entry : cases.entrySet())
          for (final UnaryOperator<TypeBuilder<?>> setter : entry.getValue()) {
            ++n;
            final String viaSql = "ViaSql" + n;
            final String viaBuilder = "ViaBuilder" + n;

            final String sql = setter.apply(kind.get().withName(viaSql)).toSQL();
            assertThat(sql).as("%s has a SQL form", entry.getKey()).isNotNull();
            database.command("sql", sql);
            setter.apply(kind.get().withName(viaBuilder)).create();

            final String expected = configurationOf(database.getSchema().getType(viaBuilder));
            final String actual = configurationOf(database.getSchema().getType(viaSql));
            if (!expected.equals(actual))
              mismatches.add(entry.getKey() + " via '" + sql + "': builder made " + expected + ", statement made " + actual);
          }
      }
      assertThat(mismatches).as("setters whose rendered statement makes a different type than the builder").isEmpty();
    }

    /** The fixture supertype of the builder's own kind: a type can only extend a type of the same kind. */
    private static String baseTypeOf(final TypeBuilder<?> builder) {
      if (builder.type.equals(VertexType.class))
        return "BaseV";
      if (builder.type.equals(EdgeType.class))
        return "BaseE";
      return "BaseD";
    }

    private static String configurationOf(final DocumentType type) {
      final StringBuilder s = new StringBuilder(type.getClass().getSimpleName());
      final List<Bucket> buckets = type.getBuckets(false);
      s.append(" buckets=").append(buckets.size());
      s.append(" pageSizes=").append(buckets.stream().map(b -> ((PaginatedComponent) b).getPageSize()).distinct().toList());
      s.append(" superTypes=").append(type.getSuperTypes().stream().map(DocumentType::getName).toList());
      if (type instanceof EdgeType edge)
        s.append(" bidirectional=").append(edge.isBidirectional()).append(" lightweight=").append(edge.isLightweight())
            .append(" unique=").append(edge.isUnique());
      return s.toString();
    }
  }

  /**
   * Regression for #10147: a {@code CREATE ... TYPE} statement is ONE schema publication, whatever it declares.
   * <p>
   * Under Raft every outermost {@code recordFileChanges} frame publishes its own {@code SCHEMA_ENTRY}, and the statement ran one frame
   * for the create and one more for each super type it linked (and the index of a UNIQUE edge type, and the CUSTOM values). A follower
   * deciding whether a transaction that had already written can be carried past the type it asked the leader for
   * ({@code RaftReplicatedDatabase.transactionStaysValidPastTheType}) counts the entries applied since, expects exactly one, and
   * refused every composite openCypher label ({@code CREATE (n:A:B)} renders {@code CREATE VERTEX TYPE `A~B` EXTENDS A, B}) with
   * a message blaming a schema change nobody made. It also saw the type appear without its super types and then gain them.
   * <p>
   * The guarantee is the invariant the counting rule depends on, over every shape the statement can take: it opens one outermost
   * session (the number of Raft entries the same code publishes, see {@link SchemaSessionCounter}) and advances
   * {@code LocalSchema.getVersion()} - which moves once per schema publication - by exactly one.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CreateTypeOneSchemaEntry extends TestHelper {

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE VERTEX TYPE A");
      database.command("sql", "CREATE VERTEX TYPE B");
      database.command("sql", "CREATE EDGE TYPE BaseRel");
      database.command("sql", "CREATE DOCUMENT TYPE BaseDoc");
      // A super type that declares an index: linking a sub type propagates it over the sub type's buckets, in transactions of their own
      database.command("sql", "CREATE VERTEX TYPE Indexed");
      database.command("sql", "CREATE PROPERTY Indexed.key STRING");
      database.command("sql", "CREATE INDEX ON Indexed (key) UNIQUE");
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {
        "CREATE VERTEX TYPE Plain",
        "CREATE DOCUMENT TYPE PlainDoc",
        "CREATE EDGE TYPE Rel",
        "CREATE VERTEX TYPE SubOne EXTENDS A",
        "CREATE VERTEX TYPE `A~B` IF NOT EXISTS EXTENDS A, B",
        "CREATE VERTEX TYPE ThreeSupers EXTENDS A, B, Indexed",
        "CREATE VERTEX TYPE SubIndexed EXTENDS Indexed",
        "CREATE EDGE TYPE SubRel EXTENDS BaseRel",
        "CREATE DOCUMENT TYPE SubDoc EXTENDS BaseDoc",
        "CREATE EDGE TYPE RelUnique UNIQUE",
        "CREATE EDGE TYPE SubRelUnique EXTENDS BaseRel UNIQUE",
        "CREATE EDGE TYPE RelLight LIGHTWEIGHT",
        "CREATE VERTEX TYPE WithBuckets BUCKETS 4",
        "CREATE VERTEX TYPE WithBucketsAndSupers EXTENDS A, B BUCKETS 2",
        "CREATE VERTEX TYPE WithPageSize PAGESIZE 131072",
        "CREATE VERTEX TYPE WithCustom CUSTOM a = 1, b = 'x'",
        "CREATE VERTEX TYPE WithSupersAndCustom EXTENDS A, B CUSTOM a = 1" })
    void oneStatementIsOneSchemaEntryWhateverItDeclares(final String statement) {
      final long versionBefore = schema().getVersion();

      final int sessions = SchemaSessionCounter.count((LocalDatabase) database, () -> database.command("sql", statement).close());

      assertThat(sessions).as("outermost schema sessions, i.e. SCHEMA_ENTRY under Raft, of: %s", statement).isEqualTo(1);
      assertThat(schema().getVersion() - versionBefore).as("schema versions published by: %s", statement).isEqualTo(1);
    }

    /** The same statement under a name that is a variable, which is resolved when it runs. */
    @Test
    void theStatementWithAVariableNameIsOneEntryToo() {
      final long versionBefore = schema().getVersion();

      final int sessions = SchemaSessionCounter.count((LocalDatabase) database,
          () -> database.command("sqlscript", "LET name = 'Dynamic'; CREATE VERTEX TYPE $name EXTENDS A, B;").close());

      assertThat(sessions).isEqualTo(1);
      assertThat(schema().getVersion() - versionBefore).isEqualTo(1);
      assertThat(database.getSchema().getType("Dynamic").getSuperTypes()).extracting(DocumentType::getName)
          .containsExactlyInAnyOrder("A", "B");
    }

    /** One entry is useless if it carries less than the three of them did: the type, both links and the custom values are all there. */
    @Test
    void theSingleEntryCarriesTheTypeItsSuperTypesAndItsCustomValues() {
      database.command("sql", "CREATE VERTEX TYPE `A~B` EXTENDS A, B CUSTOM origin = 'cypher'").close();

      assertComposite();

      reopenDatabase();

      assertComposite();
      assertThat(schema().getEmbedded().toJSON().toString()).contains("A~B");
    }

    private void assertComposite() {
      final DocumentType composite = database.getSchema().getType("A~B");
      assertThat(composite.getSuperTypes()).extracting(DocumentType::getName).containsExactlyInAnyOrder("A", "B");
      assertThat(composite.getCustomValue("origin")).isEqualTo("cypher");
      assertThat(database.getSchema().getType("A").getSubTypes()).extracting(DocumentType::getName).contains("A~B");
      assertThat(database.getSchema().getType("B").getSubTypes()).extracting(DocumentType::getName).contains("A~B");
    }

    /** The inherited index is propagated by the same statement: a record of the composite type is in the super type's unique index. */
    @Test
    void theIndexOfASuperTypeCoversTheNewSubTypeAfterTheSingleEntry() {
      database.command("sql", "CREATE VERTEX TYPE SubIndexed EXTENDS Indexed").close();

      database.transaction(() -> database.newVertex("SubIndexed").set("key", "k1").save());

      assertThat(database.query("sql", "SELECT FROM Indexed WHERE key = 'k1'").stream().count()).isEqualTo(1L);
      assertThatThrownBy(() -> database.transaction(() -> database.newVertex("Indexed").set("key", "k1").save()))
          .isInstanceOf(DuplicatedKeyException.class);
    }

    /** {@code IF NOT EXISTS} on a type that is there asks nothing of the schema: no entry, and nothing bumps the version. */
    @Test
    void ifNotExistsOnAnExistingTypePublishesNothing() {
      final long versionBefore = schema().getVersion();

      final int sessions = SchemaSessionCounter.count((LocalDatabase) database,
          () -> database.command("sql", "CREATE VERTEX TYPE A IF NOT EXISTS EXTENDS B").close());

      assertThat(sessions).isZero();
      assertThat(schema().getVersion()).isEqualTo(versionBefore);
      assertThat(database.getSchema().getType("A").getSuperTypes()).isEmpty();
    }

    /** A statement refused before it creates anything opens no session at all. */
    @Test
    void aStatementRefusedBeforeItCreatesAnythingPublishesNothing() {
      database.command("sql", "CREATE TIMESERIES TYPE Metrics TIMESTAMP ts TAGS (sensor STRING) FIELDS (temperature DOUBLE)").close();
      final long versionBefore = schema().getVersion();

      final SchemaSessionCounter.Count seen = new SchemaSessionCounter.Count();
      assertThatThrownBy(() -> SchemaSessionCounter.count((LocalDatabase) database, seen,
          () -> database.command("sql", "CREATE VERTEX TYPE Child EXTENDS Metrics").close())).isInstanceOf(SchemaException.class);

      assertThat(seen.sessions()).isZero();
      assertThat(schema().getVersion()).isEqualTo(versionBefore);
      assertThat(database.getSchema().existsType("Child")).isFalse();
    }

    /**
     * The take-back of a refused link is a drop that can fail too: the statement still ends with the refusal that made it necessary, the
     * failure of the drop is kept on it as suppressed, and the schema is still usable. The type the statement creates is one the schema
     * never held, so that linking it is refused and dropping it fails.
     */
    @Test
    void aFailingTakeBackSurfacesTheRefusalAndKeepsTheDropFailureAsSuppressed() {
      final SchemaException refusal = new SchemaException("the link is refused");
      final CreateVertexTypeStatement statement = new CreateVertexTypeStatement() {
        @Override
        protected DocumentType createType(final Schema schema, final String typeName) {
          return (DocumentType) Proxy.newProxyInstance(DocumentType.class.getClassLoader(), new Class<?>[] { DocumentType.class },
              (proxy, method, args) -> {
                if (method.getName().equals("addSuperType"))
                  throw refusal;
                if (method.getName().equals("getBuckets"))
                  return List.of();
                return null;
              });
        }
      };
      statement.name = new Identifier("Ghosty");
      statement.supertypes = List.of(new Identifier("A"));
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);

      final Throwable thrown = catchThrowable(() -> statement.executeDDL(context));

      assertThat(thrown).isSameAs(refusal);
      assertThat(thrown.getSuppressed()).as("the failure of the take-back").hasSize(1);
      assertThat(database.getSchema().existsType("Ghosty")).isFalse();

      // Nothing was left locked or half published: the next statement is one entry as usual
      final long versionBefore = schema().getVersion();
      assertThat(SchemaSessionCounter.count((LocalDatabase) database, () -> database.command("sql", "CREATE VERTEX TYPE Fine EXTENDS A").close()))
          .isEqualTo(1);
      assertThat(schema().getVersion() - versionBefore).isEqualTo(1);
    }

    /**
     * A value that is refused once the type exists (setting it is a schema change of its own) takes the type back like a refused link: the
     * statement ends with the refusal, nothing of the type is left, and the take-back, which succeeds here, adds nothing suppressed.
     */
    @Test
    void aRefusedCustomValueTakesTheTypeBack() {
      final SchemaException refusal = new SchemaException("the value is refused");
      final CreateVertexTypeStatement parsed = (CreateVertexTypeStatement) ((DatabaseInternal) database).getStatementCache()
          .get("CREATE VERTEX TYPE Valued CUSTOM k = 1");
      final CreateVertexTypeStatement statement = new CreateVertexTypeStatement() {
        @Override
        protected DocumentType createType(final Schema schema, final String typeName) {
          // A real type, so that the take-back has something to take back, which refuses the one step after it
          final DocumentType real = schema.createVertexType(typeName);
          return (DocumentType) Proxy.newProxyInstance(DocumentType.class.getClassLoader(), new Class<?>[] { DocumentType.class },
              (proxy, method, args) -> {
                if (method.getName().equals("setCustomValue"))
                  throw refusal;
                try {
                  return method.invoke(real, args);
                } catch (final InvocationTargetException e) {
                  throw e.getCause();
                }
              });
        }
      };
      statement.name = parsed.name;
      statement.customProperties.putAll(parsed.customProperties);
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);

      final Throwable thrown = catchThrowable(() -> statement.executeDDL(context));

      assertThat(thrown).isSameAs(refusal);
      assertThat(thrown.getSuppressed()).as("the take-back succeeded").isEmpty();
      assertThat(database.getSchema().existsType("Valued")).isFalse();
      assertThat(database.getSchema().getBuckets()).extracting(bucket -> bucket.getName()).noneMatch(name -> name.startsWith("Valued_"));
    }

    /**
     * A value that cannot be computed refuses a statement for a type that is still to be created, and leaves nothing behind. A statement
     * that lost the race for an {@code IF NOT EXISTS} type does not fail on it: it answers that the type is there, which is what makes the
     * statement safe to repeat. The race is made at the one point it can happen, between the first look at the schema and the lock, and the
     * value is computed before the lock because a user expression must not hold every writer of the database.
     */
    @Test
    void aCustomValueThatCannotBeComputedRefusesTheStatementUnlessItLostTheRaceForAnIfNotExistsType() {
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);

      final Throwable alone = catchThrowable(() -> statementWithAFailingCustomValue("Unvalued", false, false).executeDDL(context));
      assertThat(alone).isInstanceOf(CommandExecutionException.class).hasMessageContaining("the value cannot be computed");
      assertThat(database.getSchema().existsType("Unvalued")).as("a refused statement leaves nothing behind").isFalse();

      try (final ResultSet answer = statementWithAFailingCustomValue("Raced", true, true).executeDDL(context)) {
        assertThat(answer.next().<Boolean>getProperty("created")).as("the type is there, made by the statement that won").isFalse();
      }
      assertThat(database.getSchema().existsType("Raced")).isTrue();

      // Without IF NOT EXISTS the loser is still told the type is there, not that a value failed
      assertThat(catchThrowable(() -> statementWithAFailingCustomValue("RacedAgain", false, true).executeDDL(context)))
          .isInstanceOf(TypeAlreadyExistsException.class);
    }

    /**
     * A {@code CREATE VERTEX TYPE} whose one {@code CUSTOM} value throws when it is computed. With {@code losesTheRace} another statement
     * creates the type right after the statement first looked at the schema.
     */
    private CreateVertexTypeStatement statementWithAFailingCustomValue(final String typeName, final boolean ifNotExists,
        final boolean losesTheRace) {
      final CreateVertexTypeStatement statement = new CreateVertexTypeStatement() {
        @Override
        protected void checkSuperTypes(final Schema schema, final CommandContext context) {
          super.checkSuperTypes(schema, context);
          if (losesTheRace)
            schema.createVertexType(typeName);
        }
      };
      statement.name = new Identifier(typeName);
      statement.ifNotExists = ifNotExists;
      statement.customProperties.put(new Identifier("k"), new Expression() {
        @Override
        public Object execute(final Identifiable currentRecord, final CommandContext context) {
          throw new CommandExecutionException("the value cannot be computed");
        }
      });
      return statement;
    }

    private LocalSchema schema() {
      return ((LocalDatabase) database).getSchema().getEmbedded();
    }
  }

  /**
   * Regression for #10317 (case A): {@code CREATE DOCUMENT TYPE B BUCKET A_0}, where {@code A_0} belongs to {@code A}, was refused, but only
   * after {@code B} had been registered, so {@code B} stayed behind with no bucket, also after a reopen. The bucket is now checked
   * before the type is created, and a refused statement leaves the schema as it was.
   */
  @Nested
  class CreateTypeTakenBucket extends TestHelper {

    @Test
    void createTypeWithAnotherTypesBucketLeavesNoTypeBehind() {
      database.command("sql", "CREATE DOCUMENT TYPE A");

      assertThatThrownBy(() -> database.command("sql", "CREATE DOCUMENT TYPE B BUCKET A_0"))
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("already associated to the type 'A'");

      assertThat(database.getSchema().existsType("B")).isFalse();
      assertThat(database.getSchema().getType("A").getBuckets(false)).extracting(b -> b.getName()).containsExactly("A_0");

      reopenDatabase();

      assertThat(database.getSchema().existsType("B")).isFalse();
      assertThat(database.getSchema().getType("A").getBuckets(false)).extracting(b -> b.getName()).containsExactly("A_0");
    }

    @Test
    void typeBuilderWithAnotherTypesBucketLeavesNoTypeBehind() {
      final DocumentType a = database.getSchema().createDocumentType("A");

      assertThatThrownBy(() -> database.getSchema().buildDocumentType().withName("B").withBuckets(List.of(a.getBuckets(false).getFirst()))
          .create())
          .isInstanceOf(SchemaException.class);

      assertThat(database.getSchema().existsType("B")).isFalse();
    }

    @Test
    void createTypeReusingABucketNamedLikeItsOwnButOwnedByAnotherLeavesNoTypeBehind() {
      // C_0 is the name CREATE TYPE C would give its first bucket, and it already belongs to A
      database.command("sql", "CREATE BUCKET C_0");
      database.command("sql", "CREATE DOCUMENT TYPE A BUCKET C_0");

      assertThatThrownBy(() -> database.command("sql", "CREATE DOCUMENT TYPE C BUCKETS 1"))
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("already associated to the type 'A'");

      assertThat(database.getSchema().existsType("C")).isFalse();
    }

    @Test
    void createTypeWithTheSameBucketTwiceLeavesNoTypeBehind() {
      // the second Free is refused by addBucket as "already associated to B", which B only is once the first one is attached
      database.command("sql", "CREATE BUCKET Free");

      assertThatThrownBy(() -> database.command("sql", "CREATE DOCUMENT TYPE B BUCKET Free, Free"))
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("'Free'");

      assertThat(database.getSchema().existsType("B")).isFalse();
      assertThat(database.getSchema().existsBucket("Free")).isTrue();

      reopenDatabase();

      assertThat(database.getSchema().existsType("B")).isFalse();
      assertThat(database.getSchema().existsBucket("Free")).isTrue();
    }

    @Test
    void typeBuilderWithTheSameBucketTwiceLeavesNoTypeBehind() {
      final Bucket free = database.getSchema().createBucket("Free");

      assertThatThrownBy(() -> database.getSchema().buildDocumentType().withName("B").withBuckets(List.of(free, free)).create())
          .isInstanceOf(SchemaException.class);

      assertThat(database.getSchema().existsType("B")).isFalse();
    }

    @Test
    void growingAnExistingTypeOntoATakenBucketLeavesItAsItWas() {
      // G_1 is free and reused, G_2 belongs to Owner: the refusal must not keep the G_1 attached on the way
      database.command("sql", "CREATE DOCUMENT TYPE G BUCKETS 1");
      database.command("sql", "CREATE BUCKET G_1");
      database.command("sql", "CREATE BUCKET G_2");
      database.command("sql", "CREATE DOCUMENT TYPE Owner BUCKET G_2");

      assertThatThrownBy(() -> database.getSchema().buildDocumentType().withName("G").withIgnoreIfExists(true).withTotalBuckets(3)
          .create())
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("already associated to the type 'Owner'");

      assertThat(database.getSchema().getType("G").getBuckets(false)).extracting(b -> b.getName()).containsExactly("G_0");
      assertThat(database.getSchema().getType("Owner").getBuckets(false)).extracting(b -> b.getName()).containsExactly("G_2");

      reopenDatabase();

      assertThat(database.getSchema().getType("G").getBuckets(false)).extracting(b -> b.getName()).containsExactly("G_0");
    }

    @Test
    void growingAnExistingTypeOntoFreeBucketsStillWorks() {
      database.command("sql", "CREATE DOCUMENT TYPE G BUCKETS 1");
      database.command("sql", "CREATE BUCKET G_1");

      database.getSchema().buildDocumentType().withName("G").withIgnoreIfExists(true).withTotalBuckets(3).create();

      assertThat(database.getSchema().getType("G").getBuckets(false)).extracting(b -> b.getName())
          .containsExactly("G_0", "G_1", "G_2");
    }

    @Test
    void createTypeWithAFreeBucketStillWorks() {
      database.command("sql", "CREATE BUCKET Free");
      database.command("sql", "CREATE DOCUMENT TYPE B BUCKET Free");

      assertThat(database.getSchema().getType("B").getBuckets(false)).extracting(b -> b.getName()).containsExactly("Free");
    }
  }

  /**
   * Regression for #10394: creating a type registered it in the schema and gave it its buckets BEFORE its super types were linked, and the link
   * can still refuse (a TIMESERIES type on either end, a paired external-property bucket another type owns, a partition the subtype
   * cannot take, an index that cannot be propagated). The refusal then left the type behind, registered, with its buckets and with
   * the super types that had been linked before the refused one, also after a reopen. #9846 and #10317 each added a pre-check in
   * front of the registration for the refusals they knew; the rule is that a refused create leaves the schema as it found it:
   * <ul>
   *   <li>every refusal {@code addSuperType} can give before it changes anything is asked before anything is created;</li>
   *   <li>a refusal that can only come out of the linkage itself takes the type back, with the buckets it created and the super
   *       types it linked, and leaves alone the buckets it was given or found.</li>
   * </ul>
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CreateTypeRegistersLast extends TestHelper {

    @Test
    void typeBuilderUnderATimeSeriesTypeLeavesNothingBehind() {
      createTimeSeriesType("Metrics");

      assertThatThrownBy(() -> database.getSchema().buildDocumentType().withName("Child").withSuperType("Metrics").create())
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("TIMESERIES");

      assertNothingLeftOf("Child");
      assertThat(database.getSchema().getType("Metrics").getSubTypes()).isEmpty();

      reopenDatabase();

      assertNothingLeftOf("Child");
      assertThat(database.getSchema().getType("Metrics").getSubTypes()).isEmpty();
    }

    @Test
    void typeBuilderOfEveryKindUnderATimeSeriesTypeLeavesNothingBehind() {
      createTimeSeriesType("Metrics");

      assertThatThrownBy(() -> database.getSchema().buildVertexType().withName("V").withSuperType("Metrics").create())
          .isInstanceOf(SchemaException.class);
      assertThatThrownBy(() -> database.getSchema().buildEdgeType().withName("E").withSuperType("Metrics").create())
          .isInstanceOf(SchemaException.class);

      assertNothingLeftOf("V");
      assertNothingLeftOf("E");
      // A vertex type owns two more buckets for its edge lists: they are not left either
      assertThat(database.getSchema().getBuckets()).extracting(Bucket::getName).noneMatch(name -> name.startsWith("V_"));
    }

    @Test
    void aRefusedSecondSuperTypeLeavesTheFirstOneUnlinked() {
      database.getSchema().createDocumentType("Plain");
      createTimeSeriesType("Metrics");

      assertThatThrownBy(
          () -> database.getSchema().buildDocumentType().withName("Child").withSuperType("Plain").withSuperType("Metrics").create())
          .isInstanceOf(SchemaException.class);

      assertNothingLeftOf("Child");
      // The super type that was linked before the refused one does not keep a sub type that does not exist
      assertThat(database.getSchema().getType("Plain").getSubTypes()).isEmpty();

      reopenDatabase();

      assertNothingLeftOf("Child");
      assertThat(database.getSchema().getType("Plain").getSubTypes()).isEmpty();
    }

    @Test
    void sqlCreateTypeUnderATimeSeriesTypeLeavesNothingBehind() {
      createTimeSeriesType("Metrics");
      database.getSchema().createDocumentType("Plain");

      for (final String kind : new String[] { "DOCUMENT", "VERTEX", "EDGE" }) {
        assertThatThrownBy(() -> database.command("sql", "CREATE " + kind + " TYPE Child EXTENDS Plain, Metrics"))
            .as(kind).isInstanceOf(SchemaException.class).hasMessageContaining("TIMESERIES");

        assertNothingLeftOf("Child");
        assertThat(database.getSchema().getType("Plain").getSubTypes()).as(kind).isEmpty();
      }
    }

    /**
     * A refusal only the linkage can know: the super type has an EXTERNAL property, so the new type needs a paired
     * {@code <bucket>_ext} bucket, and a bucket of that name is already the primary bucket of another type. Nothing before the
     * linkage looks at it, so it is the take-back that has to leave the schema as it was.
     */
    @Test
    void aRefusalOnlyTheLinkageCanKnowTakesTheTypeBackWithItsBuckets() {
      createExternalSuperType();
      squat("X_0_ext");

      assertThatThrownBy(() -> database.getSchema().buildDocumentType().withName("X").withSuperType("Holder").create())
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("X_0_ext");

      assertNothingLeftOf("X", "X_0_ext");
      assertThat(database.getSchema().getType("Holder").getSubTypes()).isEmpty();
      assertSquatterIntact("X_0_ext");

      reopenDatabase();

      assertNothingLeftOf("X", "X_0_ext");
      assertThat(database.getSchema().getType("Holder").getSubTypes()).isEmpty();
      assertSquatterIntact("X_0_ext");
    }

    @Test
    void theTakeBackKeepsTheBucketsTheTypeWasGivenOrFound() {
      createExternalSuperType();
      squat("Y_0_ext");
      squat("Free_ext");
      // Y_0 is the name CREATE TYPE Y would give its first bucket and it exists already: the type reuses it, so it is not the type's
      // to drop. Free is a bucket the caller hands over. Neither belongs to a type before the create
      final Bucket found = database.getSchema().createBucket("Y_0");
      final Bucket given = database.getSchema().createBucket("Free");

      assertThatThrownBy(() -> database.getSchema().buildDocumentType().withName("Y").withSuperType("Holder").create())
          .isInstanceOf(SchemaException.class);
      assertNothingLeftOf("Y", "Y_0", "Y_0_ext");
      assertThat(database.getSchema().existsBucket("Y_0")).isTrue();
      assertThat(database.getSchema().getBucketByName("Y_0").getFileId()).isEqualTo(found.getFileId());

      assertThatThrownBy(() -> database.getSchema().buildDocumentType().withName("Z").withBuckets(List.of(given))
          .withSuperType("Holder").create())
          .isInstanceOf(SchemaException.class);
      assertNothingLeftOf("Z");
      assertThat(database.getSchema().existsBucket("Free")).isTrue();
      assertThat(database.getSchema().getBucketByName("Free").getFileId()).isEqualTo(given.getFileId());

      reopenDatabase();

      assertNothingLeftOf("Y", "Y_0", "Y_0_ext");
      assertNothingLeftOf("Z");
      assertThat(database.getSchema().existsBucket("Y_0")).isTrue();
      assertThat(database.getSchema().existsBucket("Free")).isTrue();
    }

    /**
     * The link of the first super type indexed the records of the found bucket (an index file over data that is not the type's to
     * lose), and the second one is refused. The take-back drops that index file with the type, never the bucket nor its records.
     */
    @Test
    void theTakeBackKeepsTheRecordsOfAFoundBucketTheLinkIndexed() {
      database.command("sql", "CREATE DOCUMENT TYPE Indexed");
      database.command("sql", "CREATE PROPERTY Indexed.p STRING");
      database.command("sql", "CREATE INDEX ON Indexed (p) NOTUNIQUE");
      createExternalSuperType();
      squat("W_0_ext");
      database.getSchema().createBucket("W_0");
      database.transaction(() -> ((DatabaseInternal) database).createRecord(database.newDocument("Indexed").set("p", "kept"), "W_0"));
      final int indexFilesBefore = subIndexCountOf("Indexed");

      assertThatThrownBy(() -> database.getSchema().buildDocumentType().withName("W").withSuperType("Indexed").withSuperType("Holder")
          .create())
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("W_0_ext");

      assertNothingLeftOf("W", "W_0", "W_0_ext");
      assertThat(database.getSchema().existsBucket("W_0")).isTrue();
      assertThat(database.countBucket("W_0")).isEqualTo(1L);
      assertThat(subIndexCountOf("Indexed")).isEqualTo(indexFilesBefore);

      reopenDatabase();

      assertThat(database.getSchema().existsBucket("W_0")).isTrue();
      assertThat(database.countBucket("W_0")).isEqualTo(1L);
      assertThat(subIndexCountOf("Indexed")).isEqualTo(indexFilesBefore);
    }

    /**
     * The statement links its super types after it creates the type, and takes the type back when one refuses. The buckets it was
     * given (BUCKET Free) and the ones it found under its own names (C_0) were there before the statement and hold records: they
     * stay. A bucket the statement made for a name that did not exist (BUCKET Fresh) was made for the type and goes with it.
     */
    @Test
    void aRefusedStatementKeepsTheBucketsThatExistedBeforeIt() {
      createExternalSuperType();
      squat("Free_ext");
      squat("C_0_ext");
      squat("Fresh_ext");
      database.getSchema().createDocumentType("Plain");
      database.getSchema().createBucket("Free");
      database.getSchema().createBucket("C_0");
      database.transaction(() -> {
        ((DatabaseInternal) database).createRecord(database.newDocument("Plain").set("p", "free"), "Free");
        ((DatabaseInternal) database).createRecord(database.newDocument("Plain").set("p", "found"), "C_0");
      });

      assertThatThrownBy(() -> database.command("sql", "CREATE DOCUMENT TYPE B EXTENDS Holder BUCKET Free"))
          .isInstanceOf(SchemaException.class).hasMessageContaining("Free_ext");
      assertThatThrownBy(() -> database.command("sql", "CREATE DOCUMENT TYPE C EXTENDS Holder"))
          .isInstanceOf(SchemaException.class).hasMessageContaining("C_0_ext");
      assertThatThrownBy(() -> database.command("sql", "CREATE DOCUMENT TYPE D EXTENDS Holder BUCKET Fresh"))
          .isInstanceOf(SchemaException.class).hasMessageContaining("Fresh_ext");

      assertThat(database.getSchema().existsType("B")).isFalse();
      assertThat(database.getSchema().existsType("C")).isFalse();
      assertThat(database.getSchema().existsType("D")).isFalse();
      assertThat(database.getSchema().existsBucket("Free")).isTrue();
      assertThat(database.countBucket("Free")).isEqualTo(1L);
      assertThat(database.getSchema().existsBucket("C_0")).isTrue();
      assertThat(database.countBucket("C_0")).isEqualTo(1L);
      assertThat(database.getSchema().existsBucket("Fresh")).as("made for the type that was refused").isFalse();

      reopenDatabase();

      assertThat(database.countBucket("Free")).isEqualTo(1L);
      assertThat(database.countBucket("C_0")).isEqualTo(1L);
      assertThat(database.getSchema().existsType("B")).isFalse();
      assertThat(database.getSchema().existsType("C")).isFalse();
    }

    @Test
    void growingATypeOntoARefusedSecondSuperTypeLeavesItAsItWas() {
      database.getSchema().createDocumentType("Plain");
      createTimeSeriesType("Metrics");
      database.getSchema().createDocumentType("Grown");

      assertThatThrownBy(() -> database.getSchema().buildDocumentType().withName("Grown").withIgnoreIfExists(true)
          .withSuperType("Plain").withSuperType("Metrics").create())
          .isInstanceOf(SchemaException.class);

      assertThat(database.getSchema().getType("Grown").getSuperTypes()).isEmpty();
      assertThat(database.getSchema().getType("Plain").getSubTypes()).isEmpty();

      reopenDatabase();

      assertThat(database.getSchema().getType("Grown").getSuperTypes()).isEmpty();
      assertThat(database.getSchema().getType("Plain").getSubTypes()).isEmpty();
    }

    /**
     * The unlink of a super type linked earlier in the same growth is {@code removeSuperType}, which also drops the sub-indexes the
     * link propagated: the refusal comes from the second super type, after the first has put an index over the grown type's bucket.
     */
    @Test
    void growingATypeOntoARefusedSecondSuperTypeTakesTheIndexesOfTheFirstWithIt() {
      database.command("sql", "CREATE DOCUMENT TYPE Indexed");
      database.command("sql", "CREATE PROPERTY Indexed.p STRING");
      database.command("sql", "CREATE INDEX ON Indexed (p) NOTUNIQUE");
      createExternalSuperType();
      database.getSchema().createDocumentType("Grown");
      squat("Grown_0_ext");
      final int indexFilesBefore = subIndexCountOf("Indexed");

      assertThatThrownBy(() -> database.getSchema().buildDocumentType().withName("Grown").withIgnoreIfExists(true)
          .withSuperType("Indexed").withSuperType("Holder").create())
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("Grown_0_ext");

      assertThat(database.getSchema().getType("Grown").getSuperTypes()).isEmpty();
      assertThat(database.getSchema().getType("Indexed").getSubTypes()).isEmpty();
      assertThat(database.getSchema().getType("Grown").getAllIndexes(true)).isEmpty();
      assertThat(subIndexCountOf("Indexed")).isEqualTo(indexFilesBefore);

      reopenDatabase();

      assertThat(database.getSchema().getType("Grown").getSuperTypes()).isEmpty();
      assertThat(database.getSchema().getType("Grown").getAllIndexes(true)).isEmpty();
      assertThat(subIndexCountOf("Indexed")).isEqualTo(indexFilesBefore);
      // The type still takes records, and the index of the super type it never joined holds none of them
      database.transaction(() -> database.newDocument("Grown").set("p", "x").save());
      assertThat(database.query("sql", "SELECT FROM Indexed WHERE p = 'x'").stream().count()).isZero();
    }

    @Test
    void aTypeUnderValidSuperTypesIsStillCreatedWithEveryLink() {
      database.getSchema().createDocumentType("A");
      database.getSchema().createDocumentType("B");

      final DocumentType child = database.getSchema().buildDocumentType().withName("Child").withSuperType("A").withSuperType("B")
          .create();

      assertThat(child.getSuperTypes()).extracting(DocumentType::getName).containsExactlyInAnyOrder("A", "B");
      assertThat(database.getSchema().getType("A").getSubTypes()).extracting(DocumentType::getName).containsExactly("Child");

      reopenDatabase();

      assertThat(database.getSchema().getType("Child").getSuperTypes()).extracting(DocumentType::getName)
          .containsExactlyInAnyOrder("A", "B");
    }

    /** The type that was refused is not in the schema under its own name, nor through a bucket named after it. */
    private void assertNothingLeftOf(final String typeName, final String... keptBuckets) {
      final List<String> kept = List.of(keptBuckets);
      assertThat(database.getSchema().existsType(typeName)).as("type " + typeName).isFalse();
      assertThat(database.getSchema().getBuckets()).extracting(Bucket::getName).as("buckets named after " + typeName)
          .noneMatch(name -> name.startsWith(typeName + "_") && !kept.contains(name));
    }

    private void createTimeSeriesType(final String name) {
      database.command("sql", "CREATE TIMESERIES TYPE " + name + " TIMESTAMP ts TAGS (sensor STRING) FIELDS (temperature DOUBLE)");
    }

    /** The bucket indexes of every index {@code typeName} declares: one per bucket of the types the index covers. */
    private int subIndexCountOf(final String typeName) {
      int count = 0;
      for (final TypeIndex index : database.getSchema().getType(typeName).getAllIndexes(false))
        count += index.getIndexesOnBuckets().length;
      return count;
    }

    /** A super type with an EXTERNAL property: every type below it needs a paired {@code <bucket>_ext} bucket. */
    private void createExternalSuperType() {
      database.command("sql", "CREATE DOCUMENT TYPE Holder");
      database.command("sql", "CREATE PROPERTY Holder.blob STRING (EXTERNAL true)");
    }

    /** Makes {@code bucketName}, the name of a paired external bucket a type would adopt, the primary bucket of another type. */
    private void squat(final String bucketName) {
      database.command("sql", "CREATE BUCKET " + bucketName);
      database.command("sql", "CREATE DOCUMENT TYPE Squatter_" + bucketName + " BUCKET " + bucketName);
    }

    private void assertSquatterIntact(final String bucketName) {
      final String squatter = "Squatter_" + bucketName;
      assertThat(database.getSchema().existsType(squatter)).isTrue();
      assertThat(database.getSchema().getType(squatter).getBuckets(false)).extracting(Bucket::getName).containsExactly(bucketName);
    }
  }

  /**
   * Regression for #10537: the one schema publication per type creation is a property of creating a type, not of one SQL statement.
   * <p>
   * #10147 made {@code CREATE ... TYPE} one outermost schema frame, which is one {@code SCHEMA_ENTRY} under Raft, and put the boundary
   * in the statement. Every other route to the same schema change went through {@link TypeBuilder} or {@code Labels.ensureCompositeType}
   * and published as many entries as it made steps: the Java builder of a UNIQUE edge type four (the type, two properties and the
   * index, the constraint being built outside the type's own frame), a two-label Cypher create three (each label, then the composite),
   * three labels four. A follower that counts the entries a transaction has to tolerate reads every one of those as somebody else's schema
   * change, and the {@code engine} and {@code ha-raft} guidance states the one-entry rule as if it held for all of them.
   * <p>
   * The guarantee is the one {@code TypeCreationTest.CreateTypeOneSchemaEntry} makes for the statement, over every route that creates, grows
   * or links a type: one outermost session (what Raft publishes, see {@link SchemaSessionCounter}) and one schema version, and none
   * at all when the type is there already.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class TypeCreationOneSchemaEntry extends TestHelper {

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
      // The source of the per-record materialized view routes, and a MANUAL view over it the ALTER upgrades
      database.command("sql", "CREATE DOCUMENT TYPE MvSource");
      database.transaction(() -> database.newDocument("MvSource").set("name", "n").save());
      database.getSchema().buildMaterializedView().withName("MvManual").withQuery("SELECT name FROM MvSource")
          .withRefreshMode(MaterializedViewRefreshMode.MANUAL).create();
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
      ALTER_TYPE_UNIQUE(db -> db.command("sql", "ALTER TYPE Populated WITH unique = true").close()),
      // A per-record materialized view builds the UNIQUE index on its source link, at creation and at the ALTER upgrade (#11475)
      MV_PER_RECORD_CREATE(db -> {
        db.getSchema().buildMaterializedView().withName("MvPerRecord").withQuery("SELECT name FROM MvSource")
            .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL).create();
        assertThat(((MaterializedViewImpl) db.getSchema().getMaterializedView("MvPerRecord")).isPerRecord()).isTrue();
      }),
      MV_PER_RECORD_ALTER_UPGRADE(db -> {
        db.command("sql", "ALTER MATERIALIZED VIEW MvManual REFRESH INCREMENTAL").close();
        assertThat(((MaterializedViewImpl) db.getSchema().getMaterializedView("MvManual")).isPerRecord()).isTrue();
      });

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

  /**
   * Regression for #10923: {@code DROP TYPE} asked whether an edge type held any RECORD. A lightweight edge holds none, so the drop was accepted
   * while every edge was live in the vertices' adjacency lists, leaving entries no surface could read or delete.
   */
  @Nested
  class DropTypeLightweightEdges extends TestHelper {

    @Override
    protected boolean isCheckingDatabaseIntegrity() {
      // unsafeStillDropsAnEdgeTypeWithLightEdges strands entries on purpose, and CHECK DATABASE now reports them (#11142)
      return false;
    }

    private MutableVertex[] createVerticesAndSchema(final String edgeDdl) {
      database.command("sql", "CREATE VERTEX TYPE V");
      database.command("sql", edgeDdl);
      final MutableVertex[] vertices = new MutableVertex[2];
      database.transaction(() -> {
        vertices[0] = database.newVertex("V").save();
        vertices[1] = database.newVertex("V").save();
      });
      return vertices;
    }

    @Test
    void dropOfLightweightEdgeTypeWithEdgesIsRefused() {
      final MutableVertex[] v = createVerticesAndSchema("CREATE EDGE TYPE EL LIGHTWEIGHT");
      database.transaction(() -> v[0].newLightEdge("EL", v[1]));

      assertThatThrownBy(() -> database.command("sql", "DROP TYPE EL")).isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("contains Edges");

      assertThat(database.getSchema().existsType("EL")).isTrue();
      assertThat(v[0].countEdges(Vertex.DIRECTION.OUT, "EL")).isEqualTo(1);
    }

    @Test
    void dropOfUnidirectionalLightweightEdgeTypeWithEdgesIsRefused() {
      final MutableVertex[] v = createVerticesAndSchema("CREATE EDGE TYPE EUL UNIDIRECTIONAL LIGHTWEIGHT");
      database.transaction(() -> v[0].newLightEdge("EUL", v[1]));

      assertThatThrownBy(() -> database.command("sql", "DROP TYPE EUL")).isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("contains Edges");
      assertThat(database.getSchema().existsType("EUL")).isTrue();
    }

    @Test
    void dropOfPlainEdgeTypeHoldingLightEdgesIsRefused() {
      final MutableVertex[] v = createVerticesAndSchema("CREATE EDGE TYPE E");
      database.transaction(() -> v[0].newLightEdge("E", v[1]));

      assertThatThrownBy(() -> database.command("sql", "DROP TYPE E")).isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("contains Edges");
      assertThat(database.getSchema().existsType("E")).isTrue();
    }

    @Test
    void unsafeStillDropsAnEdgeTypeWithLightEdges() {
      final MutableVertex[] v = createVerticesAndSchema("CREATE EDGE TYPE EL LIGHTWEIGHT");
      database.transaction(() -> v[0].newLightEdge("EL", v[1]));

      database.command("sql", "DROP TYPE EL UNSAFE");
      assertThat(database.getSchema().existsType("EL")).isFalse();
    }

    @Test
    void emptyLightweightEdgeTypeCanBeDropped() {
      createVerticesAndSchema("CREATE EDGE TYPE EL LIGHTWEIGHT");
      database.command("sql", "DROP TYPE EL");
      assertThat(database.getSchema().existsType("EL")).isFalse();
    }

    @Test
    void dropSucceedsOnceTheLightEdgesAreDeleted() {
      final MutableVertex[] v = createVerticesAndSchema("CREATE EDGE TYPE EL LIGHTWEIGHT");
      database.transaction(() -> v[0].newLightEdge("EL", v[1]));

      database.transaction(() -> database.command("sql", "DELETE FROM EL"));
      database.command("sql", "DROP TYPE EL");
      assertThat(database.getSchema().existsType("EL")).isFalse();
    }

    @Test
    void edgeTypeOnlyBlockedByItsOwnEdges() {
      final MutableVertex[] v = createVerticesAndSchema("CREATE EDGE TYPE EL LIGHTWEIGHT");
      database.command("sql", "CREATE EDGE TYPE EOTHER LIGHTWEIGHT");
      database.transaction(() -> v[0].newLightEdge("EL", v[1]));

      database.command("sql", "DROP TYPE EOTHER");
      assertThat(database.getSchema().existsType("EOTHER")).isFalse();
    }

    @Test
    void lightEdgeOnAVertexOfASubtypeIsFound() {
      database.command("sql", "CREATE VERTEX TYPE V");
      database.command("sql", "CREATE VERTEX TYPE SubV EXTENDS V");
      database.command("sql", "CREATE EDGE TYPE EL LIGHTWEIGHT");
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("SubV").save();
        final MutableVertex b = database.newVertex("SubV").save();
        a.newLightEdge("EL", b);
      });

      assertThatThrownBy(() -> database.command("sql", "DROP TYPE EL")).isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("contains Edges");
    }

    @Test
    void dropOfEdgeTypeWithRecordEdgesIsRefused() {
      final MutableVertex[] v = createVerticesAndSchema("CREATE EDGE TYPE ER");
      database.transaction(() -> v[0].newEdge("ER", v[1]));

      assertThatThrownBy(() -> database.command("sql", "DROP TYPE ER")).isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("contains Edges");
    }
  }

  /**
   * Regression for #11063: a follower inside a client transaction has the leader create the types of a multi-label node, and it must be ONE
   * schema entry (the follower's re-base requires it, see #10067). The statement the follower sends is {@code ensureLabelTypes}: the
   * labels that are missing and the composite, in one outermost schema frame, answering whether this call made the composite.
   */
  @Nested
  class EnsureLabelTypesFunction extends TestHelper {

    @Test
    void missingLabelsAndTheCompositeAreOneSchemaEntry() {
      final int sessions = SchemaSessionCounter.count((LocalDatabase) database, () -> {
        try (final ResultSet rs = database.command("sql", "SELECT ensureLabelTypes(['Lb2', 'Lb1']) AS created")) {
          assertThat(rs.next().<Boolean>getProperty("created")).isTrue();
        }
      });

      assertThat(sessions).isEqualTo(1);
      assertThat(database.getSchema().existsType("Lb1")).isTrue();
      assertThat(database.getSchema().existsType("Lb2")).isTrue();
      assertThat(database.getSchema().getType("Lb1~Lb2").instanceOf("Lb1")).isTrue();
      assertThat(database.getSchema().getType("Lb1~Lb2").instanceOf("Lb2")).isTrue();
    }

    @Test
    void aCompositeThatIsThereAlreadyAnswersNotCreatedAndPublishesNothing() {
      ensure("['Lc1', 'Lc2']");

      final int sessions = SchemaSessionCounter.count((LocalDatabase) database, () -> {
        try (final ResultSet rs = database.command("sql", "SELECT ensureLabelTypes(['Lc1', 'Lc2']) AS created")) {
          assertThat(rs.next().<Boolean>getProperty("created")).isFalse();
        }
      });

      assertThat(sessions).isZero();
    }

    @Test
    void aLabelWithAQuoteIsPassedThroughIntact() {
      ensure("['it\\'s', 'Ld2']");
      assertThat(database.getSchema().existsType("it's")).isTrue();
    }

    @Test
    void theRenderedStatementCarriesBackslashQuoteAndNewlineIntact() {
      final Set<String> labels = new TreeSet<>(List.of("back\\slash", "it's", "new\nline"));
      try (final ResultSet rs = database.command("sql", Labels.ensureLabelTypesStatement(labels))) {
        assertThat(rs.next().<Boolean>getProperty("created")).isTrue();
      }
      for (final String label : labels)
        assertThat(database.getSchema().existsType(label)).as(label).isTrue();
    }

    @Test
    void aNullLabelIsRefusedClearly() {
      assertThatThrownBy(() -> ensure("['Le1', null]")).hasMessageContaining("null label");
    }

    /** The result set is lazy: the function runs when the row is read. */
    private void ensure(final String labels) {
      try (final ResultSet rs = database.command("sql", "SELECT ensureLabelTypes(" + labels + ") AS created")) {
        rs.next();
      }
    }
  }
}
