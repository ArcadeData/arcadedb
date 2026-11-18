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
import com.arcadedb.database.RID;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.fulltext.LSMTreeFullTextIndex;
import com.arcadedb.index.geospatial.LSMTreeGeoIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.index.sparsevector.SegmentFormat;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for the configuration of an index carried over by a copy or a rebuild, one nested class per fixed issue.
 */
class IndexConfigurationTest {
  /**
   * Regression for #5606: {@link TypeIndexBuilder#withType} hands back a SPECIALISED builder for the index types that carry
   * settings of their own, so every setting configured before that call has to survive being copied onto the new
   * instance. The four specialised builders used to hand-copy the same eleven fields each, which made a field added to
   * {@link IndexBuilder} work for {@code LSM_TREE} and {@code HASH} - which never swap builders - and evaporate for
   * FULL_TEXT, GEOSPATIAL and the two vector types, with no compile error and nothing to notice it.
   * <p>
   * The test is deliberately reflective rather than a list of hand-written assertions, for exactly the reason the defect
   * existed: a hand-written list is a second field list to keep in sync, and the first one already went stale.
   * {@link #everySettingSurvivesTheBuilderSwap} compares EVERY declared field, and
   * {@link #everySettingIsActuallyConfiguredByThisTest} is what stops that comparison from going vacuous - it fails when
   * a field is left at its default by the setup, which is what a newly added field looks like.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class BuilderStateCarryOver extends TestHelper {
    private static final String   TYPE_NAME     = "Issue5606";
    private static final String   BUCKET_NAME   = "Issue5606_bucket";
    private static final String[] PROPERTY_NAME = { "name" };

    /**
     * The index types {@link TypeIndexBuilder#withType} answers with a builder OTHER than itself. Kept as an enum rather
     * than read off {@code Schema.INDEX_TYPE} so that adding an index type without a specialised builder does not
     * silently widen the test into asserting nothing.
     */
    enum SpecialisedBuilder {
      FULL_TEXT(Schema.INDEX_TYPE.FULL_TEXT, TypeFullTextIndexBuilder.class, FullTextIndexMetadata.class),
      LSM_VECTOR(Schema.INDEX_TYPE.LSM_VECTOR, TypeLSMVectorIndexBuilder.class, LSMVectorIndexMetadata.class),
      LSM_SPARSE_VECTOR(Schema.INDEX_TYPE.LSM_SPARSE_VECTOR, TypeLSMSparseVectorIndexBuilder.class,
          LSMSparseVectorIndexMetadata.class),
      GEOSPATIAL(Schema.INDEX_TYPE.GEOSPATIAL, TypeGeoIndexBuilder.class, GeoIndexMetadata.class);

      final Schema.INDEX_TYPE                     indexType;
      final Class<? extends TypeIndexBuilder>     builderClass;
      final Class<? extends IndexMetadata>        metadataClass;

      SpecialisedBuilder(final Schema.INDEX_TYPE indexType, final Class<? extends TypeIndexBuilder> builderClass,
          final Class<? extends IndexMetadata> metadataClass) {
        this.indexType = indexType;
        this.builderClass = builderClass;
        this.metadataClass = metadataClass;
      }
    }

    /**
     * Fields the swap is FOR, so they are expected to differ on the new builder rather than to be carried over:
     * {@code indexType} is the index type being selected, {@code metadata} is replaced by the subclass the specialised
     * settings live in (its common definition is asserted separately), and {@code buildMode} cannot reach a specialised
     * builder at all - {@code withType()} refuses a sorted build for anything but {@code LSM_TREE}, which
     * {@link #sortedBuildIsRefusedBySpecialisedTypes} pins down.
     */
    private static final Set<String> NOT_CARRIED_OVER = Set.of("indexType", "metadata", "buildMode");

    /**
     * Additionally not settable, so {@link #everySettingIsActuallyConfiguredByThisTest} cannot expect them to differ
     * from a pristine builder's: the first two are structural and final, handed to the specialised builder from the source,
     * and {@code built} and {@code builtInCallerTransaction} are not settings but the outcome of the builder's own
     * {@code create()}, reset by every call (issues #9741, #10450).
     */
    private static final Set<String> NOT_CONFIGURABLE = Set.of("database", "indexImplementation", "built",
        "builtInCallerTransaction");

    /** The bucket-level identity, handed to the specialised builder rather than configured through a setter. */
    private static final Set<String> BUCKET_IDENTITY = Set.of("typeName", "bucketName", "propertyNames");

    @ParameterizedTest
    @EnumSource(SpecialisedBuilder.class)
    void everySettingSurvivesTheBuilderSwap(final SpecialisedBuilder specialised) {
      final TypeIndexBuilder source = configureEverySetting(database.getSchema().buildTypeIndex(TYPE_NAME, PROPERTY_NAME));

      final TypeIndexBuilder swapped = source.withType(specialised.indexType);

      assertThat(swapped).isInstanceOf(specialised.builderClass);
      assertThat(swapped.metadata).isInstanceOf(specialised.metadataClass);
      assertThat(swapped.getIndexType()).isEqualTo(specialised.indexType);

      for (final Field field : declaredBuilderFields()) {
        if (NOT_CARRIED_OVER.contains(field.getName()))
          continue;
        assertThat(Objects.deepEquals(read(field, swapped), read(field, source)))
            .withFailMessage("'%s' did not survive withType(%s): the specialised builder has <%s> but the caller had"
                    + " configured <%s>. Carry it over in TypeIndexBuilder's copy constructor (or in"
                    + " IndexBuilder.copyBaseFieldsFrom when the field is declared there) - see issue #5606.",
                field.getName(), specialised.indexType, describe(read(field, swapped)), describe(read(field, source)))
            .isTrue();
      }

      // The identity and the common part of the definition, which live on the metadata the subclass replaces. Only
      // TypeGeoIndexBuilder used to carry collations and typeIndexName across; the other three dropped both.
      assertThat(swapped.metadata.typeName).isEqualTo(source.metadata.typeName);
      assertThat(swapped.metadata.propertyNames).isEqualTo(source.metadata.propertyNames);
      assertThat(swapped.metadata.associatedBucketId).isEqualTo(source.metadata.associatedBucketId);
      assertThat(swapped.metadata.collations).isEqualTo(source.metadata.collations);
      assertThat(swapped.metadata.typeIndexName).isEqualTo(source.metadata.typeIndexName);
    }

    /**
     * The anti-vacuity half: every field the carry-over test compares must have been moved OFF its default by
     * {@link #configureEverySetting}, or comparing it proves nothing. A field added to the builder and not wired in here
     * fails this, which is the whole point - it is the reminder the four hand-written copies never gave anyone.
     */
    @Test
    void everySettingIsActuallyConfiguredByThisTest() {
      final TypeIndexBuilder pristine = database.getSchema().buildTypeIndex(TYPE_NAME, PROPERTY_NAME);
      final TypeIndexBuilder configured = configureEverySetting(
          database.getSchema().buildTypeIndex(TYPE_NAME, PROPERTY_NAME));

      final List<String> leftAtDefault = new ArrayList<>();
      for (final Field field : declaredBuilderFields()) {
        if (NOT_CARRIED_OVER.contains(field.getName()) || NOT_CONFIGURABLE.contains(field.getName()))
          continue;
        if (Objects.deepEquals(read(field, configured), read(field, pristine)))
          leftAtDefault.add(field.getName());
      }

      assertThat(leftAtDefault)
          .withFailMessage("configureEverySetting() leaves %s at its default value, so the carry-over test does not"
              + " actually check it. Give it a distinct value there - and make sure the builder copy constructor carries"
              + " it over. See issue #5606.", leftAtDefault)
          .isEmpty();
    }

    /**
     * The reason {@code buildMode} is exempt above, asserted rather than assumed: a sorted build is refused for every
     * index type that swaps builders, so the setting cannot be observed on the other side of the swap. It is still
     * copied by the constructor, because that guard is circumstantial rather than a design.
     */
    @ParameterizedTest
    @EnumSource(SpecialisedBuilder.class)
    void sortedBuildIsRefusedBySpecialisedTypes(final SpecialisedBuilder specialised) {
      final TypeIndexBuilder builder = database.getSchema().buildTypeIndex(TYPE_NAME, PROPERTY_NAME)
          .withBuildMode(IndexBuildMode.SORTED);

      assertThatThrownBy(() -> builder.withType(specialised.indexType))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Sorted build");
    }

    /**
     * A builder that is ALREADY the specialised one must be handed back untouched: {@code withType()} is called twice on
     * the SQL path ({@code CreateIndexStatement} calls it, then {@code withGeoType()}/{@code withFullTextType()}), and a
     * second swap there would throw away everything configured in between.
     */
    @ParameterizedTest
    @EnumSource(SpecialisedBuilder.class)
    void secondWithTypeKeepsTheSameBuilder(final SpecialisedBuilder specialised) {
      final TypeIndexBuilder first = database.getSchema().buildTypeIndex(TYPE_NAME, PROPERTY_NAME)
          .withType(specialised.indexType);

      assertThat(first.withType(specialised.indexType)).isSameAs(first);
    }

    /**
     * The SAME swap one hierarchy over: {@link BucketIndexBuilder#withType} hands back a
     * {@link BucketLSMVectorIndexBuilder} for {@code LSM_VECTOR}, and that copy constructor carried the identical
     * hand-written field list - already missing {@code replaceIfIncompatible} and {@code userMetadata}, the exact two
     * this issue is about. Fixing one hierarchy and leaving the other is not a fix, so it is asserted the same way.
     */
    @Test
    void everySettingSurvivesTheBucketBuilderSwap() {
      final BucketIndexBuilder source = configureEveryBaseSetting(
          database.getSchema().buildBucketIndex(TYPE_NAME, BUCKET_NAME, PROPERTY_NAME));

      final IndexBuilder<?> swapped = source.withType(Schema.INDEX_TYPE.LSM_VECTOR);

      assertThat(swapped).isInstanceOf(BucketLSMVectorIndexBuilder.class);
      assertThat(swapped.metadata).isInstanceOf(LSMVectorIndexMetadata.class);
      assertThat(swapped.getIndexType()).isEqualTo(Schema.INDEX_TYPE.LSM_VECTOR);

      for (final Field field : declaredFieldsOf(IndexBuilder.class, BucketIndexBuilder.class)) {
        if (NOT_CARRIED_OVER.contains(field.getName()))
          continue;
        assertThat(Objects.deepEquals(read(field, swapped), read(field, source)))
            .withFailMessage("'%s' did not survive BucketIndexBuilder.withType(LSM_VECTOR): the specialised builder has"
                    + " <%s> but the caller had configured <%s>. Carry it over in BucketIndexBuilder's copy constructor"
                    + " (or in IndexBuilder.copyBaseFieldsFrom when the field is declared there) - see issue #5606.",
                field.getName(), describe(read(field, swapped)), describe(read(field, source)))
            .isTrue();
      }
    }

    /** The bucket-level half of {@link #everySettingIsActuallyConfiguredByThisTest}. */
    @Test
    void everyBaseSettingIsActuallyConfiguredByThisTest() {
      final BucketIndexBuilder pristine = database.getSchema().buildBucketIndex(TYPE_NAME, BUCKET_NAME, PROPERTY_NAME);
      final BucketIndexBuilder configured = configureEveryBaseSetting(
          database.getSchema().buildBucketIndex(TYPE_NAME, BUCKET_NAME, PROPERTY_NAME));

      final List<String> leftAtDefault = new ArrayList<>();
      for (final Field field : declaredFieldsOf(IndexBuilder.class, BucketIndexBuilder.class)) {
        if (NOT_CARRIED_OVER.contains(field.getName()) || NOT_CONFIGURABLE.contains(field.getName())
            || BUCKET_IDENTITY.contains(field.getName()))
          continue;
        if (Objects.deepEquals(read(field, configured), read(field, pristine)))
          leftAtDefault.add(field.getName());
      }

      assertThat(leftAtDefault)
          .withFailMessage("configureEveryBaseSetting() leaves %s at its default value, so the bucket-level carry-over"
              + " test does not actually check it. See issue #5606.", leftAtDefault)
          .isEmpty();
    }

    /**
     * The {@link IndexBuilder} half of {@link #configureEverySetting}, for the bucket-level builder, which declares no
     * settings of its own - only the type/bucket/properties identity.
     */
    private BucketIndexBuilder configureEveryBaseSetting(final BucketIndexBuilder builder) {
      builder.withUnique(true);
      builder.withPageSize(8192);
      builder.withNullStrategy(LSMTreeIndexAbstract.NULL_STRATEGY.ERROR);
      builder.withCallback((document, totalIndexed) -> {
      });
      builder.withIgnoreIfExists(true);
      builder.withReplaceIfIncompatible(true);
      builder.withIndexName("Issue5606ManualName");
      builder.withFilePath("target/issue5606.idx");
      builder.withKeyTypes(new Type[] { Type.STRING });
      builder.withBatchSize(37);
      builder.withMaxAttempts(11);
      builder.withUserMetadata(new JSONObject().put("issue", 5606));
      return builder;
    }

    /**
     * Sets every field of {@link IndexBuilder} and {@link TypeIndexBuilder} to something distinguishable from the
     * default, through the public API rather than reflection: an option that cannot be reached from outside the class
     * is not one the carry-over has to protect.
     */
    private TypeIndexBuilder configureEverySetting(final TypeIndexBuilder builder) {
      builder.withUnique(true);
      builder.withPageSize(8192);
      builder.withNullStrategy(LSMTreeIndexAbstract.NULL_STRATEGY.ERROR);
      builder.withCallback((document, totalIndexed) -> {
      });
      builder.withIgnoreIfExists(true);
      builder.withReplaceIfIncompatible(true);
      builder.withIndexName("Issue5606ManualName");
      builder.withFilePath("target/issue5606.idx");
      builder.withKeyTypes(new Type[] { Type.STRING });
      builder.withBatchSize(37);
      builder.withMaxAttempts(11);
      builder.withUserMetadata(new JSONObject().put("issue", 5606));
      builder.withCollations(List.of(IndexMetadata.COLLATION_CI));
      builder.withDefaultKeyTypesForUndeclaredProperties(new Type[] { Type.LONG });
      builder.withBuildMemoryBudget(4L * 1024 * 1024);
      builder.withBuildSpillDirectory(Path.of("target"));
      builder.withBuildMergeFanIn(4);
      builder.withBuildParallelism(3);
      // Not a with*() setter: typeIndexName is written straight onto the metadata by addIndexInternal, and is one of the
      // two definition fields only the geospatial builder used to carry over.
      builder.metadata.typeIndexName = "Issue5606TypeIndex";
      return builder;
    }

    /** Every instance field the two type-level builder classes declare, in a stable order. */
    private static List<Field> declaredBuilderFields() {
      return declaredFieldsOf(IndexBuilder.class, TypeIndexBuilder.class);
    }

    /** Every instance field the given classes declare, in a stable order. */
    private static List<Field> declaredFieldsOf(final Class<?>... classes) {
      final List<Field> fields = new ArrayList<>();
      for (final Class<?> clazz : classes)
        for (final Field field : clazz.getDeclaredFields())
          if (!Modifier.isStatic(field.getModifiers()) && !field.isSynthetic()) {
            field.setAccessible(true);
            fields.add(field);
          }
      fields.sort((a, b) -> a.getName().compareTo(b.getName()));
      return fields;
    }

    private static Object read(final Field field, final Object target) {
      try {
        return field.get(target);
      } catch (final IllegalAccessException e) {
        throw new IllegalStateException("Cannot read field '" + field.getName() + "'", e);
      }
    }

    private static String describe(final Object value) {
      return value instanceof Object[] array ? Arrays.toString(array) : String.valueOf(value);
    }
  }

  /**
   * Regression for #5723: {@code LocalSchema.copyType()} recreated the source type's indexes through the
   * three-argument {@code createTypeIndex} overload, so every attribute of the definition other than the index type, the
   * uniqueness flag and the property list was silently replaced by a default on the copy - the tuned page size, the null
   * strategy, the collations, and the whole type-specific configuration of a full-text, geospatial or vector index.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CopyTypeIndexConfiguration extends TestHelper {

    private static final String ENGLISH_ANALYZER = "org.apache.lucene.analysis.en.EnglishAnalyzer";

    @Test
    void copyTypeKeepsThePageSize() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("Doc").createProperty("k", Type.STRING);
        database.getSchema().buildTypeIndex("Doc", new String[] { "k" })//
            .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).withPageSize(16_384).create();
      });
      database.transaction(() -> database.newDocument("Doc").set("k", "v1").save());

      assertThat(indexOf("Doc").getPageSize()).as("the fixture must not use the default page size").isEqualTo(16_384);

      database.getSchema().copyType("Doc", "Doc2", LocalDocumentType.class, 1, 65_536, 1_000);

      final IndexInternal copied = indexOf("Doc2");
      assertThat(copied.getPageSize()).as("the copy must keep the page size of the original").isEqualTo(16_384);
      assertThat(copied.isUnique()).isTrue();
      assertThat(copied.getType()).isEqualTo(Schema.INDEX_TYPE.LSM_TREE);
      assertThat(copied.countEntries()).as("the copy must be populated with the copied records").isEqualTo(1);
    }

    /**
     * A HASH index cannot be created above 65536 bytes (#5713), and the page size a copy asks for goes back through that
     * same validating creation path. Carrying the current value blindly would make copying a type that predates the guard
     * fail; {@code getPageSizeForNewFile()} answers with a legal one instead.
     */
    @Test
    void copyTypeKeepsTheHashPageSize() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("Doc").createProperty("k", Type.STRING);
        database.getSchema().buildTypeIndex("Doc", new String[] { "k" })//
            .withType(Schema.INDEX_TYPE.HASH).withUnique(true).withPageSize(8_192).create();
      });
      database.transaction(() -> database.newDocument("Doc").set("k", "v1").save());

      assertThat(indexOf("Doc").getPageSize()).isEqualTo(8_192);

      database.getSchema().copyType("Doc", "Doc2", LocalDocumentType.class, 1, 65_536, 1_000);

      assertThat(indexOf("Doc2").getPageSize()).as("the copy must keep the page size of the original").isEqualTo(8_192);
      assertThat(indexOf("Doc2").getType()).isEqualTo(Schema.INDEX_TYPE.HASH);
    }

    @Test
    void copyTypeKeepsTheNullStrategy() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("Doc").createProperty("k", Type.STRING);
        database.getSchema().buildTypeIndex("Doc", new String[] { "k" })//
            .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false)//
            .withNullStrategy(LSMTreeIndexAbstract.NULL_STRATEGY.ERROR).create();
      });

      assertThat(indexOf("Doc").getNullStrategy()).isEqualTo(LSMTreeIndexAbstract.NULL_STRATEGY.ERROR);

      database.getSchema().copyType("Doc", "Doc2", LocalDocumentType.class, 1, 65_536, 1_000);

      assertThat(indexOf("Doc2").getNullStrategy()).as("the copy must keep the null strategy of the original")//
          .isEqualTo(LSMTreeIndexAbstract.NULL_STRATEGY.ERROR);
    }

    /**
     * The collation is what makes a lookup case-insensitive, so losing it turns a working query into one that returns
     * nothing. Asserted on behaviour, not only on the persisted attribute.
     */
    @Test
    void copyTypeKeepsTheCaseInsensitiveCollation() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("Doc").createProperty("k", Type.STRING);
        database.command("sql", "CREATE INDEX ON Doc (k COLLATE CI) UNIQUE");
      });
      database.transaction(() -> database.newDocument("Doc").set("k", "Hello").save());

      database.getSchema().copyType("Doc", "Doc2", LocalDocumentType.class, 1, 65_536, 1_000);

      assertThat(indexOf("Doc2").getMetadata().collations).as("the copy must keep the collations of the original")//
          .isEqualTo(List.of(IndexMetadata.COLLATION_CI));

      try (final ResultSet rs = database.query("sql", "SELECT FROM Doc2 WHERE k.toLowerCase() = 'hello'")) {
        assertThat(rs.hasNext()).as("the case-insensitive lookup must still match on the copy").isTrue();
      }
    }

    /**
     * The analyzer and the BM25 parameters live on the full-text wrapper, not on the LSM-Tree underneath it, so the
     * carry-over has to read them from there. A copy that lost them tokenizes differently and ranks differently.
     */
    @Test
    void copyTypeKeepsTheFullTextConfiguration() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("Doc").createProperty("text", Type.STRING);
        final TypeFullTextIndexBuilder builder = (TypeFullTextIndexBuilder) database.getSchema()//
            .buildTypeIndex("Doc", new String[] { "text" }).withType(Schema.INDEX_TYPE.FULL_TEXT);
        builder.withAnalyzer(ENGLISH_ANALYZER).withAllowLeadingWildcard(true).withDefaultOperator("AND")//
            .withBM25(1.5f, 0.5f);
        builder.create();
      });
      database.transaction(() -> database.newDocument("Doc").set("text", "the quick brown foxes").save());

      database.getSchema().copyType("Doc", "Doc2", LocalDocumentType.class, 1, 65_536, 1_000);

      final FullTextIndexMetadata copied = fullTextMetadataOf("Doc2");
      assertThat(copied.getAnalyzerClass()).as("the copy must keep the analyzer of the original").isEqualTo(ENGLISH_ANALYZER);
      assertThat(copied.isAllowLeadingWildcard()).isTrue();
      assertThat(copied.getDefaultOperator()).isEqualTo("AND");
      assertThat(copied.getBm25K1()).isEqualTo(1.5f);
      assertThat(copied.getBm25B()).isEqualTo(0.5f);
      assertThat(copied.getSimilarity()).isEqualTo(FullTextIndexMetadata.SIMILARITY_BM25);
      // The English analyzer stems "foxes" to "fox": the record is findable through the stem only if the analyzer survived.
      assertThat(indexOf("Doc2").get(new Object[] { "fox" }).hasNext()).isTrue();
    }

    /**
     * The geohash resolution and the storage layout are plain fields on the geospatial wrapper. Losing the resolution
     * changes the cell size the index is built with, silently.
     */
    @Test
    void copyTypeKeepsTheGeospatialPrecision() {
      database.transaction(() -> {
        final DocumentType type = database.getSchema().createDocumentType("Doc");
        type.createProperty("name", Type.STRING);
        type.createProperty("coords", Type.STRING);
        final TypeIndexBuilder builder = database.getSchema().buildTypeIndex("Doc", new String[] { "coords" })//
            .withType(Schema.INDEX_TYPE.GEOSPATIAL);
        final GeoIndexMetadata metadata = new GeoIndexMetadata("Doc", new String[] { "coords" }, -1);
        metadata.setPrecision(7);
        builder.withMetadata(metadata);
        builder.create();
      });
      database.transaction(//
          () -> database.command("sql", "INSERT INTO Doc SET name = 'Rome', coords = 'POINT (12.5 41.9)'"));

      database.getSchema().copyType("Doc", "Doc2", LocalDocumentType.class, 1, 65_536, 1_000);

      final LSMTreeGeoIndex copied = (LSMTreeGeoIndex) subIndexOf("Doc2");
      assertThat(copied.getPrecision()).as("the copy must keep the geohash precision of the original").isEqualTo(7);
      assertThat(copied.getTokenization()).isEqualTo(GeoIndexMetadata.TOKENIZATION.FRONTIER);

      try (final ResultSet rs = database.query("sql", "SELECT name FROM Doc2 WHERE geo.intersects(coords,"
          + " geo.geomFromText('POLYGON ((10 38, 16 38, 16 44, 10 44, 10 38))')) = true")) {
        assertThat(rs.hasNext()).isTrue();
      }
    }

    /**
     * Without its dimensions and similarity a vector index is not merely differently tuned, it is unusable: the copy
     * would come up with dimensions 0 and reject every write.
     */
    @Test
    void copyTypeKeepsTheVectorConfiguration() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("Doc").createProperty("embedding", Type.ARRAY_OF_FLOATS);
        database.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {dimensions: 4, similarity: 'EUCLIDEAN'}");
      });
      database.transaction(() -> database.newDocument("Doc").set("embedding", new float[] { 1f, 2f, 3f, 4f }).save());

      database.getSchema().copyType("Doc", "Doc2", LocalDocumentType.class, 1, 65_536, 1_000);

      final LSMVectorIndexMetadata copied = (LSMVectorIndexMetadata) indexOf("Doc2").getMetadataForNewFile();
      assertThat(copied.dimensions).as("the copy must keep the dimensions of the original").isEqualTo(4);
      assertThat(copied.similarityFunction).isEqualTo(VectorSimilarityFunction.EUCLIDEAN);
    }

    /**
     * A user-supplied index name is unique across the schema and still belongs to the source type, so the copy takes the
     * auto-derived form rather than colliding with it.
     */
    @Test
    void copyTypeDoesNotReuseTheManualIndexName() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("Doc").createProperty("k", Type.STRING);
        database.command("sql", "CREATE INDEX myIndex ON Doc (k) UNIQUE");
      });

      database.getSchema().copyType("Doc", "Doc2", LocalDocumentType.class, 1, 65_536, 1_000);

      assertThat(database.getSchema().existsIndex("myIndex")).isTrue();
      assertThat(database.getSchema().getIndexByName("myIndex").getTypeName()).isEqualTo("Doc");
      assertThat(database.getSchema().getType("Doc2").getAllIndexes(false).iterator().next().getName()).isEqualTo("Doc2[k]");
    }

    /**
     * The conversion that motivates {@code copyType()} in the first place is document to vertex, and that goes through
     * the other branch of the record-copy loop ({@code database.newVertex(...)}). Every other case here copies to a
     * document type, so this one covers the branch the reported scenario actually takes.
     */
    @Test
    void copyTypeToAVertexTypeKeepsTheDefinitionAndTheRecords() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("Doc").createProperty("k", Type.STRING);
        database.getSchema().buildTypeIndex("Doc", new String[] { "k" })//
            .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).withPageSize(16_384).create();
      });
      database.transaction(() -> database.newDocument("Doc").set("k", "v1").save());

      database.getSchema().copyType("Doc", "Doc2", LocalVertexType.class, 1, 65_536, 1_000);

      assertThat(database.getSchema().getType("Doc2")).isInstanceOf(LocalVertexType.class);

      final IndexInternal copied = indexOf("Doc2");
      assertThat(copied.getPageSize()).isEqualTo(16_384);
      assertThat(copied.countEntries()).as("the vertex copy must be populated with the copied records").isEqualTo(1);

      try (final ResultSet rs = database.query("sql", "SELECT k FROM Doc2 WHERE k = 'v1'")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat(rs.next().<String>getProperty("k")).as("the vertex copy must carry the record content").isEqualTo("v1");
      }
    }

    /**
     * {@code LSMVectorIndexMetadata.copy()} carried only the collations before this change and now goes through
     * {@code copyCommonTo()}, which also carries the user-supplied index name. That reaches one caller outside
     * {@code copyType()} - {@code TRUNCATE TYPE}, which drops and recreates the index from its own definition - so a
     * manually named vector index now keeps its name across a truncate instead of reverting to the auto-derived form.
     * Asserted here rather than left to be inferred.
     */
    @Test
    void truncatingAManuallyNamedVectorIndexKeepsItsName() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("Doc").createProperty("embedding", Type.ARRAY_OF_FLOATS);
        database.command("sql", "CREATE INDEX myVectorIndex ON Doc (embedding) LSM_VECTOR"//
            + " METADATA {dimensions: 4, similarity: 'EUCLIDEAN'}");
      });
      database.transaction(() -> database.newDocument("Doc").set("embedding", new float[] { 1f, 2f, 3f, 4f }).save());

      assertThat(database.getSchema().existsIndex("myVectorIndex")).isTrue();

      // NOT inside a transaction: with one active TRUNCATE TYPE joins it and deletes record by record, which never
      // drops the index at all - so the carry-over this test is about would not run (issue #6220).
      database.command("sql", "TRUNCATE TYPE Doc UNSAFE").close();

      assertThat(database.getSchema().existsIndex("myVectorIndex")).as("the truncated index must keep its manual name")//
          .isTrue();
      final LSMVectorIndexMetadata metadata = (LSMVectorIndexMetadata) //
          ((IndexInternal) database.getSchema().getIndexByName("myVectorIndex")).getMetadataForNewFile();
      assertThat(metadata.dimensions).isEqualTo(4);
      assertThat(metadata.similarityFunction).isEqualTo(VectorSimilarityFunction.EUCLIDEAN);
    }

    private IndexInternal indexOf(final String typeName) {
      return (IndexInternal) database.getSchema().getType(typeName).getAllIndexes(false).iterator().next();
    }

    private IndexInternal subIndexOf(final String typeName) {
      return ((TypeIndex) indexOf(typeName)).getSubIndexes().getFirst();
    }

    private FullTextIndexMetadata fullTextMetadataOf(final String typeName) {
      return (FullTextIndexMetadata) ((LSMTreeFullTextIndex) subIndexOf(typeName)).getMetadataForNewFile();
    }
  }

  /**
   * Regression for #5742: an index rebuilt from its own definition lost the configuration the WRAPPER index
   * types - full-text, geospatial, sparse-vector - keep outside the underlying LSM-Tree, because every carry-over site
   * read it with {@code IndexInternal.getMetadata()}, which on those types answers with the underlying index's plain
   * {@link IndexMetadata}. Four sites did it: {@code TRUNCATE TYPE}, {@code REBUILD INDEX} / {@code CHECK DATABASE FIX}
   * (for the sparse-vector type, where it did not merely lose the settings but failed outright), {@code ALTER TYPE ...
   * BUCKET +...} and {@code ALTER TYPE ... SUPERTYPE +...}. Each is a distinct user-visible operation, so each gets its
   * own case here.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class IndexConfigurationCarryOver extends TestHelper {

    private static final String ENGLISH_ANALYZER = "org.apache.lucene.analysis.en.EnglishAnalyzer";

    @Test
    void truncateTypeKeepsTheFullTextConfiguration() {
      createFullTextFixture("Doc");
      database.transaction(() -> database.newDocument("Doc").set("text", "the quick brown foxes").save());

      database.command("sql", "TRUNCATE TYPE Doc UNSAFE").close();

      assertThat(fullTextMetadataOf("Doc").getAnalyzerClass()).isEqualTo(ENGLISH_ANALYZER);
      assertThat(fullTextMetadataOf("Doc").getBm25K1()).isEqualTo(1.5f);

      database.transaction(() -> database.newDocument("Doc").set("text", "the quick brown foxes").save());
      assertThat(indexOf("Doc").get(new Object[] { "fox" }).hasNext())
          .as("the English stem must still match after the truncate").isTrue();
    }

    @Test
    void truncateTypeKeepsTheGeospatialPrecision() {
      createGeoFixture("Doc", 7);
      database.transaction(() -> database.command("sql", "INSERT INTO Doc SET name = 'Rome', coords = 'POINT (12.5 41.9)'").close());

      database.command("sql", "TRUNCATE TYPE Doc UNSAFE").close();

      assertThat(((LSMTreeGeoIndex) subIndexOf("Doc")).getPrecision()).isEqualTo(7);
    }

    @Test
    void truncateTypeKeepsTheVectorConfiguration() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("Doc").createProperty("embedding", Type.ARRAY_OF_FLOATS);
        database.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {dimensions: 4, similarity: 'EUCLIDEAN'}").close();
      });
      database.transaction(() -> database.newDocument("Doc").set("embedding", new float[] { 1f, 2f, 3f, 4f }).save());

      database.command("sql", "TRUNCATE TYPE Doc UNSAFE").close();

      final LSMVectorIndexMetadata metadata = (LSMVectorIndexMetadata) indexOf("Doc").getMetadataForNewFile();
      assertThat(metadata.dimensions).isEqualTo(4);
    }

    @Test
    void addingABucketKeepsTheFullTextConfiguration() {
      createFullTextFixture("Doc");

      database.command("sql", "ALTER TYPE Doc BUCKET +Doc_extra").close();

      for (final IndexInternal sub : ((TypeIndex) indexOf("Doc")).getSubIndexes())
        assertThat(((FullTextIndexMetadata) sub.getMetadataForNewFile()).getAnalyzerClass())
            .as("every bucket sub-index must carry the analyzer").isEqualTo(ENGLISH_ANALYZER);
    }

    @Test
    void addingASuperTypeKeepsTheFullTextConfiguration() {
      createFullTextFixture("Super");
      database.transaction(() -> database.getSchema().createDocumentType("Sub").createProperty("text", Type.STRING));

      database.command("sql", "ALTER TYPE Sub SUPERTYPE +Super").close();

      final TypeIndex index = (TypeIndex) indexOf("Super");
      assertThat(index.getSubIndexes().size()).as("the index must have been propagated onto Sub's buckets")
          .isGreaterThan(1);
      for (final IndexInternal sub : index.getSubIndexes())
        assertThat(((FullTextIndexMetadata) sub.getMetadataForNewFile()).getAnalyzerClass())
            .as("every propagated sub-index must carry the analyzer").isEqualTo(ENGLISH_ANALYZER);
    }

    @Test
    void rebuildIndexKeepsTheSparseVectorConfiguration() {
      database.transaction(() -> {
        final DocumentType type = database.getSchema().createDocumentType("Doc");
        type.createProperty("dims", Type.ARRAY_OF_INTEGERS);
        type.createProperty("weights", Type.ARRAY_OF_FLOATS);
        database.command("sql", "CREATE INDEX ON Doc (dims, weights) LSM_SPARSE_VECTOR"
            + " METADATA {dimensions: 128, modifier: 'IDF', weightQuantization: 'FP16'}").close();
      });

      database.command("sql", "REBUILD INDEX `Doc[dims,weights]`").close();

      final LSMSparseVectorIndexMetadata metadata = (LSMSparseVectorIndexMetadata) indexOf("Doc").getMetadataForNewFile();
      assertThat(metadata.dimensions).isEqualTo(128);
      assertThat(metadata.modifier).isEqualTo(LSMSparseVectorIndexMetadata.MODIFIER_IDF);
      assertThat(metadata.weightQuantization).isEqualTo(SegmentFormat.WeightQuantization.FP16);
    }

    /**
     * The sparse-vector wrapper keeps its dimensionality, scoring modifier and weight quantization in a metadata of its
     * own, and its builder REFUSES a plain {@link IndexMetadata}: the shared rebuild path did not merely lose those
     * settings, it could not rebuild the index at all ("An LSM_SPARSE_VECTOR index requires
     * LSMSparseVectorIndexMetadata but got com.arcadedb.schema.IndexMetadata").
     */
    @Test
    void checkDatabaseFixKeepsTheSparseVectorConfiguration() {
      final AtomicReference<RID> victim = new AtomicReference<>();
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE Doc").close();
        database.command("sql", "CREATE PROPERTY Doc.dims ARRAY_OF_INTEGERS").close();
        database.command("sql", "CREATE PROPERTY Doc.weights ARRAY_OF_FLOATS").close();
        final Result inserted = database.command("sql", "INSERT INTO Doc SET dims = [1,5], weights = [0.5,0.25]").next();
        database.command("sql", "CREATE INDEX sparseDoc ON Doc (dims, weights) LSM_SPARSE_VECTOR"
            + " METADATA {dimensions: 128, modifier: 'IDF', weightQuantization: 'FP16'}").close();
        victim.set(inserted.toElement().getIdentity());
      });

      corruptRecordTypeByte((DatabaseInternal) database, victim.get());

      try (final ResultSet result = database.command("sql", "CHECK DATABASE FIX")) {
        assertThat(result.next().<Long>getProperty("autoFix")).isGreaterThan(0L);
      }

      assertThat(database.getSchema().existsIndex("sparseDoc")).as("the repaired index must keep its manual name").isTrue();
      final IndexMetadata repaired = ((IndexInternal) database.getSchema().getIndexByName("sparseDoc")).getMetadataForNewFile();
      assertThat(repaired).as("the repaired index must still be a sparse-vector index, not a plain one")
          .isInstanceOf(LSMSparseVectorIndexMetadata.class);
      final LSMSparseVectorIndexMetadata metadata = (LSMSparseVectorIndexMetadata) repaired;
      assertThat(metadata.dimensions).isEqualTo(128);
      assertThat(metadata.modifier).isEqualTo(LSMSparseVectorIndexMetadata.MODIFIER_IDF);
      assertThat(metadata.weightQuantization).isEqualTo(SegmentFormat.WeightQuantization.FP16);
    }

    /**
     * The two halves of a wrapper index's definition are joined again on schema RELOAD, and the bucket-level index JSON
     * the type-specific half is read from carries no {@code typeName} key - so the base-field read is skipped and the
     * collations and the user-supplied index name are only on the underlying definition. A carry-over site that reads
     * the type-specific half would then trade one silent loss for another: right analyzer, lost index name.
     */
    @Test
    void truncateAfterAReopenKeepsBothHalvesOfTheDefinition() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("Doc").createProperty("text", Type.STRING);
        database.command("sql", "CREATE INDEX myFullText ON Doc (text) FULL_TEXT"
            + " METADATA {\"analyzer\": \"" + ENGLISH_ANALYZER + "\"}").close();
      });
      database.transaction(() -> database.newDocument("Doc").set("text", "the quick brown foxes").save());

      reopenDatabase();

      database.command("sql", "TRUNCATE TYPE Doc UNSAFE").close();

      assertThat(database.getSchema().existsIndex("myFullText")).as("the truncated index must keep its manual name").isTrue();
      assertThat(((FullTextIndexMetadata) ((IndexInternal) database.getSchema().getIndexByName("myFullText"))
          .getMetadataForNewFile()).getAnalyzerClass())
          .as("and its analyzer").isEqualTo(ENGLISH_ANALYZER);
    }

    private void createFullTextFixture(final String typeName) {
      database.transaction(() -> {
        database.getSchema().createDocumentType(typeName).createProperty("text", Type.STRING);
        final TypeFullTextIndexBuilder builder = (TypeFullTextIndexBuilder) database.getSchema()//
            .buildTypeIndex(typeName, new String[] { "text" }).withType(Schema.INDEX_TYPE.FULL_TEXT);
        builder.withAnalyzer(ENGLISH_ANALYZER).withBM25(1.5f, 0.5f);
        builder.create();
      });
    }

    private void createGeoFixture(final String typeName, final int precision) {
      database.transaction(() -> {
        final DocumentType type = database.getSchema().createDocumentType(typeName);
        type.createProperty("name", Type.STRING);
        type.createProperty("coords", Type.STRING);
        final TypeIndexBuilder builder = database.getSchema().buildTypeIndex(typeName, new String[] { "coords" })//
            .withType(Schema.INDEX_TYPE.GEOSPATIAL);
        final GeoIndexMetadata metadata = new GeoIndexMetadata(typeName, new String[] { "coords" }, -1);
        metadata.setPrecision(precision);
        builder.withMetadata(metadata);
        builder.create();
      });
    }

    private IndexInternal indexOf(final String typeName) {
      return (IndexInternal) database.getSchema().getType(typeName).getAllIndexes(false).iterator().next();
    }

    private IndexInternal subIndexOf(final String typeName) {
      return ((TypeIndex) indexOf(typeName)).getSubIndexes().getFirst();
    }

    private FullTextIndexMetadata fullTextMetadataOf(final String typeName) {
      return (FullTextIndexMetadata) ((LSMTreeFullTextIndex) subIndexOf(typeName)).getMetadataForNewFile();
    }
  }
}
