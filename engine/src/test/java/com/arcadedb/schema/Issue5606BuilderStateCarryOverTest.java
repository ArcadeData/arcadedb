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
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.serializer.json.JSONObject;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #5606: {@link TypeIndexBuilder#withType} hands back a SPECIALISED builder for the index types that carry
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
class Issue5606BuilderStateCarryOverTest extends TestHelper {
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
   * from a pristine builder's: both are structural and final, handed to the specialised builder from the source.
   */
  private static final Set<String> NOT_CONFIGURABLE = Set.of("database", "indexImplementation");

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
