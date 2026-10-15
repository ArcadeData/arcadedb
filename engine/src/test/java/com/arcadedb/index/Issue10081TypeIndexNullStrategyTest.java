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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.index.IndexInternal.INDEX_STATUS;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract.NULL_STRATEGY;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #10081: {@link TypeIndex#setNullStrategy} applied the strategy to the first bucket's index only,
 * while its getter read that same first index. The change read back as applied, but the other buckets kept enforcing the old
 * strategy, so the same null-key insert was refused or accepted depending on the bucket the record happened to land in.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10081TypeIndexNullStrategyTest extends TestHelper {
  private static final String TYPE_NAME = "V10081";
  private static final int    BUCKETS   = 4;

  @Test
  void setNullStrategyReachesEveryBucketIndex() {
    final TypeIndex typeIndex = createTypeIndex();
    assertThat(typeIndex.getIndexesOnBuckets()).hasSize(BUCKETS);
    assertThat(strategies(typeIndex)).containsOnly(NULL_STRATEGY.SKIP);

    typeIndex.setNullStrategy(NULL_STRATEGY.ERROR);

    assertThat(typeIndex.getNullStrategy()).isEqualTo(NULL_STRATEGY.ERROR);
    assertThat(strategies(typeIndex)).as("every bucket index must enforce the strategy the facade reports").hasSize(BUCKETS)
        .containsOnly(NULL_STRATEGY.ERROR);
  }

  @Test
  void nullKeyIsRefusedOnEveryBucketAfterTheChange() {
    final TypeIndex typeIndex = createTypeIndex();
    final DocumentType type = database.getSchema().getType(TYPE_NAME);
    final List<String> bucketNames = new ArrayList<>();
    type.getBuckets(false).forEach(bucket -> bucketNames.add(bucket.getName()));
    assertThat(bucketNames).hasSize(BUCKETS);

    typeIndex.setNullStrategy(NULL_STRATEGY.ERROR);

    // the landing bucket is chosen by the bucket selection strategy: the outcome must not depend on it
    for (final String bucketName : bucketNames)
      assertThatThrownBy(() -> database.transaction(() -> database.newDocument(TYPE_NAME).save(bucketName)))
          .as("a null key must be refused on bucket %s", bucketName).rootCause().hasMessageContaining("cannot be NULL");

    assertThat(database.countType(TYPE_NAME, true)).isZero();
  }

  @Test
  void newStrategyIsKeptAfterReopen() {
    final TypeIndex typeIndex = createTypeIndex();
    typeIndex.setNullStrategy(NULL_STRATEGY.INDEX);
    // the strategy is persisted with the schema: force a save, as any later schema change would
    database.getSchema().getType(TYPE_NAME).createProperty("other", Type.STRING);

    reopenDatabase();

    final TypeIndex reloaded = (TypeIndex) database.getSchema().getIndexByName(TYPE_NAME + "[k]");
    assertThat(reloaded.getNullStrategy()).isEqualTo(NULL_STRATEGY.INDEX);
    assertThat(strategies(reloaded)).hasSize(BUCKETS).containsOnly(NULL_STRATEGY.INDEX);
  }

  /**
   * Every write on {@link TypeIndex} must either fan out over all the bucket indexes or refuse: a setter that borrows the
   * single-index read helper changes one bucket and reads back as applied. A new {@code setXxx} on the {@link Index} or
   * {@link IndexInternal} interfaces fails here until it is classified.
   */
  @Test
  void everySetterAppliesToEveryBucketIndexOrRefuses() throws Exception {
    final TypeIndex typeIndex = createTypeIndex();

    final Set<Method> setters = new LinkedHashSet<>();
    for (final Class<?> contract : new Class<?>[] { Index.class, IndexInternal.class })
      for (final Method method : contract.getMethods())
        if (method.getName().startsWith("set"))
          setters.add(TypeIndex.class.getMethod(method.getName(), method.getParameterTypes()));

    assertThat(setters).as("the Index and IndexInternal setters").isNotEmpty();

    for (final Method setter : setters) {
      switch (setter.getName() + "/" + setter.getParameterCount()) {
      case "setNullStrategy/1" -> {
        typeIndex.setNullStrategy(NULL_STRATEGY.INDEX);
        assertThat(strategies(typeIndex)).as(setter.toString()).hasSize(BUCKETS).containsOnly(NULL_STRATEGY.INDEX);
      }
      case "setMetadata/1", "setTypeIndex/1" -> assertRefuses(setter, typeIndex);
      case "setStatus/2" -> {
        // a facade has no status of its own: it must not report a change that no bucket index went through
        assertThat(typeIndex.setStatus(new INDEX_STATUS[] { INDEX_STATUS.AVAILABLE }, INDEX_STATUS.UNAVAILABLE))
            .as(setter.toString()).isFalse();
      }
      default -> throw new AssertionError("Unclassified setter " + setter + ": TypeIndex must apply it to every bucket index "
          + "(loop over indexesOnBuckets) or refuse it, never delegate it to getFirstUnderlyingIndex(). Add it to this test.");
      }
    }
  }

  private void assertRefuses(final Method setter, final TypeIndex typeIndex) {
    final Object[] args = new Object[setter.getParameterCount()];
    assertThatThrownBy(() -> {
      try {
        setter.invoke(typeIndex, args);
      } catch (final InvocationTargetException e) {
        throw e.getCause();
      }
    }).as(setter.toString()).isInstanceOf(UnsupportedOperationException.class);
  }

  private TypeIndex createTypeIndex() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(BUCKETS).create();
      type.createProperty("k", Type.INTEGER);
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "k" }).withType(Schema.INDEX_TYPE.LSM_TREE)
          .withUnique(false).withNullStrategy(NULL_STRATEGY.SKIP).create();
    });
    return (TypeIndex) database.getSchema().getIndexByName(TYPE_NAME + "[k]");
  }

  private static NULL_STRATEGY[] strategies(final TypeIndex typeIndex) {
    final IndexInternal[] onBuckets = typeIndex.getIndexesOnBuckets();
    final NULL_STRATEGY[] result = new NULL_STRATEGY[onBuckets.length];
    for (int i = 0; i < onBuckets.length; i++)
      result[i] = onBuckets[i].getNullStrategy();
    return result;
  }
}
