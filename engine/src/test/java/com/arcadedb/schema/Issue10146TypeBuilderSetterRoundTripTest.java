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
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.PaginatedComponent;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10146: {@link TypeBuilder#toSQL()} is what a replica sends the leader in place of creating the type itself (#10067), so the
 * statement has to create the type the builder would. A census of the builder's field NAMES cannot see a setter that writes a field
 * without the flag that renders it ({@code withBuckets(emptyList)} wrote {@code buckets = 0} without {@code bucketsChosen}, so the
 * statement carried no {@code BUCKETS} clause and the other node gave the abstract type its default buckets). The census here is by
 * VALUE: every setter is applied with a non-default argument, the rendered statement is run, and the type it makes is compared with
 * the type the same builder makes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10146TypeBuilderSetterRoundTripTest extends TestHelper {

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
