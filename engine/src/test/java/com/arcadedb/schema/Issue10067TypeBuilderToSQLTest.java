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
import com.arcadedb.engine.PaginatedComponent;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10067: a replica that has to ask the leader to create a type renders the builder's description as the {@code CREATE ... TYPE}
 * statement the leader runs. The rendering has to describe the same type the builder would create, so every statement here is also
 * run, and the type it makes is compared with the one the builder makes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10067TypeBuilderToSQLTest extends TestHelper {

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
