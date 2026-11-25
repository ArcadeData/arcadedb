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
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #12436: {@code CREATE ... TYPE X BUCKET <id>} read the name of the bucket before checking it was named by name, so a bucket
 * given by its id crashed the statement with a raw {@code NullPointerException} and the branch that looks the id up was never
 * reached. A bucket named by its id has no name to create: it has to exist already, and then it is the type's own.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12436CreateTypeBucketByIdTest extends TestHelper {

  @Test
  void anExistingBucketNamedByItsIdBecomesTheBucketOfTheType() {
    database.command("sql", "CREATE BUCKET Spare12436");
    final int bucketId = database.getSchema().getBucketByName("Spare12436").getFileId();

    database.command("sql", "CREATE DOCUMENT TYPE ById12436 BUCKET " + bucketId);

    final List<Bucket> buckets = database.getSchema().getType("ById12436").getBuckets(false);
    assertThat(buckets).hasSize(1);
    assertThat(buckets.getFirst().getName()).isEqualTo("Spare12436");
    assertThat(database.getSchema().getTypeByBucketName("Spare12436").getName()).isEqualTo("ById12436");

    database.transaction(() -> database.command("sql", "INSERT INTO ById12436 SET x = 1"));

    try (final ResultSet resultSet = database.query("sql", "SELECT count(@rid) AS n FROM bucket:Spare12436")) {
      assertThat(resultSet.next().<Long>getProperty("n")).as("the record landed in the bucket given by its id").isEqualTo(1L);
    }
  }

  @Test
  void aVertexTypeTakesABucketNamedByItsIdToo() {
    database.command("sql", "CREATE BUCKET SpareVertices12436");
    final int bucketId = database.getSchema().getBucketByName("SpareVertices12436").getFileId();

    database.command("sql", "CREATE VERTEX TYPE VertexById12436 BUCKET " + bucketId);

    assertThat(database.getSchema().getType("VertexById12436").getBuckets(false)).extracting(Bucket::getName)
        .containsExactly("SpareVertices12436");
  }

  @Test
  void aBucketNamedByAnIdNobodyHoldsIsASchemaErrorAndCreatesNothing() {
    final Throwable refused = catchThrowable(() -> database.command("sql", "CREATE DOCUMENT TYPE Unknown12436 BUCKET 999999"));

    assertThat(refused).isInstanceOf(SchemaException.class).hasMessageContaining("999999");
    assertThat(database.getSchema().existsType("Unknown12436")).isFalse();
  }

  @Test
  void aBucketAnotherTypeOwnsIsRefusedAndTheOwnerKeepsIt() {
    database.command("sql", "CREATE DOCUMENT TYPE Owner12436");
    final Bucket owned = database.getSchema().getType("Owner12436").getBuckets(false).getFirst();

    final Throwable refused = catchThrowable(() -> database.command("sql", "CREATE DOCUMENT TYPE Thief12436 BUCKET " + owned.getFileId()));

    assertThat(refused).isInstanceOf(SchemaException.class).hasMessageContaining("already associated to the type 'Owner12436'");
    assertThat(database.getSchema().existsType("Thief12436")).isFalse();
    assertThat(database.getSchema().getTypeByBucketName(owned.getName()).getName()).isEqualTo("Owner12436");
  }
}
