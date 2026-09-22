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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.engine.Bucket;
import com.arcadedb.schema.Schema;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7871: DROP TYPE, DROP BUCKET and DROP PROPERTY with IF EXISTS returned zero rows on the
 * no-op path instead of a {@code dropped: false} row like the other four DROP statements, and
 * CREATE MATERIALIZED VIEW / CREATE CONTINUOUS AGGREGATE with IF NOT EXISTS never set the
 * {@code created} flag at all.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7871DropIfExistsResultShapeTest extends TestHelper {

  @Test
  void dropTypeIfExistsReportsDroppedFlagBothWays() {
    final Schema schema = database.getSchema();
    schema.createDocumentType("Issue7871Type");

    ResultSet result = database.command("sql", "drop type Issue7871Type if exists");
    assertThat(result.hasNext()).isTrue();
    Result next = result.next();
    assertThat(next.<String>getProperty("operation")).isEqualTo("drop type");
    assertThat(next.<Boolean>getProperty("dropped")).isTrue();
    result.close();

    // second call: type no longer exists, IF EXISTS must still return one row, dropped=false
    result = database.command("sql", "drop type Issue7871Type if exists");
    assertThat(result.hasNext()).isTrue();
    next = result.next();
    assertThat(next.<String>getProperty("operation")).isEqualTo("drop type");
    assertThat(next.<String>getProperty("typeName")).isEqualTo("Issue7871Type");
    assertThat(next.<Boolean>getProperty("dropped")).isFalse();
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void dropBucketIfExistsByNameReportsDroppedFlagBothWays() {
    final Schema schema = database.getSchema();
    schema.createBucket("Issue7871Bucket");

    ResultSet result = database.command("sql", "drop bucket Issue7871Bucket if exists");
    Result next = result.next();
    assertThat(next.<Boolean>getProperty("dropped")).isTrue();
    result.close();

    result = database.command("sql", "drop bucket Issue7871Bucket if exists");
    assertThat(result.hasNext()).isTrue();
    next = result.next();
    assertThat(next.<String>getProperty("operation")).isEqualTo("drop bucket");
    assertThat(next.<String>getProperty("bucketName")).isEqualTo("Issue7871Bucket");
    assertThat(next.<Boolean>getProperty("dropped")).isFalse();
    assertThat(result.hasNext()).isFalse();
    result.close();

    assertThat(schema.existsBucket("Issue7871Bucket")).isFalse();
  }

  @Test
  void dropBucketIfExistsByIdOnMissingIdReturnsRowNotEmptySet() {
    final Bucket bucket = database.getSchema().createBucket("Issue7871BucketById");
    final int bucketId = bucket.getFileId();
    database.getSchema().dropBucket("Issue7871BucketById");

    final ResultSet result = database.command("sql", "drop bucket " + bucketId + " if exists");
    assertThat(result.hasNext()).isTrue();
    final Result next = result.next();
    assertThat(next.<String>getProperty("operation")).isEqualTo("drop bucket");
    assertThat(next.<Boolean>getProperty("dropped")).isFalse();
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void dropPropertyIfExistsReportsDroppedFlagBothWays() {
    final Schema schema = database.getSchema();
    schema.createDocumentType("Issue7871PropType").createProperty("name", String.class);

    ResultSet result = database.command("sql", "drop property Issue7871PropType.name if exists");
    Result next = result.next();
    assertThat(next.<Boolean>getProperty("dropped")).isTrue();
    result.close();

    result = database.command("sql", "drop property Issue7871PropType.name if exists");
    assertThat(result.hasNext()).isTrue();
    next = result.next();
    assertThat(next.<String>getProperty("operation")).isEqualTo("drop property");
    assertThat(next.<String>getProperty("typeName")).isEqualTo("Issue7871PropType");
    assertThat(next.<String>getProperty("propertyName")).isEqualTo("name");
    assertThat(next.<Boolean>getProperty("dropped")).isFalse();
    assertThat(result.hasNext()).isFalse();
    result.close();

    schema.dropType("Issue7871PropType");
  }

  @Test
  void createMaterializedViewIfNotExistsReportsCreatedFlagBothWays() {
    database.getSchema().createDocumentType("Issue7871MVSource");

    ResultSet result = database.command("sql",
        "create materialized view if not exists Issue7871MV as select from Issue7871MVSource");
    Result next = result.next();
    assertThat(next.<Boolean>getProperty("created")).isTrue();
    result.close();

    result = database.command("sql",
        "create materialized view if not exists Issue7871MV as select from Issue7871MVSource");
    assertThat(result.hasNext()).isTrue();
    next = result.next();
    assertThat(next.<String>getProperty("operation")).isEqualTo("create materialized view");
    assertThat(next.<Boolean>getProperty("created")).isFalse();
    result.close();

    database.getSchema().dropMaterializedView("Issue7871MV");
    database.getSchema().dropType("Issue7871MVSource");
  }

  @Test
  void createContinuousAggregateIfNotExistsReportsCreatedFlagBothWays() {
    database.command("sql",
        "create timeseries type Issue7871CASource timestamp ts tags (sensor_id string) fields (val double)");

    ResultSet result = database.command("sql",
        "create continuous aggregate if not exists Issue7871CA as "
            + "select sensor_id, ts.timeBucket('1h', ts) as hour, avg(val) as avgVal "
            + "from Issue7871CASource group by sensor_id, hour");
    Result next = result.next();
    assertThat(next.<Boolean>getProperty("created")).isTrue();
    result.close();

    result = database.command("sql",
        "create continuous aggregate if not exists Issue7871CA as "
            + "select sensor_id, ts.timeBucket('1h', ts) as hour, avg(val) as avgVal "
            + "from Issue7871CASource group by sensor_id, hour");
    assertThat(result.hasNext()).isTrue();
    next = result.next();
    assertThat(next.<String>getProperty("operation")).isEqualTo("create continuous aggregate");
    assertThat(next.<Boolean>getProperty("created")).isFalse();
    result.close();

    database.getSchema().dropContinuousAggregate("Issue7871CA");
    database.getSchema().dropType("Issue7871CASource");
  }
}
