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
package com.arcadedb.mongo;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10464 (follow-up of #10442): a filtered delete or update over a TIMESERIES type selected its targets by record identity,
 * and a sample has none, so it reported {@code n: 0} while a count of the same filter answered the matching samples. A delete is
 * now handed to the engine of the type by time range and tags, as SQL does, and anything a time series cannot do exactly - an
 * update, an upsert, a delete of one sample, a filter on a FIELD column - is refused with an error naming the type.
 */
public class MongoDBTimeSeriesWriteIssue10464Test extends BaseMongoServerTest {
  private static final long T0 = 1792396800000L;
  private static final long T1 = 1792396860000L;
  private static final long T2 = 1792396920000L;

  private MongoClient               client;
  private MongoCollection<Document> collection;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("MongoDB:com.arcadedb.mongo.MongoDBProtocolPlugin");
  }

  @BeforeEach
  @Override
  public void beginTest() {
    super.beginTest();
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sqlscript", """
        CREATE TIMESERIES TYPE Ev TIMESTAMP ts PRECISION MILLISECOND TAGS (k STRING) FIELDS (v DOUBLE);
        INSERT INTO Ev SET ts = %d, k = 'a', v = 1.0;
        INSERT INTO Ev SET ts = %d, k = 'a', v = 2.0;
        INSERT INTO Ev SET ts = %d, k = 'b', v = 3.0;
        """.formatted(T0, T1, T2));

    client = new MongoClient(new ServerAddress("localhost", getServerMongoPort()),
        MongoCredential.createPlainCredential("root", getDatabaseName(), DEFAULT_PASSWORD_FOR_TESTS.toCharArray()),
        MongoClientOptions.builder().serverSelectionTimeout(5000).build());
    final MongoDatabase mongoDatabase = client.getDatabase(getDatabaseName());
    collection = mongoDatabase.getCollection("Ev");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    if (client != null)
      client.close();
    super.endTest();
  }

  @Test
  void deleteManyByTagRemovesTheMatchingSamples() {
    assertThat(collection.deleteMany(Filters.eq("k", "a")).getDeletedCount()).isEqualTo(2);
    assertThat(collection.countDocuments()).isEqualTo(1);
    assertThat(values()).containsExactly(3.0);
  }

  @Test
  void deleteManyByExplicitEqOnATagRemovesTheMatchingSamples() {
    assertThat(collection.deleteMany(new Document("k", new Document("$eq", "b"))).getDeletedCount()).isEqualTo(1);
    assertThat(values()).containsExactlyInAnyOrder(1.0, 2.0);
  }

  @Test
  void deleteManyByTimeRangeRemovesTheSamplesInTheRange() {
    assertThat(collection.deleteMany(Filters.lt("ts", T1)).getDeletedCount()).isEqualTo(1);
    assertThat(values()).containsExactlyInAnyOrder(2.0, 3.0);

    assertThat(collection.deleteMany(Filters.and(Filters.gte("ts", T1), Filters.lte("ts", T1))).getDeletedCount()).isEqualTo(1);
    assertThat(values()).containsExactly(3.0);
  }

  @Test
  void deleteManyByTimeRangeAndTagRemovesOnlyTheirIntersection() {
    assertThat(collection.deleteMany(Filters.and(Filters.gt("ts", T0), Filters.eq("k", "a"))).getDeletedCount()).isEqualTo(1);
    assertThat(values()).containsExactlyInAnyOrder(1.0, 3.0);
  }

  @Test
  void deleteManyByDateRemovesTheSamplesBeforeIt() {
    assertThat(collection.deleteMany(Filters.lt("ts", new Date(T2))).getDeletedCount()).isEqualTo(2);
    assertThat(values()).containsExactly(3.0);
  }

  @Test
  void deleteManyWithAFilterMatchingNothingRemovesNothing() {
    assertThat(collection.deleteMany(Filters.eq("k", "z")).getDeletedCount()).isEqualTo(0);
    assertThat(collection.countDocuments()).isEqualTo(3);
    // a collection that does not exist still answers n: 0, as MongoDB does
    assertThat(client.getDatabase(getDatabaseName()).getCollection("Missing").deleteMany(Filters.eq("k", "a")).getDeletedCount())
        .isEqualTo(0);
  }

  @Test
  void unfilteredDeleteManyRemovesEverySample() {
    assertThat(collection.deleteMany(new Document()).getDeletedCount()).isEqualTo(3);
    assertThat(collection.countDocuments()).isEqualTo(0);
  }

  @Test
  void deleteManyWithAnAndOfEmptyFiltersRemovesEverySample() {
    // like MongoDB, {$and: [{}]} matches every document
    assertThat(collection.deleteMany(new Document("$and", List.of(new Document()))).getDeletedCount()).isEqualTo(3);
    assertThat(collection.countDocuments()).isEqualTo(0);
  }

  @Test
  void deleteOneIsRefusedNamingTheType() {
    assertThatThrownBy(() -> collection.deleteOne(Filters.eq("k", "a"))).isInstanceOf(MongoException.class)
        .hasMessageContaining("Cannot delete one sample of the TIMESERIES type 'Ev'");
    assertThatThrownBy(() -> collection.deleteOne(new Document())).isInstanceOf(MongoException.class)
        .hasMessageContaining("Cannot delete one sample of the TIMESERIES type 'Ev'");
    assertThat(collection.countDocuments()).isEqualTo(3);
  }

  @Test
  void deleteManyOnAFieldColumnIsRefusedNamingTheType() {
    assertThatThrownBy(() -> collection.deleteMany(Filters.eq("v", 1.0))).isInstanceOf(MongoException.class)
        .hasMessageContaining("TIMESERIES type 'Ev' with the condition on 'v'");
    assertThat(collection.countDocuments()).isEqualTo(3);
  }

  @Test
  void deleteManyWithAnInexactConditionIsRefusedNamingTheType() {
    // none of these can be handed to the engine by time range and tag values: a sample they did not name could be removed
    assertThatThrownBy(() -> collection.deleteMany(Filters.ne("k", "a"))).isInstanceOf(MongoException.class)
        .hasMessageContaining("TIMESERIES type 'Ev' with the condition on 'k'");
    assertThatThrownBy(() -> collection.deleteMany(Filters.or(Filters.eq("k", "a"), Filters.eq("k", "b"))))
        .isInstanceOf(MongoException.class).hasMessageContaining("TIMESERIES type 'Ev' with the condition on '$or'");
    assertThatThrownBy(() -> collection.deleteMany(Filters.in("k", "a", "b"))).isInstanceOf(MongoException.class)
        .hasMessageContaining("TIMESERIES type 'Ev' with the condition on 'k'");
    assertThatThrownBy(() -> collection.deleteMany(Filters.eq("missing", 1))).isInstanceOf(MongoException.class)
        .hasMessageContaining("TIMESERIES type 'Ev' with the condition on 'missing'");
    // a tag compared with a value of another kind: SQL would coerce it, MongoDB would match nothing
    assertThatThrownBy(() -> collection.deleteMany(Filters.eq("k", 1))).isInstanceOf(MongoException.class)
        .hasMessageContaining("TIMESERIES type 'Ev' with the condition on 'k'");
    assertThatThrownBy(() -> collection.deleteMany(Filters.lt("ts", "2026-01-01"))).isInstanceOf(MongoException.class)
        .hasMessageContaining("TIMESERIES type 'Ev' with the condition on 'ts'");
    assertThat(collection.countDocuments()).isEqualTo(3);
  }

  @Test
  void aRefusedEntryOfABulkDeleteLeavesTheOtherEntriesUnapplied() {
    // one delete command with two entries: the engine's delete is not transactional, so the second entry's refusal must come
    // before the first entry runs
    assertThatThrownBy(() -> collection.bulkWrite(List.of(new DeleteManyModel<>(Filters.eq("k", "a")), new DeleteManyModel<>(Filters.eq("v", 3.0)))))
        .isInstanceOf(MongoException.class).hasMessageContaining("TIMESERIES type 'Ev' with the condition on 'v'");
    assertThat(collection.countDocuments()).isEqualTo(3);

    // two entries both accepted are both applied
    assertThat(collection.bulkWrite(List.of(new DeleteManyModel<>(Filters.eq("k", "b")), new DeleteManyModel<>(Filters.lt("ts", T1))))
        .getDeletedCount()).isEqualTo(2);
    assertThat(values()).containsExactly(2.0);
  }

  @Test
  void deleteManyAcceptsNestedAndAndBothBoundsInOneOperatorDocument() {
    assertThat(collection.deleteMany(new Document("$and", List.of(new Document("$and", List.of(new Document("k", "a"))),
        new Document("ts", new Document("$gt", T0).append("$lt", T2))))).getDeletedCount()).isEqualTo(1);
    assertThat(values()).containsExactlyInAnyOrder(1.0, 3.0);
  }

  @Test
  void deleteManyAcceptsAWholeDoubleTimestamp() {
    assertThat(collection.deleteMany(Filters.lte("ts", (double) T1)).getDeletedCount()).isEqualTo(2);
    assertThat(values()).containsExactly(3.0);
  }

  @Test
  void deleteManyRefusesATimestampSqlWouldNarrow() {
    assertThatThrownBy(() -> collection.deleteMany(Filters.lt("ts", T1 + 0.5))).isInstanceOf(MongoException.class)
        .hasMessageContaining("TIMESERIES type 'Ev' with the condition on 'ts'");
    assertThatThrownBy(() -> collection.deleteMany(Filters.lt("ts", 1e30))).isInstanceOf(MongoException.class)
        .hasMessageContaining("TIMESERIES type 'Ev' with the condition on 'ts'");
    // whole, inside a long, but past 2^53: the double may already be a neighbour of the millisecond the client meant
    assertThatThrownBy(() -> collection.deleteMany(Filters.lt("ts", 1e17))).isInstanceOf(MongoException.class)
        .hasMessageContaining("TIMESERIES type 'Ev' with the condition on 'ts'");
    assertThatThrownBy(() -> collection.deleteMany(new Document("$and", List.of()))).isInstanceOf(MongoException.class)
        .hasMessageContaining("TIMESERIES type 'Ev' with the condition on '$and'");
    assertThat(collection.countDocuments()).isEqualTo(3);
  }

  @Test
  void deleteManyRefusesANumericTagValueSqlWouldCoerce() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sqlscript", """
        CREATE TIMESERIES TYPE Num TIMESTAMP ts PRECISION MILLISECOND TAGS (n INTEGER, f DOUBLE) FIELDS (v DOUBLE);
        INSERT INTO Num SET ts = %d, n = 1, f = 0.5, v = 1.0;
        INSERT INTO Num SET ts = %d, n = 2, f = 1.5, v = 2.0;
        """.formatted(T0, T1));
    final MongoCollection<Document> num = client.getDatabase(getDatabaseName()).getCollection("Num");

    // out of the range of the INTEGER tag, or not whole: SQL would narrow it to another series
    assertThatThrownBy(() -> num.deleteMany(Filters.eq("n", 1e30))).isInstanceOf(MongoException.class)
        .hasMessageContaining("TIMESERIES type 'Num' with the condition on 'n'");
    assertThatThrownBy(() -> num.deleteMany(Filters.eq("n", 4294967297L))).isInstanceOf(MongoException.class)
        .hasMessageContaining("TIMESERIES type 'Num' with the condition on 'n'");
    assertThatThrownBy(() -> num.deleteMany(Filters.eq("n", 1.5))).isInstanceOf(MongoException.class)
        .hasMessageContaining("TIMESERIES type 'Num' with the condition on 'n'");
    // a NaN never equals itself in MongoDB
    assertThatThrownBy(() -> num.deleteMany(Filters.eq("f", Double.NaN))).isInstanceOf(MongoException.class)
        .hasMessageContaining("TIMESERIES type 'Num' with the condition on 'f'");
    assertThat(num.countDocuments()).isEqualTo(2);

    // a value of the tag's range, whole or as a whole double, is applied
    assertThat(num.deleteMany(Filters.eq("n", 1.0)).getDeletedCount()).isEqualTo(1);
    assertThat(num.deleteMany(Filters.eq("f", 1.5)).getDeletedCount()).isEqualTo(1);
    assertThat(num.countDocuments()).isEqualTo(0);
  }

  @Test
  void filteredUpdateIsRefusedNamingTheType() {
    assertThatThrownBy(() -> collection.updateMany(Filters.eq("k", "a"), Updates.set("v", 9.0))).isInstanceOf(MongoException.class)
        .hasMessageContaining("Cannot update the samples of the TIMESERIES type 'Ev'");
    assertThatThrownBy(() -> collection.updateOne(Filters.eq("k", "a"), Updates.set("v", 9.0))).isInstanceOf(MongoException.class)
        .hasMessageContaining("Cannot update the samples of the TIMESERIES type 'Ev'");
    assertThat(values()).containsExactlyInAnyOrder(1.0, 2.0, 3.0);
  }

  @Test
  void unfilteredUpdateIsRefusedNamingTheType() {
    assertThatThrownBy(() -> collection.updateMany(new Document(), Updates.set("v", 9.0))).isInstanceOf(MongoException.class)
        .hasMessageContaining("Cannot update the samples of the TIMESERIES type 'Ev'");
    assertThat(values()).containsExactlyInAnyOrder(1.0, 2.0, 3.0);
  }

  @Test
  void replaceIsRefusedNamingTheType() {
    assertThatThrownBy(() -> collection.replaceOne(Filters.eq("k", "b"), new Document("ts", T2).append("k", "b").append("v", 9.0)))
        .isInstanceOf(MongoException.class)
        .hasMessageContaining("Cannot update the samples of the TIMESERIES type 'Ev'");
    assertThat(values()).containsExactlyInAnyOrder(1.0, 2.0, 3.0);
  }

  @Test
  void upsertIsRefusedNamingTheTypeAndLeavesTheSchemaAlone() {
    assertThatThrownBy(
        () -> collection.updateOne(Filters.eq("k", "z"), Updates.set("v", 9.0), new UpdateOptions().upsert(true)))
        .isInstanceOf(MongoException.class)
        .hasMessageContaining("Cannot update the samples of the TIMESERIES type 'Ev'");
    assertThat(collection.countDocuments()).isEqualTo(3);
    // the upsert never got as far as preparing an _id index on the type
    assertThat(getServerDatabase(0, getDatabaseName()).getSchema().getType("Ev").getAllIndexes(false)).isEmpty();
  }

  private List<Double> values() {
    final List<Double> values = new ArrayList<>();
    for (final Document d : collection.find())
      values.add(((Number) d.get("v")).doubleValue());
    return values;
  }
}
