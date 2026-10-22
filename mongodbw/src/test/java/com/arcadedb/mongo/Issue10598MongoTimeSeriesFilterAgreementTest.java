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
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10598: over a TIMESERIES type, {@code deleteMany({ts: <number>})} removed samples that {@code find} and {@code count} with
 * the identical filter reported as not matching. A sample is handed to a Mongo client with its timestamp as a BSON date, and the
 * read matcher (correctly, as MongoDB does) does not equate a BSON number with a date; the delete had a matcher of its own, written
 * for #10464, which read the number as epoch milliseconds. The usual safe workflow - preview with {@code find} or {@code count},
 * then delete the same filter - silently destroyed samples it had shown none of.
 * <p>
 * The rule: for one filter document, {@code find}, {@code count} and {@code deleteMany} over the same collection select the same
 * rows, or the delete is refused by name. The census below states it over every shape of filter the delete reads, so a shape one
 * half accepts and the other does not cannot arrive unnoticed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue10598MongoTimeSeriesFilterAgreementTest extends BaseMongoServerTest {
  private static final long T0 = 1792396800000L;
  private static final long T1 = 1792396860000L;
  private static final long T2 = 1792396920000L;

  private MongoClient               client;
  private MongoCollection<Document> events;
  private MongoCollection<Document> numbers;

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
    database.command("sql", "CREATE TIMESERIES TYPE Ev TIMESTAMP ts PRECISION MILLISECOND TAGS (k STRING) FIELDS (v DOUBLE)");
    database.command("sql",
        "CREATE TIMESERIES TYPE Num TIMESTAMP ts PRECISION MILLISECOND TAGS (n INTEGER, f DOUBLE, b BOOLEAN) FIELDS (v DOUBLE)");

    client = new MongoClient(new ServerAddress("localhost", getServerMongoPort()),
        MongoCredential.createPlainCredential("root", getDatabaseName(), DEFAULT_PASSWORD_FOR_TESTS.toCharArray()),
        MongoClientOptions.builder().serverSelectionTimeout(5000).build());
    events = client.getDatabase(getDatabaseName()).getCollection("Ev");
    numbers = client.getDatabase(getDatabaseName()).getCollection("Num");
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
  void aNumberOnTheTimestampIsRefusedWhereFindAndCountAnswerNothing() {
    refillEvents();

    // the three views of one filter: a number is not a date, so nothing is shown...
    final Document equality = new Document("ts", T0);
    assertThat(events.find(equality).into(new ArrayList<>())).isEmpty();
    assertThat(events.count(equality)).isZero();
    assertThat(events.countDocuments(equality)).isZero();
    // ...and so nothing may be removed: the delete is refused, and says what to compare with
    assertThatThrownBy(() -> events.deleteMany(equality)).isInstanceOf(MongoException.class)
        .hasMessageContaining("TIMESERIES type 'Ev' with the condition on 'ts'").hasMessageContaining("date");
    assertThat(events.countDocuments()).isEqualTo(3);

    final Document range = new Document("ts", new Document("$gte", T0).append("$lt", T2));
    assertThat(events.find(range).into(new ArrayList<>())).isEmpty();
    assertThat(events.count(range)).isZero();
    assertThatThrownBy(() -> events.deleteMany(range)).isInstanceOf(MongoException.class)
        .hasMessageContaining("TIMESERIES type 'Ev' with the condition on 'ts'");
    assertThat(events.countDocuments()).isEqualTo(3);
  }

  @Test
  void aDateOnTheTimestampIsShownAndRemovedAlike() {
    refillEvents();

    final Document equality = new Document("ts", new Date(T0));
    assertThat(events.find(equality).into(new ArrayList<>())).hasSize(1);
    assertThat(events.count(equality)).isEqualTo(1);
    assertThat(events.deleteMany(equality).getDeletedCount()).isEqualTo(1);

    refillEvents();

    final Document range = new Document("ts", new Document("$gte", new Date(T0)).append("$lt", new Date(T2)));
    assertThat(events.find(range).into(new ArrayList<>())).hasSize(2);
    assertThat(events.count(range)).isEqualTo(2);
    assertThat(events.deleteMany(range).getDeletedCount()).isEqualTo(2);
    assertThat(events.countDocuments()).isEqualTo(1);
  }

  @Test
  void aTimestampFilterOfAnyNumericKindIsRefused() {
    refillEvents();

    for (final Object number : new Object[] { 1, T0, (double) T0, T0 + 0.5, 1e30, Double.NaN }) {
      final Document filter = new Document("ts", new Document("$lte", number));
      assertThatThrownBy(() -> events.deleteMany(filter)).as("a %s", number.getClass().getSimpleName())
          .isInstanceOf(MongoException.class).hasMessageContaining("TIMESERIES type 'Ev' with the condition on 'ts'");
    }
    assertThat(events.countDocuments()).isEqualTo(3);
  }

  @Test
  void deleteManyRemovesExactlyWhatFindAndCountShowOverEveryShapeOfFilter() {
    final Map<String, Document> shapes = new LinkedHashMap<>();
    shapes.put("empty", new Document());
    shapes.put("ts = int32", new Document("ts", 1_000_000));
    shapes.put("ts = int64", new Document("ts", T0));
    shapes.put("ts = whole double", new Document("ts", (double) T0));
    shapes.put("ts = date", new Document("ts", new Date(T0)));
    shapes.put("ts = date in no sample", new Document("ts", new Date(T0 + 1)));
    shapes.put("ts $eq date", new Document("ts", new Document("$eq", new Date(T1))));
    shapes.put("ts $eq int64", new Document("ts", new Document("$eq", T1)));
    shapes.put("ts $gt date", new Document("ts", new Document("$gt", new Date(T0))));
    shapes.put("ts $gte date", new Document("ts", new Document("$gte", new Date(T1))));
    shapes.put("ts $lt date", new Document("ts", new Document("$lt", new Date(T1))));
    shapes.put("ts $lte date", new Document("ts", new Document("$lte", new Date(T1))));
    shapes.put("ts range int64", new Document("ts", new Document("$gte", T0).append("$lt", T2)));
    shapes.put("ts range date", new Document("ts", new Document("$gte", new Date(T0)).append("$lt", new Date(T2))));
    shapes.put("ts range int64 lower, date upper", new Document("ts", new Document("$gte", T0).append("$lt", new Date(T2))));
    shapes.put("ts range date lower, int64 upper", new Document("ts", new Document("$gte", new Date(T0)).append("$lt", T2)));
    shapes.put("ts string", new Document("ts", "2026-10-19T08:00:00Z"));
    shapes.put("ts $ne date", new Document("ts", new Document("$ne", new Date(T0))));
    shapes.put("ts $in dates", new Document("ts", new Document("$in", List.of(new Date(T0), new Date(T1)))));
    shapes.put("tag", new Document("k", "a"));
    shapes.put("tag $eq", new Document("k", new Document("$eq", "b")));
    shapes.put("tag absent", new Document("k", "z"));
    shapes.put("tag int", new Document("k", 1));
    shapes.put("tag + ts date", new Document("k", "a").append("ts", new Document("$gt", new Date(T0))));
    shapes.put("tag + ts int64", new Document("k", "a").append("ts", new Document("$gt", T0)));
    shapes.put("$and of tag and date range",
        new Document("$and", List.of(new Document("k", "a"), new Document("ts", new Document("$lt", new Date(T2))))));
    shapes.put("$and of tag and int64 range",
        new Document("$and", List.of(new Document("k", "a"), new Document("ts", new Document("$lt", T2)))));
    shapes.put("nested $and", new Document("$and", List.of(new Document("$and", List.of(new Document("k", "a"))),
        new Document("ts", new Document("$gt", new Date(T0))))));
    shapes.put("$and of empty", new Document("$and", List.of(new Document())));
    shapes.put("field", new Document("v", 1.0));

    int applied = 0;
    for (final Map.Entry<String, Document> shape : shapes.entrySet()) {
      applied += assertDeleteAgreesWithReads(events, shape.getKey(), shape.getValue(), this::refillEvents, 3);
    }
    // the census would be vacuous if the delete refused everything
    assertThat(applied).isGreaterThan(10);
  }

  @Test
  void deleteManyRemovesExactlyWhatFindAndCountShowOverTheTagKinds() {
    final Map<String, Document> shapes = new LinkedHashMap<>();
    for (final Object operand : new Object[] { 1, 1L, 1.0, 1.5, 4294967297L, 1e30, "1", true })
      shapes.put("n = " + operand + " (" + operand.getClass().getSimpleName() + ")", new Document("n", operand));
    for (final Object operand : new Object[] { 0.5, 0.5f, 1, 1L, "0.5", Double.NaN })
      shapes.put("f = " + operand + " (" + operand.getClass().getSimpleName() + ")", new Document("f", operand));
    for (final Object operand : new Object[] { true, false, 1, "true" })
      shapes.put("b = " + operand + " (" + operand.getClass().getSimpleName() + ")", new Document("b", operand));
    shapes.put("n $eq", new Document("n", new Document("$eq", 2)));
    shapes.put("n, f and ts date", new Document("n", 1).append("f", 0.5).append("ts", new Document("$lte", new Date(T0))));

    int applied = 0;
    for (final Map.Entry<String, Document> shape : shapes.entrySet())
      applied += assertDeleteAgreesWithReads(numbers, shape.getKey(), shape.getValue(), this::refillNumbers, 2);
    assertThat(applied).isGreaterThan(5);
  }

  /**
   * Whether the delete of {@code filter} removed what {@code find} and {@code count} showed, or was refused with nothing removed.
   *
   * @return 1 when the delete was applied, 0 when it was refused
   */
  private static int assertDeleteAgreesWithReads(final MongoCollection<Document> collection, final String label, final Document filter,
      final Runnable refill, final int samples) {
    refill.run();
    final long found = collection.find(filter).into(new ArrayList<>()).size();
    assertThat(collection.count(filter)).as("count of %s", label).isEqualTo(found);
    assertThat(collection.countDocuments(filter)).as("countDocuments of %s", label).isEqualTo(found);

    final long deleted;
    try {
      deleted = collection.deleteMany(filter).getDeletedCount();
    } catch (final MongoException refused) {
      assertThat(collection.countDocuments()).as("a refused delete of %s removes nothing", label).isEqualTo(samples);
      return 0;
    }
    assertThat(deleted).as("deleteMany of %s removes what find shows", label).isEqualTo(found);
    assertThat(collection.countDocuments()).as("what is left after %s", label).isEqualTo(samples - found);
    return 1;
  }

  private void refillEvents() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "DELETE FROM Ev");
    database.command("sqlscript", """
        INSERT INTO Ev SET ts = %d, k = 'a', v = 1.0;
        INSERT INTO Ev SET ts = %d, k = 'a', v = 2.0;
        INSERT INTO Ev SET ts = %d, k = 'b', v = 3.0;
        """.formatted(T0, T1, T2));
  }

  private void refillNumbers() {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.command("sql", "DELETE FROM Num");
    database.command("sqlscript", """
        INSERT INTO Num SET ts = %d, n = 1, f = 0.5, b = true, v = 1.0;
        INSERT INTO Num SET ts = %d, n = 2, f = 1.5, b = false, v = 2.0;
        """.formatted(T0, T1));
  }
}
