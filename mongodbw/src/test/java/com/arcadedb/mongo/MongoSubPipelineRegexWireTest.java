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
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A regular expression in the {@code $match} of an aggregation sub-pipeline ({@code $facet}, {@code $lookup}) is a pattern bounded by
 * {@code arcadedb.command.regexTimeout} when it is spelled as one, and a {@code $regex} that sits anywhere else is refused before the
 * pipeline runs, where the {@code $match} stages of the top level compare it as a value, as they always did.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class MongoSubPipelineRegexWireTest extends BaseMongoServerTest {
  /** An expression that backtracks for a long time on a long run of "a" that does not end the text. */
  private static final String SLOW = "(.*a){20}$";

  private MongoClient client;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("MongoDB:com.arcadedb.mongo.MongoDBProtocolPlugin");
  }

  @BeforeEach
  @Override
  public void beginTest() {
    super.beginTest();
    getDatabase(0);
    client = new MongoClient(new ServerAddress("localhost", getServerMongoPort()),
        MongoCredential.createPlainCredential("root", getDatabaseName(), DEFAULT_PASSWORD_FOR_TESTS.toCharArray()),
        MongoClientOptions.builder().serverSelectionTimeout(5000).build());
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    if (client != null)
      client.close();
    super.endTest();
  }

  /**
   * @param run how many "a" the first document holds before its "!": 24 keeps {@link #SLOW} to well under a second where nothing bounds
   *            it, 40 is out of reach of any machine
   */
  private MongoCollection<Document> collection(final int run) {
    final MongoCollection<Document> collection = client.getDatabase(getDatabaseName()).getCollection("subjects");
    collection.insertMany(List.of(new Document("_id", 1).append("s", "a".repeat(run) + "!"), new Document("_id", 2).append("s", "alpha")));
    client.getDatabase(getDatabaseName()).getCollection("others")
        .insertMany(List.of(new Document("_id", 1).append("s", "a".repeat(run) + "!"), new Document("_id", 2).append("s", "alpha")));
    return collection;
  }

  private static Document of(final Object... keyValues) {
    final Document document = new Document();
    for (int i = 0; i < keyValues.length; i += 2)
      document.put((String) keyValues[i], keyValues[i + 1]);
    return document;
  }

  private static List<Document> facet(final Document query) {
    return List.of(of("$facet", of("f", List.of(of("$match", query)))));
  }

  private static List<Document> lookup(final Document query) {
    return List.of(of("$lookup", of("from", "others", "as", "o", "pipeline", List.of(of("$match", query)))));
  }

  private static List<Document> run(final MongoCollection<Document> collection, final List<Document> pipeline) {
    return collection.aggregate(pipeline).into(new ArrayList<>());
  }

  @Test
  @Timeout(120)
  void aRegexThatIsNotBoundedIsRefusedInASubPipeline() {
    final MongoCollection<Document> c = collection(24);
    for (final Document shape : List.of(
        of("s", of("x", 1, "$regex", SLOW)),
        of("s", of("$not", of("x", 1, "$regex", SLOW))),
        of("s", of("$in", List.of(of("$regex", SLOW)))),
        of("s", of("$all", List.of(of("$regex", SLOW)))),
        of("s", of("$elemMatch", of("x", 1, "$regex", SLOW))))) {
      for (final List<Document> pipeline : List.of(facet(shape), lookup(shape)))
        // 2 is BadValue
        assertThatThrownBy(() -> run(c, pipeline)).as(pipeline.toString()).isInstanceOf(MongoException.class)
            .satisfies(e -> assertThat(((MongoException) e).getCode()).isEqualTo(2));
    }

    // the connection and the collection are as they were
    assertThat(run(c, List.of(of("$match", of("s", "alpha"))))).hasSize(1);
  }

  @Test
  @Timeout(120)
  void aRegexThatIsBoundedStopsInASubPipelineAtItsTimeout() {
    final MongoCollection<Document> c = collection(40);
    final long previous = GlobalConfiguration.COMMAND_REGEX_TIMEOUT.getValueAsLong();
    GlobalConfiguration.COMMAND_REGEX_TIMEOUT.setValue(200);
    try {
      // 50 is MaxTimeMSExpired, the answer to a regular expression that ran out of its time
      for (final Document shape : List.of(of("s", Pattern.compile(SLOW)), of("s", of("$regex", SLOW)),
          of("s", of("$in", List.of(Pattern.compile(SLOW)))), of("s", of("$not", Pattern.compile(SLOW))),
          of("s", of("$regex", SLOW, "$ne", "b"))))
        assertThatThrownBy(() -> run(c, facet(shape))).as(shape.toString()).isInstanceOf(MongoException.class)
            .satisfies(e -> assertThat(((MongoException) e).getCode()).isEqualTo(50));
    } finally {
      GlobalConfiguration.COMMAND_REGEX_TIMEOUT.setValue(previous);
    }
  }

  @Test
  @Timeout(120)
  void aRegexThatIsBoundedStillMatchesInASubPipeline() {
    final MongoCollection<Document> c = collection(24);
    for (final Document shape : List.of(of("s", Pattern.compile("^AL", Pattern.CASE_INSENSITIVE)), of("s", of("$regex", "^al")),
        of("s", of("$regex", "^AL", "$options", "i")), of("s", of("$in", List.of(Pattern.compile("^al"), "b"))),
        of("s", of("$not", Pattern.compile("^a{3}"))), of("s", of("$regex", "^al", "$ne", "b"))))
      assertThat(run(c, facet(shape)).getFirst().getList("f", Document.class)).as(shape.toString()).extracting(d -> d.get("_id"))
          .contains(2);

    // the sub-pipeline of a lookup takes the same shapes: the stage is accepted (this plugin joins no document from a sub-pipeline,
    // so every outer document is answered, with an empty array)
    for (final Document shape : List.of(of("s", Pattern.compile("^al")), of("s", of("$regex", "^al", "$ne", "b"))))
      assertThat(run(c, lookup(shape))).as(shape.toString()).extracting(d -> d.get("_id")).containsExactlyInAnyOrder(1, 2);
  }

  /**
   * Outside a sub-pipeline the document is a value to compare with, which no stored document equals: it is never searched as a
   * pattern, whatever its text. Where the plugin refuses a shape ({@code $not}, {@code $elemMatch} of fields that are no operator) it
   * still does, and the answer is never an expired regular expression.
   */
  @Test
  @Timeout(120)
  void aTopLevelMatchStillComparesTheShapeAsAValue() {
    final MongoCollection<Document> c = collection(24);
    assertThat(run(c, List.of(of("$match", of("s", of("x", 1, "$regex", "alpha")))))).isEmpty();
    assertThat(c.countDocuments(of("s", of("x", 1, "$regex", SLOW)))).isZero();
    assertThat(c.find(of("s", of("x", 1, "$regex", SLOW))).into(new ArrayList<>())).isEmpty();
    assertThat(c.find(of("s", of("$in", List.of(of("$regex", SLOW))))).into(new ArrayList<>())).isEmpty();
    assertThat(c.find(of("s", of("$all", List.of(of("$regex", SLOW))))).into(new ArrayList<>())).isEmpty();

    for (final Document shape : List.of(of("s", of("$not", of("x", 1, "$regex", SLOW))),
        of("s", of("$elemMatch", of("x", 1, "$regex", SLOW)))))
      try {
        assertThat(c.find(shape).into(new ArrayList<>())).isEmpty();
      } catch (final MongoException e) {
        assertThat(e.getCode()).isNotEqualTo(50);
      }
  }
}
