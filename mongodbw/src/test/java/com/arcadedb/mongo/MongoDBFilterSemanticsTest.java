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
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.MongoException;
import com.mongodb.MongoQueryException;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for #9137 (array fields), #9136 (BSON type bracketing), #9138 (nested {@code $exists}), #9139 (dotted
 * collection names) and #9065 (implicit collection creation). Every expectation is the answer of a MongoDB 7 server.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class MongoDBFilterSemanticsTest extends BaseMongoServerTest {

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

  private MongoCollection<Document> collection(final String name, final String... documents) {
    final MongoCollection<Document> collection = client.getDatabase(getDatabaseName()).getCollection(name);
    final List<Document> list = new ArrayList<>();
    for (final String document : documents)
      list.add(Document.parse(document));
    collection.insertMany(list);
    return collection;
  }

  private static List<Object> ids(final MongoCollection<Document> collection, final String filter) {
    final List<Object> ids = new ArrayList<>();
    for (final Document d : collection.find(Document.parse(filter)).sort(Document.parse("{_id:1}")))
      ids.add(d.get("_id"));
    return ids;
  }

  private MongoCollection<Document> arrays() {
    return collection("arrays", "{_id:1, tags:['a','b']}", "{_id:2, tags:['b']}", "{_id:3, tags:[]}", "{_id:4}", "{_id:5, tags:[1,2,3]}",
        "{_id:6, tags:[{k:1},{k:2}]}");
  }

  @Test
  void arrayFieldsMatchTheirElements() {
    final MongoCollection<Document> c = arrays();
    assertThat(ids(c, "{tags:'a'}")).containsExactly(1);
    assertThat(ids(c, "{tags:'b'}")).containsExactly(1, 2);
    assertThat(ids(c, "{tags:{$in:['a']}}")).containsExactly(1);
    assertThat(ids(c, "{'tags.0':'a'}")).containsExactly(1);
    assertThat(ids(c, "{'tags.k':1}")).containsExactly(6);
    assertThat(ids(c, "{tags:2}")).containsExactly(5);
    assertThat(ids(c, "{tags:{$gt:2}}")).containsExactly(5);
    assertThat(ids(c, "{tags:{$size:0}}")).containsExactly(3);
    assertThat(ids(c, "{tags:{$all:['a','b']}}")).containsExactly(1);
    assertThat(ids(c, "{tags:{$elemMatch:{k:2}}}")).containsExactly(6);
    assertThat(ids(c, "{tags:['b']}")).containsExactly(2);
  }

  @Test
  void negationsOnArraysExcludeTheDocumentsHoldingTheElement() {
    final MongoCollection<Document> c = arrays();
    assertThat(ids(c, "{tags:{$ne:'a'}}")).containsExactly(2, 3, 4, 5, 6);
    assertThat(ids(c, "{tags:{$nin:['a']}}")).containsExactly(2, 3, 4, 5, 6);
  }

  @Test
  void deleteManyWithNeKeepsTheDocumentHoldingTheElement() {
    final MongoCollection<Document> c = arrays();
    assertThat(c.deleteMany(Document.parse("{tags:{$ne:'a'}}")).getDeletedCount()).isEqualTo(5);
    assertThat(ids(c, "{}")).containsExactly(1);
  }

  @Test
  void updateManyOnArrayElement() {
    final MongoCollection<Document> c = arrays();
    assertThat(c.updateMany(Document.parse("{tags:'b'}"), Document.parse("{$set:{hit:1}}")).getModifiedCount()).isEqualTo(2);
    assertThat(ids(c, "{hit:1}")).containsExactly(1, 2);
  }

  @Test
  void typesAreBracketed() {
    final MongoCollection<Document> c = collection("types", "{_id:1, n:5}", "{_id:2, n:'5'}", "{_id:3, n:5.0}", "{_id:4, n:true}",
        "{_id:5, n:'true'}", "{_id:6, n:1}");
    assertThat(ids(c, "{n:5}")).containsExactly(1, 3);
    assertThat(ids(c, "{n:'5'}")).containsExactly(2);
    assertThat(ids(c, "{n:true}")).containsExactly(4);
    assertThat(ids(c, "{n:1}")).containsExactly(6);
    assertThat(ids(c, "{n:{$gt:0}}")).containsExactly(1, 3, 6);

    final MongoCollection<Document> ages = collection("ages", "{_id:1, age:30}", "{_id:2, age:'40'}", "{_id:3, age:'abc'}", "{_id:4, age:20}");
    assertThat(ids(ages, "{age:{$gt:26}}")).containsExactly(1);
    assertThat(ids(ages, "{age:{$lt:'50'}}")).containsExactly(2);
  }

  @Test
  void dateDoesNotEqualALong() {
    final MongoCollection<Document> c = client.getDatabase(getDatabaseName()).getCollection("dates");
    c.insertOne(new Document("_id", 1).append("d", new Date(5000)));
    c.insertOne(new Document("_id", 2).append("d", 5000L));
    final List<Object> found = new ArrayList<>();
    for (final Document d : c.find(new Document("d", new Date(5000))))
      found.add(d.get("_id"));
    assertThat(found).containsExactly(1);
  }

  @Test
  void writesOnlyTouchTheDocumentsOfTheFilterType() {
    final MongoCollection<Document> c = collection("bytype", "{_id:1, n:5}", "{_id:2, n:'5'}");
    assertThat(c.updateMany(Document.parse("{n:'5'}"), Document.parse("{$set:{hit:1}}")).getModifiedCount()).isEqualTo(1);
    assertThat(c.deleteMany(Document.parse("{n:5}")).getDeletedCount()).isEqualTo(1);
    assertThat(ids(c, "{}")).containsExactly(2);
  }

  @Test
  void existsOnNestedPath() {
    final MongoCollection<Document> c = collection("nested", "{_id:1, a:{b:1, c:{d:'x'}}}", "{_id:2, a:{c:1}}", "{_id:3, a:5}", "{_id:4, z:1}",
        "{_id:5, a:null}", "{_id:6, a:{}}");
    assertThat(ids(c, "{'a.b':{$exists:true}}")).containsExactly(1);
    assertThat(ids(c, "{'a.b':{$exists:false}}")).containsExactly(2, 3, 4, 5, 6);
    assertThat(ids(c, "{'a.c.d':{$exists:true}}")).containsExactly(1);
    assertThat(ids(c, "{'a.c.d':{$exists:false}}")).containsExactly(2, 3, 4, 5, 6);
    assertThat(ids(c, "{'a.c':{$exists:true}}")).containsExactly(1, 2);
    assertThat(ids(c, "{a:{$exists:true}}")).containsExactly(1, 2, 3, 5, 6);
    assertThat(ids(c, "{a:{$exists:false}}")).containsExactly(4);
    assertThat(c.deleteMany(Document.parse("{'a.b':{$exists:false}}")).getDeletedCount()).isEqualTo(5);
  }

  @Test
  void dottedCollectionNameIsReadable() {
    final MongoCollection<Document> c = collection("fs.files", "{_id:1, k:'a'}", "{_id:2, k:'b'}");
    assertThat(c.countDocuments()).isEqualTo(2);
    assertThat(ids(c, "{}")).containsExactly(1, 2);
    assertThat(ids(c, "{k:'b'}")).containsExactly(2);
    assertThat(ids(c, "{_id:1}")).containsExactly(1);

    final MongoCollection<Document> deep = collection("a.b.c", "{_id:1, k:'a'}");
    assertThat(ids(deep, "{k:'a'}")).containsExactly(1);
  }

  @Test
  void insertAndCountOnMissingCollection() {
    final MongoCollection<Document> missing = client.getDatabase(getDatabaseName()).getCollection("never_created_0");
    assertThat(missing.countDocuments()).isZero();
    assertThat(missing.countDocuments(Document.parse("{k:1}"))).isZero();

    final MongoCollection<Document> one = client.getDatabase(getDatabaseName()).getCollection("never_created_1");
    one.insertOne(new Document("_id", 1));
    assertThat(one.countDocuments()).isEqualTo(1);

    final MongoCollection<Document> many = client.getDatabase(getDatabaseName()).getCollection("never_created_2");
    many.insertMany(List.of(new Document("_id", 1), new Document("_id", 2)));
    assertThat(many.countDocuments()).isEqualTo(2);
  }

  @Test
  void regexStillMatches() {
    final MongoCollection<Document> c = collection("regex", "{_id:1, s:'Hello'}", "{_id:2, s:'world'}", "{_id:3, s:['x','hello']}");
    assertThat(ids(c, "{s:{$regex:'^hel', $options:'i'}}")).containsExactly(1, 3);
    assertThat(ids(c, "{s:{$regex:'^hel', $options:'i', $ne:'Hello'}}")).containsExactly(3);
    assertThat(ids(c, "{s:{$not:{$regex:'^hel', $options:'i'}}}")).containsExactly(2);
  }

  @Test
  void sortSkipAndLimitCountTheMatchesOfAFilterOnAnArray() {
    final MongoCollection<Document> c = collection("paged", "{_id:1, t:['x'], n:3}", "{_id:2, t:['y'], n:9}", "{_id:3, t:['x'], n:1}",
        "{_id:4, t:['x','y'], n:2}", "{_id:5, t:['x'], n:4}");
    final List<Object> page = new ArrayList<>();
    for (final Document d : c.find(Document.parse("{t:'x'}")).sort(Document.parse("{n:1}")).skip(1).limit(2))
      page.add(d.get("_id"));
    assertThat(page).containsExactly(4, 1);

    assertThat(c.countDocuments(Document.parse("{t:'x'}"))).isEqualTo(4);
    assertThat(c.countDocuments(Document.parse("{t:'x'}"), new CountOptions().skip(1).limit(2))).isEqualTo(2);
  }

  @Test
  void deleteOneAndUpsertOnANonIdFilter() {
    final MongoCollection<Document> c = collection("writes", "{_id:1, t:['x']}", "{_id:2, t:['x']}", "{_id:3, t:['y']}");
    assertThat(c.deleteOne(Document.parse("{t:'x'}")).getDeletedCount()).isEqualTo(1);
    assertThat(c.countDocuments()).isEqualTo(2);

    // the array already holds 'y': the upsert matches it instead of inserting
    c.updateOne(Document.parse("{t:'y'}"), Document.parse("{$set:{hit:1}}"), new UpdateOptions().upsert(true));
    assertThat(c.countDocuments()).isEqualTo(2);
    assertThat(ids(c, "{hit:1}")).containsExactly(3);
  }

  @Test
  void logicalAndRegexOperatorsOnArrays() {
    final MongoCollection<Document> c = collection("logic", "{_id:1, s:['alpha','beta']}", "{_id:2, s:['gamma']}", "{_id:3, s:['delta']}");
    assertThat(ids(c, "{$nor:[{s:'gamma'},{s:'delta'}]}")).containsExactly(1);
    assertThat(ids(c, "{s:{$in:[{$regex:'^ga'}]}}")).isEmpty();
    assertThatThrownBy(() -> ids(c, "{s:{$elemMatch:{$regex:'^be'}}}")).isInstanceOf(MongoQueryException.class);
    assertThat(ids(c, "{s:{$regex:'^d'}}")).containsExactly(3);
  }

  @Test
  void topLevelNotIsRefusedLikeMongoDB() {
    final MongoCollection<Document> c = collection("notop", "{_id:1, k:1}");
    assertThatThrownBy(() -> ids(c, "{$not:{k:1}}")).isInstanceOf(MongoQueryException.class);
  }

  @Test
  void objectIdOutsideTheIdMatchesBothStoredForms() {
    final ObjectId oid = new ObjectId("507f1f77bcf86cd799439021");
    final MongoCollection<Document> c = client.getDatabase(getDatabaseName()).getCollection("oids");
    c.insertOne(new Document("_id", 1).append("ref", oid));
    c.insertOne(new Document("_id", 2).append("ref", new ObjectId("507f1f77bcf86cd799439022")));
    c.insertOne(new Document("_id", 3).append("refs", List.of(oid)));
    assertThat(ids(c, "{}")).hasSize(3);
    final List<Object> found = new ArrayList<>();
    for (final Document d : c.find(new Document("ref", oid)))
      found.add(d.get("_id"));
    assertThat(found).containsExactly(1);
    found.clear();
    for (final Document d : c.find(new Document("refs", oid)))
      found.add(d.get("_id"));
    assertThat(found).containsExactly(3);
  }

  @Test
  void regexLiteralInsideAnOperatorDocumentKeepsItsFlags() {
    final MongoCollection<Document> c = collection("literal", "{_id:1, s:'Hello'}", "{_id:2, s:'world'}");
    assertThat(c.find(new Document("s", new Document("$regex", Pattern.compile("^hel", Pattern.CASE_INSENSITIVE)))).into(new ArrayList<>()))
        .hasSize(1);
  }

  @Test
  void invalidFiltersOnTheWritePathAreCleanErrorsAndLeaveNothingOpen() {
    final MongoCollection<Document> c = collection("badwrites", "{_id:1, s:'a'}", "{_id:2, s:'b'}");
    assertThatThrownBy(() -> c.deleteMany(Document.parse("{s:{$regex:'a)|(b'}}"))).isInstanceOf(MongoException.class);
    assertThatThrownBy(() -> c.updateMany(Document.parse("{$not:{s:'a'}}"), Document.parse("{$set:{x:1}}"))).isInstanceOf(MongoException.class);
    assertThatThrownBy(() -> c.updateOne(Document.parse("{s:{$regex:'(', $options:'i'}}"), Document.parse("{$set:{x:1}}"),
        new UpdateOptions().upsert(true))).isInstanceOf(MongoException.class);

    assertThat(c.countDocuments()).isEqualTo(2);
    assertThat(c.deleteOne(Document.parse("{s:'b'}")).getDeletedCount()).isEqualTo(1);
  }

  @Test
  void idInsideLogicalOperatorsTakesTheStoredForm() {
    final ObjectId oid = new ObjectId("507f1f77bcf86cd799439031");
    final MongoCollection<Document> c = client.getDatabase(getDatabaseName()).getCollection("oridfilter");
    c.insertOne(new Document("_id", oid).append("k", List.of(1, 2)));
    c.insertOne(new Document("_id", new ObjectId("507f1f77bcf86cd799439032")).append("k", List.of(3)));
    assertThat(c.countDocuments(new Document("$or", List.of(new Document("_id", oid), new Document("k", 99))))).isEqualTo(1);
    assertThat(c.countDocuments(new Document("$and", List.of(new Document("_id", oid), new Document("k", 2))))).isEqualTo(1);
    assertThat(c.countDocuments(new Document("$and", List.of(new Document("_id", oid), new Document("k", 3))))).isZero();
  }

  @Test
  void regexOperandsInsideInAndNotAreBounded() {
    final MongoCollection<Document> c = collection("regexops", "{_id:1, s:'alpha'}", "{_id:2, s:'beta'}", "{_id:3, s:['gamma','alpine']}");
    assertThat(ids(c, "{s:{$not:{$regex:'^al'}}}")).containsExactly(2);
    assertThat(c.find(new Document("s", new Document("$in", List.of(Pattern.compile("^al"), "beta")))).into(new ArrayList<>())).hasSize(3);
    assertThat(c.find(new Document("s", new Document("$nin", List.of(Pattern.compile("^al"))))).into(new ArrayList<>())).hasSize(1);
  }

  @Test
  void aCatastrophicRegexIsAbortedWithACleanErrorAndTheConnectionSurvives() {
    final MongoCollection<Document> c = collection("redos", new Document("_id", 1).append("s", "a".repeat(40) + "!").toJson());
    final long previous = GlobalConfiguration.COMMAND_REGEX_TIMEOUT.getValueAsLong();
    GlobalConfiguration.COMMAND_REGEX_TIMEOUT.setValue(200);
    try {
      // 50 is MaxTimeMSExpired, the answer to a regular expression that ran out of its time
      assertThatThrownBy(() -> ids(c, "{s:{$regex:'(.*a){20}$'}}")).isInstanceOf(MongoException.class)
          .satisfies(e -> assertThat(((MongoException) e).getCode()).isEqualTo(50));
    } finally {
      GlobalConfiguration.COMMAND_REGEX_TIMEOUT.setValue(previous);
    }
    assertThat(ids(c, "{}")).containsExactly(1);
  }

  @Test
  void aCatastrophicRegexOnIdInAMixedFilterIsBoundedToo() {
    final MongoCollection<Document> c = collection("redosid", new Document("_id", "a".repeat(40) + "!").append("n", 1).toJson());
    final long previous = GlobalConfiguration.COMMAND_REGEX_TIMEOUT.getValueAsLong();
    GlobalConfiguration.COMMAND_REGEX_TIMEOUT.setValue(200);
    try {
      assertThatThrownBy(() -> ids(c, "{_id:{$regex:'(.*a){20}$'}, n:1}")).isInstanceOf(MongoException.class)
          .satisfies(e -> assertThat(((MongoException) e).getCode()).isEqualTo(50));
      assertThatThrownBy(() -> c.find(new Document("$or", List.of(new Document("_id", Pattern.compile("(.*a){20}$")), new Document("n", 2))))
          .into(new ArrayList<>())).isInstanceOf(MongoException.class);
    } finally {
      GlobalConfiguration.COMMAND_REGEX_TIMEOUT.setValue(previous);
    }
    assertThat(ids(c, "{_id:{$regex:'^a'}, n:1}")).hasSize(1);
  }

  @Test
  void exprIsRefusedAndAllHoldingAnElemMatchWorks() {
    final MongoCollection<Document> c = collection("exprall", "{_id:1, s:[{k:'x'},{k:'y'}]}", "{_id:2, s:[{k:'x'}]}");
    assertThatThrownBy(() -> ids(c, "{$expr:{$regexMatch:{input:'$s', regex:'a'}}}")).isInstanceOf(MongoQueryException.class);
    assertThat(ids(c, "{s:{$all:[{$elemMatch:{k:'x'}},{$elemMatch:{k:'y'}}]}}")).containsExactly(1);
  }

  @Test
  void aCatastrophicRegexUnderElemMatchAndLogicalOperatorsIsBoundedToo() {
    final MongoCollection<Document> c = collection("redosnested",
        "{_id:1, arr:[{name:'" + "a".repeat(40) + "!'}]}");
    final long previous = GlobalConfiguration.COMMAND_REGEX_TIMEOUT.getValueAsLong();
    GlobalConfiguration.COMMAND_REGEX_TIMEOUT.setValue(200);
    try {
      assertThatThrownBy(() -> ids(c, "{arr:{$elemMatch:{$or:[{name:{$regex:'(.*a){20}$'}}]}}}")).isInstanceOf(MongoException.class)
          .satisfies(e -> assertThat(((MongoException) e).getCode()).isEqualTo(50));
      assertThatThrownBy(() -> ids(c, "{arr:{$elemMatch:{$and:[{name:{$regex:'(.*a){20}$'}}, {name:{$exists:true}}]}}}"))
          .isInstanceOf(MongoException.class).satisfies(e -> assertThat(((MongoException) e).getCode()).isEqualTo(50));
    } finally {
      GlobalConfiguration.COMMAND_REGEX_TIMEOUT.setValue(previous);
    }
    assertThat(ids(c, "{arr:{$elemMatch:{$or:[{name:{$regex:'^a'}}]}}}")).containsExactly(1);
  }

  @Test
  void idPlusAnotherFieldFiltersLikeOptimisticLocking() {
    final MongoCollection<Document> c = collection("locking", "{_id:1, version:1, tags:['a']}", "{_id:2, version:1, tags:['b']}",
        "{_id:3, version:2, tags:['a','b']}");
    assertThat(ids(c, "{_id:1, version:1}")).containsExactly(1);
    assertThat(ids(c, "{_id:1, version:2}")).isEmpty();
    assertThat(ids(c, "{_id:{$in:[1,2,3]}, tags:'a'}")).containsExactly(1, 3);
    assertThat(c.countDocuments(Document.parse("{_id:{$in:[1,2,3]}, tags:'b'}"))).isEqualTo(2);

    assertThat(c.updateOne(Document.parse("{_id:1, version:2}"), Document.parse("{$set:{x:1}}")).getMatchedCount()).isZero();
    assertThat(c.updateOne(Document.parse("{_id:1, version:1}"), Document.parse("{$set:{version:2}}")).getModifiedCount()).isEqualTo(1);
    assertThat(c.deleteOne(Document.parse("{_id:2, tags:'a'}")).getDeletedCount()).isZero();
    assertThat(c.deleteOne(Document.parse("{_id:2, tags:'b'}")).getDeletedCount()).isEqualTo(1);
    assertThat(ids(c, "{}")).containsExactly(1, 3);
    final List<Object> sorted = new ArrayList<>();
    for (final Document d : c.find(Document.parse("{_id:{$in:[1,3]}, tags:'a'}")).sort(Document.parse("{version:-1}")))
      sorted.add(d.get("_id"));
    assertThat(sorted).containsExactly(3, 1);
  }
}
