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
import com.mongodb.MongoCommandException;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CollationStrength;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11778: {@code find}, {@code count}, {@code aggregate}, {@code update}, {@code delete} and {@code create} read the fields
 * they implement and acknowledged everything else with {@code ok:1}. A {@code collation} was ignored, so a case-insensitive query,
 * update or delete matched nothing; a {@code hint} with a {@code max}, a {@code returnKey}, a {@code showRecordId} and a misspelled
 * field had no effect; a delete {@code limit} other than 0 or 1 meant "every match". Each is now refused naming the field, as
 * {@code createIndexes} refuses the index options it cannot honour (issue #11446).
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class Issue11778MongoIgnoredCommandFieldsTest extends BaseMongoServerTest {
  private static final int BAD_VALUE       = 2;
  private static final int FAILED_TO_PARSE = 9;
  private static final int UNKNOWN_FIELD   = 40415;

  private static final Document CASE_INSENSITIVE = new Document("locale", "en").append("strength", 2);

  private MongoClient               client;
  private MongoDatabase             mongoDatabase;
  private MongoCollection<Document> collection;
  private int                       created;

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
    mongoDatabase = client.getDatabase(getDatabaseName());
    collection = mongoDatabase.getCollection("c");
    collection.insertOne(new Document("_id", 1).append("name", "abc").append("arr", List.of(1, 2, 3)));
    collection.insertOne(new Document("_id", 2).append("name", "xyz"));
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    if (client != null)
      client.close();
    super.endTest();
  }

  // ---------------------------------------------------------------------------------------------------------------------------
  // the rows of the report
  // ---------------------------------------------------------------------------------------------------------------------------

  @Test
  void aCollationIsRefusedByEveryCommandInsteadOfMatchingNothing() {
    final Document upperCase = new Document("name", "ABC");

    assertRefused(new Document("find", "c").append("filter", upperCase).append("collation", CASE_INSENSITIVE), BAD_VALUE, "collation");
    assertRefused(new Document("count", "c").append("query", upperCase).append("collation", CASE_INSENSITIVE), BAD_VALUE, "collation");
    assertRefused(new Document("aggregate", "c").append("pipeline", List.of(new Document("$match", upperCase))).append("cursor", new Document())
        .append("collation", CASE_INSENSITIVE), BAD_VALUE, "collation");
    assertRefused(new Document("update", "c").append("updates",
        List.of(new Document("q", upperCase).append("u", new Document("$set", new Document("seen", true))).append("collation", CASE_INSENSITIVE))),
        BAD_VALUE, "collation");
    assertRefused(new Document("delete", "c").append("deletes",
        List.of(new Document("q", upperCase).append("limit", 0).append("collation", CASE_INSENSITIVE))), BAD_VALUE, "collation");

    assertThat(collection.countDocuments()).isEqualTo(2);
    assertThat(collection.countDocuments(new Document("seen", true))).isZero();
  }

  @Test
  void aCollationIsRefusedThroughTheDriverHelpers() {
    final Collation collation = Collation.builder().locale("en").collationStrength(CollationStrength.SECONDARY).build();
    final Document upperCase = new Document("name", "ABC");

    assertThatThrownBy(() -> collection.find(upperCase).collation(collation).first()).isInstanceOf(MongoException.class)
        .hasMessageContaining("'collation'");
    assertThatThrownBy(
        () -> collection.updateOne(upperCase, new Document("$set", new Document("seen", true)), new UpdateOptions().collation(collation)))
        .isInstanceOf(MongoException.class).hasMessageContaining("'collation'");
    assertThatThrownBy(() -> collection.deleteMany(upperCase, new DeleteOptions().collation(collation))).isInstanceOf(MongoException.class)
        .hasMessageContaining("'collation'");
    assertThatThrownBy(() -> mongoDatabase.createCollection("collated", new CreateCollectionOptions().collation(collation)))
        .isInstanceOf(MongoException.class).hasMessageContaining("'collation'");

    assertThat(serverDatabase().getSchema().existsType("collated")).isFalse();
    assertThat(collection.countDocuments()).isEqualTo(2);
  }

  @Test
  void findRefusesTheOptionsThatWouldChangeItsAnswer() {
    assertRefused(new Document("find", "c").append("filter", new Document()).append("hint", new Document("_id", 1))
        .append("max", new Document("_id", 2)), BAD_VALUE, "hint");
    assertRefused(new Document("find", "c").append("filter", new Document()).append("max", new Document("_id", 2)), BAD_VALUE, "max");
    assertRefused(new Document("find", "c").append("filter", new Document()).append("min", new Document("_id", 2)), BAD_VALUE, "min");
    assertRefused(new Document("find", "c").append("filter", new Document("_id", 1)).append("returnKey", true), BAD_VALUE, "returnKey");
    assertRefused(new Document("find", "c").append("filter", new Document("_id", 1)).append("showRecordId", true), BAD_VALUE, "showRecordId");
  }

  @Test
  void anUnknownFieldIsRefusedAsMongodRefusesIt() {
    final Document entry = new Document("q", new Document("_id", 1)).append("u", new Document("$set", new Document("seen", true)));
    final Map<String, Document> commands = new HashMap<>();
    commands.put("find.bogusOption", new Document("find", "c").append("filter", new Document("_id", 1)).append("bogusOption", 1));
    commands.put("count.bogusOption", new Document("count", "c").append("bogusOption", 1));
    commands.put("aggregate.bogusOption",
        new Document("aggregate", "c").append("pipeline", List.of()).append("cursor", new Document()).append("bogusOption", 1));
    commands.put("update.bogusOption", new Document("update", "c").append("updates", List.of(entry)).append("bogusOption", 1));
    commands.put("update.updates.bogusOption",
        new Document("update", "c").append("updates", List.of(new Document(entry).append("bogusOption", 1))));
    commands.put("delete.bogusOption",
        new Document("delete", "c").append("deletes", List.of(new Document("q", new Document()).append("limit", 0))).append("bogusOption", 1));
    commands.put("delete.deletes.bogusOption",
        new Document("delete", "c").append("deletes", List.of(new Document("q", new Document()).append("limit", 0).append("bogusOption", 1))));
    commands.put("create.bogusOption", new Document("create", "bogus").append("bogusOption", 1));
    // a generic argument belongs to the command, not to one of its statements
    commands.put("update.updates.comment",
        new Document("update", "c").append("updates", List.of(new Document(entry).append("comment", "x"))));

    for (final Map.Entry<String, Document> command : commands.entrySet())
      assertThatThrownBy(() -> mongoDatabase.runCommand(command.getValue())).as(command.getKey())
          .isInstanceOfSatisfying(MongoCommandException.class, e -> {
            assertThat(e.getErrorCode()).as(command.getKey()).isEqualTo(UNKNOWN_FIELD);
            assertThat(e.getErrorMessage()).as(command.getKey()).contains("BSON field '" + command.getKey() + "' is an unknown field");
          });

    // nothing ran
    assertThat(collection.countDocuments()).isEqualTo(2);
    assertThat(collection.countDocuments(new Document("seen", true))).isZero();
    assertThat(serverDatabase().getSchema().existsType("bogus")).isFalse();
  }

  @Test
  void anUpdateWithAHintOrArrayFiltersIsRefusedWithNothingUpdated() {
    final Document set = new Document("$set", new Document("seen", true));

    assertRefused(new Document("update", "c").append("updates",
        List.of(new Document("q", new Document("_id", 1)).append("u", set).append("hint", "no_such_index"))), BAD_VALUE, "hint");
    assertRefused(new Document("update", "c").append("updates",
        List.of(new Document("q", new Document("_id", 1)).append("u", set).append("arrayFilters", List.of(new Document("x", 1))))), BAD_VALUE,
        "arrayFilters");
    // the statement before the refused one does not run either: the command is refused when it is parsed
    assertRefused(new Document("update", "c").append("updates",
        List.of(new Document("q", new Document("_id", 2)).append("u", set), new Document("q", new Document("_id", 1)).append("u", set)
            .append("sort", new Document("_id", -1)))), BAD_VALUE, "sort");

    assertThat(collection.countDocuments(new Document("seen", true))).isZero();
  }

  @Test
  void aDeleteLimitOtherThanZeroOrOneIsRefusedWithNothingDeleted() {
    for (final Object limit : List.<Object>of(5, -1, 2L, 0.5)) {
      // the first entry is valid and matches every document: it must not run
      final Document command = new Document("delete", "c").append("deletes",
          List.of(new Document("q", new Document()).append("limit", 0), new Document("q", new Document()).append("limit", limit)));
      assertThatThrownBy(() -> mongoDatabase.runCommand(command)).as("%s", limit).isInstanceOfSatisfying(MongoCommandException.class, e -> {
        assertThat(e.getErrorCode()).as("%s", limit).isEqualTo(FAILED_TO_PARSE);
        assertThat(e.getErrorMessage()).as("%s", limit).contains("The limit field in delete objects must be 0 or 1. Got " + limit);
      });
    }

    assertThatThrownBy(() -> mongoDatabase.runCommand(
        new Document("delete", "c").append("deletes", List.of(new Document("q", new Document()).append("limit", "1")))))
        .isInstanceOfSatisfying(MongoCommandException.class, e -> assertThat(e.getErrorCode()).isEqualTo(14));
    assertThatThrownBy(() -> mongoDatabase.runCommand(new Document("delete", "c").append("deletes", List.of(new Document("q", new Document())))))
        .isInstanceOfSatisfying(MongoCommandException.class, e -> {
          assertThat(e.getErrorCode()).isEqualTo(40414);
          assertThat(e.getErrorMessage()).contains("'delete.deletes.limit'");
        });
    // refused on a collection that does not exist too
    assertRefusedWithCode(new Document("delete", "nosuch").append("deletes", List.of(new Document("q", new Document()).append("limit", 5))),
        FAILED_TO_PARSE);

    assertThat(collection.countDocuments()).isEqualTo(2);

    // 0 and 1, of any numeric type, are the two limits mongod has
    assertThat(mongoDatabase.runCommand(new Document("delete", "c").append("deletes", List.of(new Document("q", new Document()).append("limit", 1.0))))
        .get("n")).isEqualTo(1);
    assertThat(mongoDatabase.runCommand(new Document("delete", "c").append("deletes", List.of(new Document("q", new Document()).append("limit", 0L))))
        .get("n")).isEqualTo(1);
  }

  @Test
  void aDeleteLimitOtherThanZeroOrOneIsRefusedOnATimeSeriesType() {
    final Database database = serverDatabase();
    database.command("sql", "CREATE TIMESERIES TYPE Ms TIMESTAMP ts PRECISION MILLISECOND TAGS (k STRING) FIELDS (v DOUBLE)");
    database.command("sqlscript", """
        INSERT INTO Ms SET ts = 1792396800000, k = 'a', v = 1.0;
        INSERT INTO Ms SET ts = 1792396860000, k = 'a', v = 2.0;
        """);
    final MongoCollection<Document> samples = mongoDatabase.getCollection("Ms");
    assertThat(samples.countDocuments()).isEqualTo(2);

    assertRefusedWithCode(new Document("delete", "Ms").append("deletes", List.of(new Document("q", new Document("k", "a")).append("limit", 5))),
        FAILED_TO_PARSE);
    assertRefused(new Document("delete", "Ms").append("deletes",
        List.of(new Document("q", new Document("k", "a")).append("limit", 0).append("collation", CASE_INSENSITIVE))), BAD_VALUE, "collation");
    assertThat(samples.countDocuments()).isEqualTo(2);

    assertThat(samples.deleteMany(new Document("k", "a")).getDeletedCount()).isEqualTo(2);
  }

  @Test
  void createRefusesTheCollectionKindsItWouldNotCreate() {
    final Map<String, Object> options = new HashMap<>();
    options.put("capped", true);
    options.put("size", 4096);
    options.put("validator", new Document("name", new Document("$type", "string")));
    options.put("collation", CASE_INSENSITIVE);
    options.put("timeseries", new Document("timeField", "ts"));
    options.put("viewOn", "c");

    for (final Map.Entry<String, Object> option : options.entrySet()) {
      final String name = "kind_" + option.getKey();
      assertRefused(new Document("create", name).append(option.getKey(), option.getValue()), BAD_VALUE, option.getKey());
      assertThat(serverDatabase().getSchema().existsType(name)).as(option.getKey()).isFalse();
    }

    // the driver's helper sends "capped: false" with every create
    mongoDatabase.createCollection("plain");
    mongoDatabase.createCollection("plainWithOptions", new CreateCollectionOptions().capped(false));
    assertThat(serverDatabase().getSchema().existsType("plain")).isTrue();
    assertThat(serverDatabase().getSchema().existsType("plainWithOptions")).isTrue();
  }

  /** What a driver sends on its own, and the options that ask for nothing more than the plugin does, keep working. */
  @Test
  void theDriverHelpersAndTheirNeutralOptionsStillWork() {
    final List<Document> found = collection.find(new Document()).sort(new Document("_id", 1)).projection(new Document("name", 1)).skip(0).limit(5)
        .batchSize(1).maxTime(1, TimeUnit.MINUTES).comment("issue 11778").noCursorTimeout(true).partial(true).into(new ArrayList<>());
    assertThat(found).extracting(d -> d.get("name")).containsExactly("abc", "xyz");

    assertThat(collection.countDocuments(new Document("name", "abc"))).isEqualTo(1);
    assertThat(collection.estimatedDocumentCount()).isEqualTo(2);
    assertThat(collection.aggregate(List.of(new Document("$match", new Document("name", "abc")))).allowDiskUse(true).batchSize(10)
        .maxTime(1, TimeUnit.MINUTES).into(new ArrayList<>())).hasSize(1);
    assertThat(collection.updateOne(new Document("_id", 1), new Document("$set", new Document("seen", true)),
        new UpdateOptions().upsert(false).bypassDocumentValidation(false)).getModifiedCount()).isEqualTo(1);
    assertThat(collection.updateMany(new Document(), new Document("$set", new Document("all", true))).getModifiedCount()).isEqualTo(2);
    assertThat(collection.deleteOne(new Document("_id", 2)).getDeletedCount()).isEqualTo(1);
    assertThat(collection.deleteMany(new Document()).getDeletedCount()).isEqualTo(1);
  }

  @Test
  void anUnsupportedFieldThatAsksForItsDefaultIsAccepted() {
    final Document all = new Document();
    assertOk(new Document("find", "c").append("filter", all).append("tailable", false).append("awaitData", false).append("returnKey", false)
        .append("showRecordId", 0).append("collation", new Document("locale", "simple")).append("hint", new Document()).append("let", new Document())
        .append("min", null));
    assertOk(new Document("count", "c").append("collation", new Document("locale", "simple")).append("hint", null));
    assertOk(new Document("aggregate", "c").append("pipeline", List.of()).append("cursor", new Document()).append("explain", false)
        .append("let", new Document()));
    assertOk(new Document("update", "c").append("let", new Document()).append("updates", List.of(
        new Document("q", new Document("_id", 99)).append("u", new Document("$set", new Document("x", 1))).append("arrayFilters", List.of())
            .append("upsertSupplied", false).append("collation", new Document("locale", "simple")))));
    assertOk(new Document("delete", "c").append("deletes",
        List.of(new Document("q", new Document("_id", 99)).append("limit", 1).append("hint", new Document()))));
    assertOk(new Document("create", "defaults").append("capped", false).append("autoIndexId", true).append("validator", new Document())
        .append("storageEngine", new Document()));

    // the same fields asking for something are refused: "false" is only the default of a flag, an empty document only of some fields
    assertRefused(new Document("find", "c").append("tailable", true), BAD_VALUE, "tailable");
    assertRefused(new Document("find", "c").append("hint", false), BAD_VALUE, "hint");
    assertRefused(new Document("find", "c").append("collation", new Document()), BAD_VALUE, "collation");
    assertRefused(new Document("find", "c").append("collation", new Document("locale", "simple").append("strength", 1)), BAD_VALUE, "collation");
    assertRefused(new Document("create", "noIdIndex").append("autoIndexId", false), BAD_VALUE, "autoIndexId");
    assertRefused(new Document("aggregate", "c").append("pipeline", List.of()).append("cursor", new Document()).append("explain", true), BAD_VALUE,
        "explain");
  }

  // ---------------------------------------------------------------------------------------------------------------------------
  // the census: every field of every table, sent over the wire
  // ---------------------------------------------------------------------------------------------------------------------------

  /** The fields of the commands and of their statements as the MongoDB manual and mongod's IDL files list them. */
  private static Map<String, List<String>> documentedFields() {
    final Map<String, List<String>> documented = new HashMap<>();
    documented.put("find", List.of("find", "filter", "sort", "projection", "hint", "skip", "limit", "batchSize", "singleBatch", "comment",
        "maxTimeMS", "readConcern", "max", "min", "returnKey", "showRecordId", "tailable", "oplogReplay", "noCursorTimeout", "awaitData",
        "allowPartialResults", "collation", "allowDiskUse", "let", "$db", "lsid", "$clusterTime", "$readPreference", "apiVersion",
        "apiStrict", "apiDeprecationErrors", "txnNumber", "autocommit", "startTransaction"));
    documented.put("count", List.of("count", "query", "limit", "skip", "hint", "readConcern", "maxTimeMS", "collation", "comment", "$db",
        "lsid"));
    documented.put("aggregate", List.of("aggregate", "pipeline", "explain", "allowDiskUse", "cursor", "maxTimeMS", "bypassDocumentValidation",
        "readConcern", "collation", "hint", "comment", "writeConcern", "let", "$db", "lsid"));
    documented.put("update", List.of("update", "updates", "ordered", "maxTimeMS", "writeConcern", "bypassDocumentValidation", "comment", "let",
        "$db", "lsid", "txnNumber"));
    documented.put("update.updates", List.of("q", "u", "c", "upsert", "multi", "collation", "arrayFilters", "hint", "sort"));
    documented.put("delete", List.of("delete", "deletes", "comment", "let", "ordered", "writeConcern", "maxTimeMS", "$db", "lsid", "txnNumber"));
    documented.put("delete.deletes", List.of("q", "limit", "collation", "hint"));
    documented.put("create", List.of("create", "capped", "timeseries", "expireAfterSeconds", "clusteredIndex", "changeStreamPreAndPostImages",
        "autoIndexId", "size", "max", "storageEngine", "validator", "validationLevel", "validationAction", "indexOptionDefaults", "viewOn",
        "pipeline", "collation", "writeConcern", "encryptedFields", "comment", "$db", "lsid"));
    return documented;
  }

  @Test
  void everyDocumentedFieldIsEitherAppliedOrRefusedByNameNeverUnknown() {
    final Map<String, List<String>> documented = documentedFields();
    assertThat(MongoCommandFields.contexts()).as("the commands MongoCommandFields has a table for")
        .containsExactlyInAnyOrderElementsOf(documented.keySet());
    // the scan still reaches the options of the report
    assertThat(MongoCommandFields.unsupported("find")).as("MongoCommandFields: the unsupported fields of find")
        .contains("collation", "hint", "min", "max", "returnKey", "showRecordId");
    assertThat(MongoCommandFields.generic()).as("MongoCommandFields: the generic arguments").contains("$db", "lsid", "maxTimeMS");

    for (final Map.Entry<String, List<String>> command : documented.entrySet()) {
      final String context = command.getKey();
      final boolean statement = context.contains(".");
      for (final String field : command.getValue()) {
        final boolean known = MongoCommandFields.accepted(context).contains(field) || MongoCommandFields.unsupported(context).contains(field)
            || (!statement && MongoCommandFields.generic().contains(field));
        assertThat(known).as("'%s.%s' is a field of mongod: MongoCommandFields must list it as accepted, generic or unsupported", context, field)
            .isTrue();
      }
      // a field is in one table only: what is applied is not refused
      assertThat(MongoCommandFields.accepted(context)).as(context).doesNotContainAnyElementsOf(MongoCommandFields.unsupported(context));
      if (!statement)
        assertThat(MongoCommandFields.generic()).as(context).doesNotContainAnyElementsOf(MongoCommandFields.accepted(context))
            .doesNotContainAnyElementsOf(MongoCommandFields.unsupported(context));
    }
  }

  /** A value that asks for the feature of each field the plugin does not support. */
  private static Map<String, Object> refusedValues() {
    final Map<String, Object> values = new HashMap<>();
    for (final String flag : List.of("returnKey", "showRecordId", "tailable", "awaitData", "readOnce", "allowSpeculativeMajorityRead", "rawData",
        "mirrored", "includeQueryStatsMetrics", "explain", "needsMerge", "fromMongos", "fromRouter", "isMapReduceCommand",
        "isClusterQueryWithoutShardKeyCmd", "isTimeseriesNamespace", "bypassEmptyTsReplacement", "upsertSupplied", "capped", "temp",
        "recordPreImages", "recordIdsReplicated", "$_requestResumeToken", "$_requestReshardingResumeToken", "$_generateV2ResumeTokens",
        "$_allowShardKeyUpdatesWithoutFullShardKeyInQuery"))
      values.put(flag, true);
    values.put("autoIndexId", false);
    values.put("collation", CASE_INSENSITIVE);
    values.put("hint", new Document("_id", 1));
    values.put("min", new Document("_id", 1));
    values.put("max", new Document("_id", 2));
    values.put("let", new Document("v", 1));
    values.put("collectionUUID", "f7a6b3c2-0000-4000-8000-000000000000");
    values.put("encryptionInformation", new Document("type", 1));
    values.put("term", 1L);
    values.put("querySettings", new Document("reject", true));
    values.put("sampleId", "f7a6b3c2-0000-4000-8000-000000000001");
    values.put("$_resumeAfter", new Document("$recordId", 1L));
    values.put("$_startAt", new Document("$recordId", 1L));
    values.put("exchange", new Document("consumers", 2));
    values.put("runtimeConstants", new Document("localNow", new Date()));
    values.put("passthroughToShard", new Document("shard", "s0"));
    values.put("$_passthroughToShard", new Document("shard", "s0"));
    values.put("stmtIds", List.of(1));
    values.put("$_originalQuery", new Document("a", 1));
    values.put("$_originalCollation", CASE_INSENSITIVE);
    values.put("arrayFilters", List.of(new Document("x", 1)));
    values.put("c", new Document("v", 1));
    values.put("sort", new Document("_id", -1));
    values.put("idIndex", new Document("key", new Document("_id", 1)).append("name", "_id_"));
    values.put("size", 4096);
    values.put("storageEngine", new Document("wiredTiger", new Document()));
    values.put("validator", new Document("name", new Document("$type", "string")));
    values.put("validationLevel", "strict");
    values.put("validationAction", "error");
    values.put("indexOptionDefaults", new Document("storageEngine", new Document("wiredTiger", new Document())));
    values.put("viewOn", "c");
    values.put("pipeline", List.of(new Document("$match", new Document())));
    values.put("changeStreamPreAndPostImages", new Document("enabled", true));
    values.put("timeseries", new Document("timeField", "ts"));
    values.put("clusteredIndex", new Document("key", new Document("_id", 1)).append("unique", true));
    values.put("expireAfterSeconds", 60);
    values.put("encryptedFields", new Document("fields", List.of()));
    values.put("flags", 1);
    return values;
  }

  /** A value of each field the plugin applies, or accepts because it changes nothing: the command still answers. */
  private static Map<String, Object> acceptedValues() {
    final Map<String, Object> values = new HashMap<>();
    values.put("find", "c");
    values.put("count", "c");
    values.put("aggregate", "c");
    values.put("update", "c");
    values.put("delete", "c");
    values.put("filter", new Document("_id", 1));
    values.put("query", new Document("_id", 1));
    values.put("sort", new Document("_id", 1));
    values.put("projection", new Document("name", 1));
    values.put("skip", 0);
    values.put("limit", 1);
    values.put("batchSize", 10);
    values.put("singleBatch", true);
    values.put("allowDiskUse", true);
    values.put("noCursorTimeout", true);
    values.put("allowPartialResults", true);
    values.put("oplogReplay", true);
    values.put("fields", new Document("name", 1));
    values.put("pipeline", List.of(new Document("$match", new Document("_id", 1))));
    values.put("cursor", new Document("batchSize", 10));
    values.put("bypassDocumentValidation", true);
    values.put("ordered", false);
    values.put("updates", List.of(new Document("q", new Document("_id", 99)).append("u", new Document("$set", new Document("x", 1)))));
    values.put("deletes", List.of(new Document("q", new Document("_id", 99)).append("limit", 1)));
    values.put("q", new Document("_id", 99));
    values.put("u", new Document("$set", new Document("x", 1)));
    values.put("multi", true);
    values.put("upsert", false);
    return values;
  }

  @Test
  void everyFieldOfEveryTableIsAppliedOrRefusedByNameOverTheWire() {
    final Map<String, Object> accepted = acceptedValues();
    final Map<String, Object> refused = refusedValues();
    int sent = 0;

    for (final String context : MongoCommandFields.contexts()) {
      for (final String field : MongoCommandFields.accepted(context)) {
        final String what = context + "." + field;
        if (!"create".equals(field))
          assertThat(accepted).as("a value to send for the accepted field '%s'", what).containsKey(field);
        assertThat(mongoDatabase.runCommand(with(context, field, accepted.get(field))).get("ok")).as(what).isEqualTo(1.0);
        ++sent;
      }
      for (final String field : MongoCommandFields.unsupported(context)) {
        final String what = context + "." + field;
        assertThat(refused).as("a value to send for the unsupported field '%s'", what).containsKey(field);
        assertThatThrownBy(() -> mongoDatabase.runCommand(with(context, field, refused.get(field)))).as(what)
            .isInstanceOfSatisfying(MongoCommandException.class, e -> {
              assertThat(e.getErrorCode()).as(what).isEqualTo(BAD_VALUE);
              assertThat(e.getErrorCodeName()).as(what).isEqualTo("BadValue");
              assertThat(e.getErrorMessage()).as(what).contains("'" + field + "'").contains("'" + context + "'");
            });
        ++sent;
      }
    }

    // the scan still reaches MongoCommandFields: the fields of the report, and far more than a handful
    assertThat(sent).as("fields of MongoCommandFields sent over the wire").isGreaterThan(100);
    // a refused command ran nothing
    assertThat(collection.countDocuments()).isEqualTo(2);
  }

  /** The generic arguments are the ones a driver adds on its own: none may be refused, on any command. */
  @Test
  void everyGenericArgumentIsAcceptedOnEveryCommand() {
    final Set<String> generic = MongoCommandFields.generic();
    assertThat(generic).as("MongoCommandFields: the generic arguments").contains("$db", "lsid", "$clusterTime", "$readPreference", "txnNumber",
        "autocommit", "startTransaction", "readConcern", "writeConcern", "apiVersion", "apiStrict", "apiDeprecationErrors", "comment",
        "maxTimeMS");

    for (final String context : MongoCommandFields.contexts()) {
      if (context.contains("."))
        continue;
      final de.bwaldvogel.mongo.bson.Document command = new de.bwaldvogel.mongo.bson.Document(context, "c");
      for (final String argument : generic)
        command.put(argument, 1);
      MongoCommandFields.check(context, command);
    }

    // and over the wire, the ones the driver lets a caller add
    assertOk(new Document("find", "c").append("comment", "x").append("maxTimeMS", 60_000).append("readConcern", new Document("level", "local"))
        .append("apiVersion", "1").append("apiStrict", false).append("apiDeprecationErrors", false));
    assertOk(new Document("update", "c").append("updates",
        List.of(new Document("q", new Document("_id", 99)).append("u", new Document("$set", new Document("x", 1)))))
        .append("writeConcern", new Document("w", 1)).append("comment", "x").append("maxTimeMS", 60_000).append("txnNumber", 1L));
  }

  /** The base command of a context with {@code field} set to {@code value}, on the command or on its only statement. */
  private Document with(final String context, final String field, final Object value) {
    final Document updateEntry = new Document("q", new Document("_id", 99)).append("u", new Document("$set", new Document("x", 1)));
    final Document deleteEntry = new Document("q", new Document("_id", 99)).append("limit", 1);
    return switch (context) {
      case "find" -> new Document("find", "c").append(field, value);
      case "count" -> new Document("count", "c").append(field, value);
      case "aggregate" -> new Document("aggregate", "c").append("pipeline", List.of()).append("cursor", new Document()).append(field, value);
      case "update" -> new Document("update", "c").append("updates", List.of(updateEntry)).append(field, value);
      case "update.updates" -> new Document("update", "c").append("updates", List.of(updateEntry.append(field, value)));
      case "delete" -> new Document("delete", "c").append("deletes", List.of(deleteEntry)).append(field, value);
      case "delete.deletes" -> new Document("delete", "c").append("deletes", List.of(deleteEntry.append(field, value)));
      // a new name each time: a refused create must not be answered "already exists" instead
      case "create" -> "create".equals(field) ? new Document("create", "census" + created++) : new Document("create", "census" + created++).append(field, value);
      default -> throw new IllegalArgumentException(context);
    };
  }

  private void assertOk(final Document command) {
    assertThat(mongoDatabase.runCommand(command).get("ok")).as("%s", command).isEqualTo(1.0);
  }

  private void assertRefused(final Document command, final int code, final String field) {
    assertThatThrownBy(() -> mongoDatabase.runCommand(command)).as("%s", command).isInstanceOfSatisfying(MongoCommandException.class, e -> {
      assertThat(e.getErrorCode()).as("%s", command).isEqualTo(code);
      assertThat(e.getErrorMessage()).as("%s", command).contains("'" + field + "'");
    });
  }

  private void assertRefusedWithCode(final Document command, final int code) {
    assertThatThrownBy(() -> mongoDatabase.runCommand(command)).as("%s", command)
        .isInstanceOfSatisfying(MongoCommandException.class, e -> assertThat(e.getErrorCode()).as("%s", command).isEqualTo(code));
  }

  private Database serverDatabase() {
    return getServerDatabase(0, getDatabaseName());
  }
}
