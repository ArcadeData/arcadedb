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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.utility.FileUtils;
import de.bwaldvogel.mongo.bson.BsonRegularExpression;
import de.bwaldvogel.mongo.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12071: an upsert that matched nothing seeded the inserted document from the top-level fields of its filter only, and copied
 * a dotted key as it was. {@code {'a.b': 5}} stored a field literally named "a.b" that no later {@code {'a.b': 5}} matched, and the
 * equalities under {@code $and}, a single-branch {@code $or}, a one-value {@code $in} and an embedded-document equality (also on
 * {@code _id}) were dropped, so running the same upsert twice inserted two documents. A replacement upsert copied every filter field
 * where mongod takes the {@code _id} alone, and the generated {@code _id} was stored as the last field instead of the first.
 * <p>
 * mongod infers the fields only when the filter matches nothing and the statement inserts: a filter it cannot infer from (a path
 * matched twice) is refused by that statement alone, as an entry of {@code writeErrors}, and is still an update of the document it
 * matches.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12071MongoUpsertSeedTest {
  private static final String DB_PATH = "./target/databases/issue12071";

  private Database               database;
  private MongoDBDatabaseWrapper wrapper;

  @BeforeEach
  void beginTest() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
    wrapper = new MongoDBDatabaseWrapper(database, null, null);
  }

  @AfterEach
  void endTest() {
    if (database != null) {
      while (database.isTransactionActive())
        database.rollback();
      ((DatabaseInternal) database).getEmbedded().drop();
    }
  }

  @Test
  void aDottedEqualitySeedsTheEmbeddedDocumentAndARepeatedUpsertMatchesIt() {
    final Document filter = new Document("a.b", 5);
    final Document first = upsert("C1", filter, new Document("$inc", new Document("hits", 1)));
    final Document second = upsert("C1", filter, new Document("$inc", new Document("hits", 1)));

    assertThat(first.get("upserted")).isNotNull();
    assertThat(second.get("upserted")).as("the second run matches the document the first inserted").isNull();
    assertThat(second.get("n")).isEqualTo(1);
    final List<Document> stored = findAll("C1");
    assertThat(stored).hasSize(1);
    assertThat(stored.getFirst().get("a")).isEqualTo(new Document("b", 5));
    assertThat(stored.getFirst().get("hits")).isEqualTo(2);
    assertThat(stored.getFirst().containsKey("a.b")).isFalse();
  }

  @Test
  void equalitiesUnderAndAndASingleBranchOrAreSeeded() {
    upsert("C2", new Document("$and", List.of(new Document("a", 1), new Document("b", 2))), new Document("$set", new Document("c", 1)));
    upsert("C2", new Document("$and", List.of(new Document("a", 1), new Document("b", 2))), new Document("$set", new Document("c", 2)));
    final List<Document> and = findAll("C2");
    assertThat(and).hasSize(1);
    assertThat(and.getFirst()).containsEntry("a", 1).containsEntry("b", 2).containsEntry("c", 2);

    upsert("C3", new Document("$or", List.of(new Document("a", 1))), new Document("$set", new Document("c", 1)));
    assertThat(findAll("C3").getFirst()).containsEntry("a", 1);

    // several branches say nothing about which one the document satisfies
    upsert("C4", new Document("$or", List.of(new Document("a", 1), new Document("b", 2))), new Document("$set", new Document("c", 1)));
    assertThat(findAll("C4").getFirst()).doesNotContainKeys("a", "b");
  }

  @Test
  void aOneValueInAndAnEmbeddedDocumentEqualityAreSeeded() {
    upsert("C5", new Document("a", new Document("$in", List.of(7))), new Document("$set", new Document("c", 1)));
    assertThat(findAll("C5").getFirst()).containsEntry("a", 7);

    upsert("C6", new Document("a", new Document("x", 1)), new Document("$set", new Document("a.y", 2)));
    assertThat(findAll("C6").getFirst().get("a")).isEqualTo(new Document("x", 1).append("y", 2));

    final Document reply = upsert("C7", new Document("_id", new Document("k", 2)), new Document("$set", new Document("c", 1)));
    assertThat(((Document) ((List<?>) reply.get("upserted")).getFirst()).get("_id")).isEqualTo(new Document("k", 2));
    assertThat(findAll("C7").getFirst().get("_id")).isEqualTo(new Document("k", 2));
  }

  @Test
  void aReplacementUpsertTakesOnlyTheIdFromTheFilter() {
    upsert("C8", new Document("b", 1).append("_id", 5), new Document("x", 1));
    final Document stored = findAll("C8").getFirst();
    assertThat(stored).containsEntry("_id", 5).containsEntry("x", 1).doesNotContainKey("b");
  }

  @Test
  void theIdIsTheFirstFieldOfTheInsertedDocument() {
    upsert("C9", new Document("a", 1), new Document("$set", new Document("c", 1)));
    upsert("C10", new Document("a", 1), new Document("$set", new Document("c", 1).append("_id", 9)));
    for (final String collection : List.of("C9", "C10"))
      assertThat(new ArrayList<>(findAll(collection).getFirst().keySet())).as(collection).containsExactly("_id", "a", "c");
  }

  @Test
  void aPathMatchedTwiceIsAWriteErrorOfTheStatementAndInsertsNothing() {
    for (final Document filter : List.of(//
        new Document("$and", List.of(new Document("a", 1), new Document("a", 2))),//
        new Document("a", 1).append("a.b", 2),//
        new Document("a", new Document("$all", List.of(5, 6))),//
        // $in of one value and $eq are two equalities on the path
        new Document("a", new Document("$in", List.of(7)).append("$eq", 7)),//
        // two equalities, whatever the regular expression that goes with them
        new Document("a", new Document("$all", List.of(5, 6, new BsonRegularExpression("x", ""))))))
    {
      final Document reply = update("C11", List.of(statement(filter, new Document("$set", new Document("c", 1)))), true);
      assertThat(reply.get("n")).as("%s", filter).isEqualTo(0);
      assertThat(reply.containsKey("upserted")).as("%s", filter).isFalse();
      assertThat(writeErrors(reply)).as("%s", filter).hasSize(1);
      final Document error = writeErrors(reply).getFirst();
      assertThat(error.get("index")).isEqualTo(0);
      assertThat(error.get("code")).isEqualTo(54);
      assertThat(error.get("codeName")).isEqualTo("NotSingleValueField");
      assertThat((String) error.get("errmsg")).startsWith("cannot infer query fields to set");
    }
    assertThat(database.getSchema().existsType("C11") ? findAll("C11") : List.of()).isEmpty();
  }

  @Test
  void theRefusedStatementIsReportedAtItsIndexAndTheOthersAreKeptLikeMongod() {
    final Document refused = statement(new Document("a", 1).append("a.b", 2), new Document("$set", new Document("c", 1)));
    final Document before = statement(new Document("k", 1), new Document("$set", new Document("c", 1)));
    final Document after = statement(new Document("k", 2), new Document("$set", new Document("c", 1)));

    final Document unordered = update("C12", List.of(before, refused, after), false);
    assertThat(writeErrors(unordered)).hasSize(1);
    assertThat(writeErrors(unordered).getFirst().get("index")).isEqualTo(1);
    assertThat(writeErrors(unordered).getFirst().get("code")).isEqualTo(54);
    assertThat(unordered.get("n")).isEqualTo(2);
    assertThat(findAll("C12")).hasSize(2);

    final Document ordered = update("C13", List.of(before, refused, after), true);
    assertThat(writeErrors(ordered)).hasSize(1);
    assertThat(writeErrors(ordered).getFirst().get("index")).isEqualTo(1);
    assertThat(ordered.get("n")).as("the statement after the refused one is not run").isEqualTo(1);
    assertThat(findAll("C13")).hasSize(1);
  }

  @Test
  void aFilterThatCannotBeInferredStillUpdatesTheDocumentItMatches() {
    // mongod infers the fields of the inserted document only when nothing matches: a document that matches is updated, whatever the
    // shape of the filter (a path matched twice is the refusal of an insert, not of the filter)
    upsert("C14", new Document("_id", 1),
        new Document("$set", new Document("tags", List.of("x", "y")).append("a", new Document("b", 1))));

    final Document[] filters = { //
        new Document("tags", new Document("$all", List.of("x", "y"))),//
        new Document("$and", List.of(new Document("tags", "x"), new Document("tags", "y"))),//
        new Document("a", new Document("b", 1)).append("a.b", 1) };
    for (int i = 0; i < filters.length; i++) {
      final Document reply = update("C14", List.of(statement(filters[i], new Document("$set", new Document("c", i + 1)))), true);
      assertThat(reply.get("n")).as("%s", filters[i]).isEqualTo(1);
      assertThat(reply.containsKey("upserted")).as("%s", filters[i]).isFalse();
      assertThat(reply.containsKey("writeErrors")).as("%s", filters[i]).isFalse();
    }
    final List<Document> stored = findAll("C14");
    assertThat(stored).hasSize(1);
    assertThat(stored.getFirst().get("c")).isEqualTo(3);
  }

  @Test
  void anAllOfElemMatchOperatorDocumentsSeedsNothingAndIsNotRefused() {
    final Document elemMatch = new Document("$elemMatch", new Document("size", "M"));
    final Document otherElemMatch = new Document("$elemMatch", new Document("size", "L"));

    // the documented pattern with several $elemMatch: none of them is an equality
    final Document several = update("C15",
        List.of(statement(new Document("qty", new Document("$all", List.of(elemMatch, otherElemMatch))),
            new Document("$set", new Document("c", 1)))), true);
    assertThat(several.containsKey("writeErrors")).isFalse();
    assertThat(several.get("upserted")).isNotNull();
    assertThat(findAll("C15").getFirst()).doesNotContainKey("qty");

    // and a single one is not stored as an embedded document with a "$elemMatch" key
    upsert("C16", new Document("qty", new Document("$all", List.of(elemMatch))), new Document("$set", new Document("c", 1)));
    assertThat(findAll("C16").getFirst()).doesNotContainKey("qty");

    // and when a document matches, the upsert is an update of it
    upsert("C17", new Document("_id", 1),
        new Document("$set", new Document("qty", List.of(new Document("size", "M"), new Document("size", "L")))));
    final Document matched = update("C17",
        List.of(statement(new Document("qty", new Document("$all", List.of(elemMatch, otherElemMatch))),
            new Document("$set", new Document("c", 1)))), true);
    assertThat(matched.get("n")).isEqualTo(1);
    assertThat(matched.containsKey("upserted")).isFalse();
    assertThat(matched.containsKey("writeErrors")).isFalse();
    assertThat(findAll("C17")).hasSize(1);
  }

  @Test
  void aRegularExpressionAmongTheAllValuesIsNotAnEqualityAndTheOtherValueIsSeeded() {
    upsert("C18", new Document("a", new Document("$all", List.of(5, new BsonRegularExpression("x", "")))),
        new Document("$set", new Document("c", 1)));
    assertThat(findAll("C18").getFirst()).containsEntry("a", 5);

    // a pattern alone, in $all, in $in or as the value, is something to match: it infers nothing
    int i = 0;
    for (final Object pattern : List.of(new BsonRegularExpression("x", ""), new Document("$all", List.of(new BsonRegularExpression("x", ""))),
        new Document("$in", List.of(new BsonRegularExpression("x", ""))))) {
      final String collection = "C19_" + i++;
      upsert(collection, new Document("a", pattern), new Document("$set", new Document("c", 1)));
      assertThat(findAll(collection).getFirst()).as("%s", pattern).doesNotContainKey("a");
    }
  }

  @Test
  void aRegularExpressionUnderEqIsAPatternAndIsNotSeeded() {
    // this plugin's matcher reads a regular expression operand, under $eq too, as a pattern to match, never as a value to compare, so
    // a regular expression it stored from the filter would not be matched by the filter that produced it: the next run would insert
    // again, the very duplicate this issue is about. It stays what it has always been, left out of the inserted document
    final Document filter = new Document("a", new Document("$eq", new BsonRegularExpression("x", "i")));
    upsert("C20", filter, new Document("$set", new Document("c", 1)));
    assertThat(findAll("C20").getFirst()).doesNotContainKey("a").containsEntry("c", 1);
  }

  @Test
  void aNumericPathSegmentMakesAnEmbeddedDocumentLikeMongod() {
    upsert("C21", new Document("tags.0", "x"), new Document("$set", new Document("c", 1)));
    assertThat(findAll("C21").getFirst().get("tags")).isEqualTo(new Document("0", "x"));
  }

  @Test
  void aDottedPathOfTheIdSeedsAnEmbeddedDocumentIdLikeMongod() {
    final Document first = upsert("C22", new Document("_id.k", 2), new Document("$inc", new Document("hits", 1)));
    final Document second = upsert("C22", new Document("_id.k", 2), new Document("$inc", new Document("hits", 1)));

    assertThat(((Document) ((List<?>) first.get("upserted")).getFirst()).get("_id")).isEqualTo(new Document("k", 2));
    assertThat(second.get("upserted")).isNull();
    assertThat(findAll("C22")).hasSize(1);
    assertThat(findAll("C22").getFirst().get("_id")).isEqualTo(new Document("k", 2));
    assertThat(findAll("C22").getFirst().get("hits")).isEqualTo(2);
  }

  private Document upsert(final String collection, final Document filter, final Document u) {
    final Document reply = update(collection, List.of(statement(filter, u)), true);
    assertThat(reply.containsKey("writeErrors")).as("%s", reply).isFalse();
    return reply;
  }

  private static Document statement(final Document filter, final Document u) {
    return new Document("q", filter).append("u", u).append("upsert", true);
  }

  private Document update(final String collection, final List<Document> statements, final boolean ordered) {
    return wrapper.handleCommand(null, "update",
        new Document("update", collection).append("updates", statements).append("ordered", ordered), null, null);
  }

  private static List<Document> writeErrors(final Document reply) {
    final List<Document> errors = new ArrayList<>();
    if (reply.get("writeErrors") instanceof List<?> list)
      for (final Object error : list)
        errors.add((Document) error);
    return errors;
  }

  private List<Document> findAll(final String collection) {
    final Document reply = wrapper.handleCommand(null, "find", new Document("find", collection).append("filter", new Document()), null,
        null);
    final List<Document> documents = new ArrayList<>();
    for (final Object document : (List<?>) ((Document) reply.get("cursor")).get("firstBatch"))
      documents.add((Document) document);
    return documents;
  }
}
