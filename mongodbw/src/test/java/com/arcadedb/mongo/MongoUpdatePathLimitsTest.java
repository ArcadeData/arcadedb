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
import de.bwaldvogel.mongo.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The paths an update names are checked as a whole before any record is read: a path reaches at most as deep as the documents
 * {@code mongod} stores (100 levels), and two paths of one statement conflict when one is the other or lies below it, whatever the
 * characters that sort between them. The check and the application of the path take time and memory proportional to the path.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class MongoUpdatePathLimitsTest {
  private static final String DB_PATH   = "./target/databases/MongoUpdatePathLimitsTest";
  private static final int    MAX_DEPTH = 100;
  private static final int    BAD_VALUE = 2;
  private static final int    CONFLICT  = 40;

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
  void aPathOfTheMaximumDepthIsSetIncrementedAndUnset() {
    final String path = path(MAX_DEPTH, "s");
    insert("deepest", new Document("_id", 1));

    final Document set = update("deepest", statement(new Document("_id", 1), new Document("$set", new Document(path, 7))));
    assertThat(set.containsKey("writeErrors")).as("%s", set).isFalse();
    assertThat(set.get("nModified")).isEqualTo(1);
    assertThat(leaf(findAll("deepest").getFirst(), MAX_DEPTH)).isEqualTo(7);

    final Document inc = update("deepest", statement(new Document("_id", 1), new Document("$inc", new Document(path, 1))));
    assertThat(inc.containsKey("writeErrors")).as("%s", inc).isFalse();
    assertThat(leaf(findAll("deepest").getFirst(), MAX_DEPTH)).isEqualTo(8);

    final Document unset = update("deepest", statement(new Document("_id", 1), new Document("$unset", new Document(path, ""))));
    assertThat(unset.containsKey("writeErrors")).as("%s", unset).isFalse();
    assertThat(unset.get("nModified")).isEqualTo(1);
    assertThat(leaf(findAll("deepest").getFirst(), MAX_DEPTH)).isNull();
  }

  @Test
  void aSegmentOfAnySizeIsStoredAsGiven() {
    final String segment = "x".repeat(2_000);
    final String path = path(MAX_DEPTH, segment);
    insert("wide", new Document("_id", 1));

    final Document set = update("wide", statement(new Document("_id", 1), new Document("$set", new Document(path, "v"))));
    assertThat(set.containsKey("writeErrors")).as("%s", set).isFalse();

    Object current = findAll("wide").getFirst();
    for (int i = 0; i < MAX_DEPTH; i++)
      current = ((Map<?, ?>) current).get(segment);
    assertThat(current).isEqualTo("v");
  }

  @Test
  void aPathBeyondTheMaximumDepthIsRefusedAndNothingChanges() {
    final Document original = new Document("_id", 1).append("n", 1);
    for (final int depth : List.of(MAX_DEPTH + 1, 5_000))
      for (final String operator : List.of("$set", "$unset", "$inc")) {
        final String collection = "beyond" + depth + operator.substring(1);
        insert(collection, original);

        final Document reply = update(collection,
            statement(new Document("_id", 1), new Document(operator, new Document(path(depth, "s"), operator.equals("$unset") ? "" : 1))));

        assertThat(writeErrors(reply)).as("%s of %d segments", operator, depth).hasSize(1);
        assertThat(writeErrors(reply).getFirst().get("index")).isEqualTo(0);
        assertThat(writeErrors(reply).getFirst().get("code")).isEqualTo(BAD_VALUE);
        assertThat(writeErrors(reply).getFirst().get("codeName")).isEqualTo("BadValue");
        assertThat(reply.get("nModified")).isEqualTo(0);
        assertThat(findAll(collection)).containsExactly(original);
      }
  }

  @Test
  void aPathBeyondTheMaximumDepthIsRefusedWhateverTheCollectionOrTheFilterMatches() {
    final String path = path(MAX_DEPTH + 1, "s");

    // no such collection: the statement is refused all the same, and the collection is not created
    final Document missing = update("nothing", statement(new Document("_id", 1), new Document("$set", new Document(path, 1))));
    assertThat(writeErrors(missing)).hasSize(1);
    assertThat(writeErrors(missing).getFirst().get("code")).isEqualTo(BAD_VALUE);
    assertThat(database.getSchema().existsType("nothing")).isFalse();

    // no such document
    insert("nomatch", new Document("_id", 1));
    final Document none = update("nomatch", statement(new Document("_id", 99), new Document("$set", new Document(path, 1))));
    assertThat(writeErrors(none)).hasSize(1);
    assertThat(writeErrors(none).getFirst().get("code")).isEqualTo(BAD_VALUE);
  }

  @Test
  void aRefusedStatementIsReportedAtItsIndexAndTheOthersAreKept() {
    insert("batch", new Document("_id", 1));
    insert("batch", new Document("_id", 2));
    insert("batch", new Document("_id", 3));
    final Document before = statement(new Document("_id", 1), new Document("$set", new Document("c", 1)));
    final Document refused = statement(new Document("_id", 2), new Document("$set", new Document(path(MAX_DEPTH + 1, "s"), 1)));
    final Document after = statement(new Document("_id", 3), new Document("$set", new Document("c", 1)));

    final Document reply = update("batch", List.of(before, refused, after), false);

    assertThat(writeErrors(reply)).hasSize(1);
    assertThat(writeErrors(reply).getFirst().get("index")).isEqualTo(1);
    assertThat(writeErrors(reply).getFirst().get("code")).isEqualTo(BAD_VALUE);
    assertThat(reply.get("n")).isEqualTo(2);
    assertThat(reply.get("nModified")).isEqualTo(2);
  }

  @Test
  void anUpsertFilterPathBeyondTheMaximumDepthInsertsNothing() {
    final Document refused = statement(new Document(path(MAX_DEPTH + 1, "s"), 1), new Document("$set", new Document("c", 1)));
    refused.put("upsert", true);

    final Document reply = update("seeded", refused);

    assertThat(writeErrors(reply)).hasSize(1);
    assertThat(writeErrors(reply).getFirst().get("code")).isEqualTo(BAD_VALUE);
    assertThat(reply.containsKey("upserted")).isFalse();
    assertThat(database.getSchema().existsType("seeded") ? findAll("seeded") : List.of()).isEmpty();

    // the deepest path is seeded as an embedded document
    final Document accepted = statement(new Document(path(MAX_DEPTH, "s"), 1), new Document("$set", new Document("c", 1)));
    accepted.put("upsert", true);
    final Document inserted = update("seeded", accepted);
    assertThat(inserted.containsKey("writeErrors")).as("%s", inserted).isFalse();
    assertThat(inserted.get("upserted")).isNotNull();
    assertThat(leaf(findAll("seeded").getFirst(), MAX_DEPTH)).isEqualTo(1);
  }

  @Test
  void aPathAndOneBelowItConflictWhateverSortsBetweenThem() {
    insert("conflicts", new Document("_id", 1));
    final Document original = findAll("conflicts").getFirst();

    // "a-b" and "a0" sort around "a.b" by character: the conflict is still found
    for (final Document set : List.of(//
        new Document("a", 1).append("a-b", 1).append("a.b", 1),//
        new Document("a.b", 1).append("a-b", 1).append("a0", 1).append("a", 1),//
        new Document("a.b.c", 1).append("a.b", 1),//
        new Document("a.b", 1).append("a.b.c", 1),//
        new Document("a b", 1).append("a!", 1).append("a.", 1).append("a", 1)))
    {
      final Document reply = update("conflicts", statement(new Document("_id", 1), new Document("$set", set)));
      assertThat(writeErrors(reply)).as("%s", set).hasSize(1);
      assertThat(writeErrors(reply).getFirst().get("code")).as("%s", set).isEqualTo(CONFLICT);
      assertThat(findAll("conflicts")).containsExactly(original);
    }

    // across operators, in either order
    for (final Document operators : List.of(//
        new Document("$set", new Document("x.y", 1)).append("$unset", new Document("x", "")),//
        new Document("$unset", new Document("x", "")).append("$inc", new Document("x.y.z", 1)),//
        new Document("$inc", new Document("x", 1)).append("$set", new Document("x", 1))))
    {
      final Document reply = update("conflicts", statement(new Document("_id", 1), operators));
      assertThat(writeErrors(reply)).as("%s", operators).hasSize(1);
      assertThat(writeErrors(reply).getFirst().get("code")).as("%s", operators).isEqualTo(CONFLICT);
    }
    assertThat(findAll("conflicts")).containsExactly(original);
  }

  @Test
  void pathsThatOnlyShareCharactersAreNotAConflict() {
    insert("siblings", new Document("_id", 1));

    final Document set = new Document("a-b", 1).append("a.b", 2).append("ab", 3).append("a0", 4).append("a.c", 5).append("a b", 6);
    final Document reply = update("siblings", statement(new Document("_id", 1), new Document("$set", set)));

    assertThat(reply.containsKey("writeErrors")).as("%s", reply).isFalse();
    final Document stored = findAll("siblings").getFirst();
    assertThat(stored).containsEntry("a-b", 1).containsEntry("ab", 3).containsEntry("a0", 4).containsEntry("a b", 6);
    assertThat(stored.get("a")).isEqualTo(new Document("b", 2).append("c", 5));
  }

  @Test
  void manyPathsAreCheckedTogether() {
    insert("many", new Document("_id", 1));
    final Document set = new Document();
    for (int i = 0; i < 2_000; i++)
      set.put("f" + i + ".x", i);

    final Document applied = update("many", statement(new Document("_id", 1), new Document("$set", set)));
    assertThat(applied.containsKey("writeErrors")).as("%s", applied).isFalse();
    final Document stored = findAll("many").getFirst();
    assertThat(stored.get("f1999")).isEqualTo(new Document("x", 1999));

    // a path that is one of those named earlier, however far apart they are
    set.put("f0", 1);
    final Document refused = update("many", statement(new Document("_id", 1), new Document("$set", set)));
    assertThat(writeErrors(refused)).hasSize(1);
    assertThat(writeErrors(refused).getFirst().get("code")).isEqualTo(CONFLICT);
    assertThat(findAll("many")).containsExactly(stored);
  }

  private static String path(final int segments, final String segment) {
    return String.join(".", Collections.nCopies(segments, segment));
  }

  /** The value {@code depth} levels down, through the fields named {@code s}: null where there is none. */
  private static Object leaf(final Document document, final int depth) {
    Object current = document;
    for (int i = 0; i < depth && current != null; i++)
      current = current instanceof Map<?, ?> map ? map.get("s") : null;
    return current;
  }

  /** Creates a document by an upsert of its {@code _id}, setting the other fields. */
  private void insert(final String collection, final Document document) {
    final Document fields = new Document();
    for (final Map.Entry<String, Object> field : document.entrySet())
      if (!"_id".equals(field.getKey()))
        fields.put(field.getKey(), field.getValue());
    final Document statement = statement(new Document("_id", document.get("_id")), new Document("$set", fields));
    statement.put("upsert", true);
    final Document reply = update(collection, statement);
    assertThat(reply.containsKey("writeErrors")).as("%s", reply).isFalse();
    assertThat(reply.get("n")).isEqualTo(1);
  }

  private static Document statement(final Document filter, final Document u) {
    return new Document("q", filter).append("u", u);
  }

  private Document update(final String collection, final Document statement) {
    return update(collection, List.of(statement), true);
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
