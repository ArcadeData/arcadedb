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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12070: an update without a filter was one SQL {@code UPDATE ... SET f += v} for {@code $inc}, and SQL {@code +=} is not
 * {@code $inc}: it turned the string "abc" into "abc1", true into "true1", appended to a list and removed a date, answering
 * {@code n=1} where mongod refuses with TypeMismatch (14) and changes nothing. The record path, which every filtered update took,
 * refused those but accepted three more updates mongod refuses: a long that overflows (stored as a double instead of error 2), a
 * null amount or a stored null (error 14), and two operators writing the same path (error 40).
 */
class Issue12070MongoIncTypeTest {
  private static final String DB_PATH = "./target/databases/issue12070";

  private Database               database;
  private MongoDBDatabaseWrapper wrapper;

  @BeforeEach
  void beginTest() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
    database.getSchema().createDocumentType("Coll");
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
  void incWithoutAFilterRefusesEveryStoredValueThatIsNotANumberAndChangesNothing() {
    final Date date = new Date(1_577_836_800_000L);
    for (final Object stored : List.of("abc", true, List.of(1), date, Map.of("x", 1))) {
      store(stored);
      final Object before = storedS();
      assertThat(before).as("stored %s", stored).isNotNull();
      for (final Document filter : List.of(new Document(), new Document("_id", 1)))
        assertThatThrownBy(() -> update(filter, new Document("$inc", new Document("s", 1)))).as("stored %s, filter %s", stored, filter)
            .isInstanceOf(MongoServerError.class).satisfies(e -> assertThat(((MongoServerError) e).getCode()).isEqualTo(14));
      assertThat(storedS()).as("stored %s", stored).isEqualTo(before);
    }
  }

  @Test
  void incOfAStoredNullIsRefusedWhileAMissingFieldTakesTheAmount() {
    store(null);
    for (final Document filter : List.of(new Document(), new Document("_id", 1)))
      assertThatThrownBy(() -> update(filter, new Document("$inc", new Document("s", 1)))).isInstanceOf(MongoServerError.class)
          .satisfies(e -> assertThat(((MongoServerError) e).getCode()).isEqualTo(14));

    update(new Document(), new Document("$inc", new Document("other", 5).append("nested.deep", 2L)));
    try (final ResultSet rs = database.query("sql", "SELECT other, nested FROM Coll")) {
      final Result row = rs.next();
      assertThat(row.<Object>getProperty("other")).isEqualTo(5);
      assertThat(row.<Map<String, Object>>getProperty("nested")).containsEntry("deep", 2L);
    }
  }

  @Test
  void anAmountThatIsNotANumberIsRefusedEvenWhenNothingMatches() {
    store(1);
    for (final Object amount : Arrays.asList(null, "x", List.of(1)))
      for (final Document filter : List.of(new Document(), new Document("_id", 99)))
        assertThatThrownBy(() -> update(filter, new Document("$inc", new Document("s", amount)))).as("amount %s", amount)
            .isInstanceOf(MongoServerError.class).satisfies(e -> assertThat(((MongoServerError) e).getCode()).isEqualTo(14));
    assertThat(storedS()).isEqualTo(1);
  }

  @Test
  void aLongThatOverflowsIsRefusedNotStoredAsADouble() {
    for (final long[] values : new long[][] { { Long.MAX_VALUE, 1 }, { Long.MIN_VALUE, -1 } }) {
      store(values[0]);
      for (final Document filter : List.of(new Document(), new Document("_id", 1)))
        assertThatThrownBy(() -> update(filter, new Document("$inc", new Document("s", values[1])))).isInstanceOf(MongoServerError.class)
            .satisfies(e -> assertThat(((MongoServerError) e).getCode()).isEqualTo(2));
      assertThat(storedS()).isEqualTo(values[0]);
    }
  }

  @Test
  void numbersStillAddLikeMongod() {
    store(Integer.MAX_VALUE);
    update(new Document(), new Document("$inc", new Document("s", 1)));
    assertThat(storedS()).isEqualTo(2_147_483_648L);

    store(1.5);
    update(new Document(), new Document("$inc", new Document("s", 2L)));
    assertThat(storedS()).isEqualTo(3.5);
  }

  @Test
  void twoOperatorsOrTwoFieldsWritingOnePathAreRefusedEvenWhenNothingMatches() {
    store(1);
    final List<Document> conflicting = List.of(//
        new Document("$set", new Document("s", 2)).append("$unset", new Document("s", "")),//
        new Document("$set", new Document("a", 2).append("a.b", 1)),//
        new Document("$set", new Document("a.b", 2)).append("$inc", new Document("a", 1)));
    for (final Document u : conflicting)
      for (final Document filter : List.of(new Document(), new Document("_id", 1), new Document("_id", 99)))
        assertThatThrownBy(() -> update(filter, u)).as("%s", u).isInstanceOf(MongoServerError.class)
            .satisfies(e -> assertThat(((MongoServerError) e).getCode()).isEqualTo(40));
    assertThat(storedS()).isEqualTo(1);

    // a field whose name only starts with another one's is no conflict
    update(new Document(), new Document("$set", new Document("ab", 2)).append("$inc", new Document("a", 1)));
    try (final ResultSet rs = database.query("sql", "SELECT a, ab FROM Coll")) {
      final Result row = rs.next();
      assertThat(row.<Object>getProperty("a")).isEqualTo(1);
      assertThat(row.<Object>getProperty("ab")).isEqualTo(2);
    }
  }

  private void store(final Object value) {
    database.transaction(() -> {
      database.command("sql", "DELETE FROM Coll");
      database.newDocument("Coll").set("_id", 1).set("s", value).save();
    });
  }

  private Object storedS() {
    try (final ResultSet rs = database.query("sql", "SELECT s FROM Coll")) {
      return rs.next().getProperty("s");
    }
  }

  private void update(final Document filter, final Document u) {
    wrapper.handleCommand(null, "update", new Document("update", "Coll").append("updates",
        List.of(new Document("q", filter).append("u", u).append("multi", true))), null, null);
  }
}
