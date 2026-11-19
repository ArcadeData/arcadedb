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
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.TypeIndexBuilder;
import com.arcadedb.utility.FileUtils;
import de.bwaldvogel.mongo.bson.BsonRegularExpression;
import de.bwaldvogel.mongo.bson.Decimal128;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.ObjectId;
import de.bwaldvogel.mongo.exception.ErrorCode;
import de.bwaldvogel.mongo.exception.MongoServerError;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12073: values of different BSON types were one key. The unique index on {@code _id} (and a unique index of
 * {@code createIndexes}) converted every key to one type, so {@code _id} 1 and "1", true and "true", 1.5 and "1.5" collided; an
 * ObjectId {@code _id} and the string with the same hex were one value to the index, to filters, to updates and deletes; the
 * matcher saw an ObjectId {@code _id} as a string ({@code $type}, regular expressions and string ranges); an ObjectId equality on
 * another field matched the string with the same hex; a unique index let several documents without the field in; and a
 * Decimal128 NaN, infinity or negative zero could not be stored. The expectations are mongod 7's answers.
 */
class Issue12073MongoTypedKeysTest {
  private static final String   DB_PATH = "./target/databases/issue12073";
  private static final String   HEX     = "5f0000000000000000000001";
  private static final ObjectId OID     = new ObjectId(HEX);

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
  void idsOfDifferentTypesAreDifferentKeysAndEqualNumbersAreOne() {
    insert("Ids", new Document("_id", 1), new Document("_id", "1"), new Document("_id", true), new Document("_id", "true"),
        new Document("_id", 1.5), new Document("_id", "1.5"));
    assertThat(find("Ids", new Document())).hasSize(6);

    for (final Object same : List.of(1L, 1.0, new Decimal128(new BigDecimal("1.00"))))
      assertThatThrownBy(() -> insert("Ids", new Document("_id", same))).as("%s", same).isInstanceOf(DuplicatedKeyException.class);
    assertThat(find("Ids", new Document("_id", "1"))).hasSize(1);
    assertThat(find("Ids", new Document("_id", 1L))).extracting(d -> d.get("_id")).containsExactly(1);
  }

  @Test
  void anObjectIdAndTheStringWithItsHexAreTwoIds() {
    insert("Oids", new Document("_id", OID).append("k", "objectid"), new Document("_id", "abc").append("k", "string"),
        new Document("_id", 3).append("k", "int"));
    insert("Oids", new Document("_id", HEX).append("k", "hex"));

    assertThat(ks("Oids", new Document("_id", HEX))).containsExactly("hex");
    assertThat(ks("Oids", new Document("_id", OID))).containsExactly("objectid");
    assertThat(ks("Oids", new Document("_id", new Document("$type", "objectId")))).containsExactly("objectid");
    assertThat(ks("Oids", new Document("_id", new Document("$type", "string")))).containsExactlyInAnyOrder("string", "hex");
    assertThat(ks("Oids", new Document("_id", new BsonRegularExpression("^5f", "")))).containsExactly("hex");
    assertThat(ks("Oids", new Document("_id", new Document("$lt", "z")))).containsExactlyInAnyOrder("string", "hex");
    assertThat(ks("Oids", new Document("_id", new Document("$gt", new ObjectId("000000000000000000000000"))))).containsExactly(
        "objectid");
    assertThat(find("Oids", new Document("_id", HEX)).getFirst().get("_id")).isEqualTo(HEX);
    assertThat(find("Oids", new Document("_id", OID)).getFirst().get("_id")).isEqualTo(OID);

    final Document updated = command("update", "Oids",
        new Document("updates", List.of(new Document("q", new Document("_id", HEX)).append("u", new Document("$set", new Document("k", "changed"))))));
    assertThat(updated.get("n")).isEqualTo(1);
    assertThat(ks("Oids", new Document("_id", OID))).containsExactly("objectid");

    final Document deleted = command("delete", "Oids",
        new Document("deletes", List.of(new Document("q", new Document("_id", HEX)).append("limit", 0))));
    assertThat(deleted.get("n")).isEqualTo(1);
    assertThat(ks("Oids", new Document())).containsExactlyInAnyOrder("objectid", "string", "int");
  }

  @Test
  void anObjectIdEqualityOnAnotherFieldDoesNotMatchTheString() {
    insert("Refs", new Document("_id", 1).append("o", OID), new Document("_id", 2).append("o", HEX));
    assertThat(find("Refs", new Document("o", OID))).extracting(d -> d.get("_id")).containsExactly(1);
    assertThat(find("Refs", new Document("o", new Document("$in", List.of(OID))))).extracting(d -> d.get("_id")).containsExactly(1);
    assertThat(find("Refs", new Document("o", HEX))).extracting(d -> d.get("_id")).containsExactly(2);
  }

  @Test
  void aUniqueIndexComparesTypedValuesAndTakesAMissingFieldAsNull() {
    insert("Uniq", new Document("_id", 1).append("a", 1));
    command("createIndexes", "Uniq", new Document("indexes", List.of(new Document("key", new Document("a", 1)).append("name", "a_1")
        .append("unique", true))));

    insert("Uniq", new Document("_id", 2).append("a", "1"), new Document("_id", 3).append("a", true), new Document("_id", 4).append("a", "true"));
    assertThatThrownBy(() -> insert("Uniq", new Document("_id", 5).append("a", 1.0))).isInstanceOf(DuplicatedKeyException.class);

    insert("Uniq", new Document("_id", 6));
    assertThatThrownBy(() -> insert("Uniq", new Document("_id", 7))).isInstanceOf(DuplicatedKeyException.class);
    assertThatThrownBy(() -> insert("Uniq", new Document("_id", 8).append("a", null))).isInstanceOf(DuplicatedKeyException.class);

    // an update that writes the value another document holds is refused, with or without a filter; one that frees it is not. The
    // refusal is the statement's, answered as a write error (issue #11502)
    assertThat(writeErrorCodes(command("update", "Uniq", new Document("updates",
        List.of(new Document("q", new Document("_id", 2)).append("u", new Document("$set", new Document("a", 1))))))))
        .containsExactly(ErrorCode.DuplicateKey.getValue());
    command("update", "Uniq", new Document("updates",
        List.of(new Document("q", new Document("_id", 1)).append("u", new Document("$set", new Document("a", 10))))));
    insert("Uniq", new Document("_id", 9).append("a", 1));
    assertThat(find("Uniq", new Document())).hasSize(6);
  }

  @Test
  void aCollectionWithTheUniqueIndexOfThePlainIdGetsTheKeysOnItsNextInsert() {
    // what the plugin built before #12073: a unique index on _id itself, with string keys for a mix of kinds
    database.getSchema().createDocumentType("Legacy");
    final TypeIndexBuilder builder = database.getSchema().buildTypeIndex("Legacy", new String[] { "_id" });
    builder.withType(Schema.INDEX_TYPE.LSM_TREE);
    builder.withUnique(true);
    builder.withDefaultKeyTypesForUndeclaredProperties(new Type[] { Type.STRING });
    builder.create();
    database.transaction(() -> {
      database.newDocument("Legacy").set("_id", 1).set("k", "int").save();
      database.newDocument("Legacy").set("_id", HEX).set("k", "objectid").save();
    });

    insert("Legacy", new Document("_id", "1").append("k", "string"));

    final List<String> indexed = new ArrayList<>();
    for (final TypeIndex index : database.getSchema().getType("Legacy").getAllIndexes(false))
      indexed.add(index.getPropertyNames().getFirst());
    assertThat(indexed).containsExactly(MongoKeys.ID_KEY);
    assertThat(ks("Legacy", new Document("_id", 1))).containsExactly("int");
    assertThat(ks("Legacy", new Document("_id", "1"))).containsExactly("string");
    // a bare hex _id stored before #12073 is an ObjectId, as it always read back
    assertThat(ks("Legacy", new Document("_id", OID))).containsExactly("objectid");
    assertThatThrownBy(() -> insert("Legacy", new Document("_id", OID))).isInstanceOf(DuplicatedKeyException.class);
  }

  @Test
  void theDerivedKeysAreNotFieldsOfTheDocument() {
    insert("Hidden", new Document("_id", 1).append("a", 2));
    assertThat(find("Hidden", new Document()).getFirst().keySet()).containsExactly("_id", "a");
    assertThatThrownBy(() -> insert("Hidden", new Document("_id", 2).append(MongoKeys.ID_KEY, "x"))).isInstanceOf(MongoServerError.class);
  }

  @Test
  void aDecimal128NaNInfinityOrNegativeZeroIsStored() {
    final List<Decimal128> values = List.of(Decimal128.NaN, Decimal128.NEGATIVE_INFINITY, Decimal128.POSITIVE_INFINITY,
        Decimal128.NEGATIVE_ZERO);
    for (int i = 0; i < values.size(); i++)
      insert("Decimals", new Document("_id", i).append("d", values.get(i)));
    for (int i = 0; i < values.size(); i++)
      assertThat(find("Decimals", new Document("_id", i)).getFirst().get("d")).isEqualTo(values.get(i));
  }

  @Test
  void anIncrementOfAStoredSpecialDecimalIsANumber() {
    insert("Incs", new Document("_id", 1).append("d", Decimal128.NEGATIVE_ZERO), new Document("_id", 2).append("d", Decimal128.NaN),
        new Document("_id", 3).append("d", Decimal128.POSITIVE_INFINITY));
    for (int id = 1; id <= 3; id++)
      command("update", "Incs", new Document("updates",
          List.of(new Document("q", new Document("_id", id)).append("u", new Document("$inc", new Document("d", 1))))));
    assertThat(find("Incs", new Document("_id", 1)).getFirst().get("d")).isEqualTo(new Decimal128(BigDecimal.ONE));
    assertThat(find("Incs", new Document("_id", 2)).getFirst().get("d")).isEqualTo(Decimal128.NaN);
    assertThat(find("Incs", new Document("_id", 3)).getFirst().get("d")).isEqualTo(Decimal128.POSITIVE_INFINITY);
  }

  @Test
  void anUpdateWithoutAFilterCannotWriteAKeyProperty() {
    insert("Reserved", new Document("_id", 1).append("a", 1));
    for (final String op : List.of("$set", "$unset", "$inc"))
      assertThat(writeErrorCodes(command("update", "Reserved", new Document("updates", List.of(new Document("q", new Document()).append("u",
          new Document(op, new Document(MongoKeys.ID_KEY, 5)))))))).as(op).containsExactly(ErrorCode.BadValue.getValue());
    assertThatThrownBy(() -> insert("Reserved", new Document("_id", 1))).isInstanceOf(DuplicatedKeyException.class);
  }

  @Test
  void aUniqueIndexOnAFieldNamedWithACommaIsOnThatField() {
    command("createIndexes", "Commas", new Document("indexes", List.of(new Document("key", new Document("a,b", 1)).append("name", "ab_1")
        .append("unique", true))));
    insert("Commas", new Document("_id", 1).append("a,b", 1), new Document("_id", 2).append("a,b", 2));
    assertThatThrownBy(() -> insert("Commas", new Document("_id", 3).append("a,b", 1))).isInstanceOf(DuplicatedKeyException.class);
    assertThat(MongoKeys.pathsOf(MongoKeys.propertyOf(new String[] { "a,b" }))).containsExactly("a,b");
    assertThat(MongoKeys.pathsOf(MongoKeys.propertyOf(new String[] { "a\\", "b" }))).containsExactly("a\\", "b");
    assertThat(MongoKeys.propertyOf(new String[] { "a,b" })).isNotEqualTo(MongoKeys.propertyOf(new String[] { "a", "b" }));
  }

  @Test
  void aDateIsOneKeyWhetherItIsADateOrAnInstant() {
    final Date date = new Date(1_700_000_000_123L);
    assertThat(MongoKeys.key(date.toInstant())).isEqualTo(MongoKeys.key(date));
  }

  @Test
  void aRecordWrittenThroughSqlHasNoKeySoAnIdFilterDoesNotSeeIt() {
    // the documented gap of MongoKeys: only the plugin writes the derived keys, and an _id filter selects through the key index
    insert("Mixed", new Document("_id", 1).append("k", "mongo"));
    database.transaction(() -> database.newDocument("Mixed").set("_id", 2).set("k", "sql").save());

    assertThat(ks("Mixed", new Document())).containsExactlyInAnyOrder("mongo", "sql");
    assertThat(ks("Mixed", new Document("_id", 1))).containsExactly("mongo");
    assertThat(ks("Mixed", new Document("_id", 2))).isEmpty();
    // nor is it checked for uniqueness
    insert("Mixed", new Document("_id", 2).append("k", "mongo2"));
    assertThat(ks("Mixed", new Document("_id", 2))).containsExactly("mongo2");
  }

  @Test
  void anArrayIsOneKeyNotOneKeyPerElement() {
    // the documented gap of MongoKeys: mongod's multikey unique index refuses the second document, this plugin accepts it
    command("createIndexes", "Arr", new Document("indexes", List.of(new Document("key", new Document("a", 1)).append("name", "a_1")
        .append("unique", true))));
    insert("Arr", new Document("_id", 1).append("a", List.of(1, 2)));
    insert("Arr", new Document("_id", 2).append("a", 2));
    assertThatThrownBy(() -> insert("Arr", new Document("_id", 3).append("a", List.of(1, 2)))).isInstanceOf(DuplicatedKeyException.class);
    assertThat(find("Arr", new Document())).hasSize(2);
  }

  @Test
  void aFailedKeyIndexBuildLeavesAnIndexThatExistedBefore() {
    database.getSchema().createDocumentType("Kept");
    final TypeIndexBuilder builder = database.getSchema().buildTypeIndex("Kept", new String[] { "p" });
    builder.withType(Schema.INDEX_TYPE.LSM_TREE);
    builder.withUnique(false);
    builder.withDefaultKeyTypesForUndeclaredProperties(new Type[] { Type.STRING });
    builder.create();
    database.transaction(() -> {
      database.newDocument("Kept").set("p", "x").save();
      database.newDocument("Kept").set("p", "x").save();
    });

    // the engine refuses a unique index next to the non-unique one: the cleanup must not drop the index it did not create
    assertThatThrownBy(() -> MongoDBCollectionWrapper.createKeyIndex(database, "Kept", "p")).isInstanceOf(IllegalArgumentException.class);
    final List<String> left = new ArrayList<>();
    for (final TypeIndex index : database.getSchema().getType("Kept").getAllIndexes(false))
      left.add(index.getPropertyNames().getFirst());
    assertThat(left).containsExactly("p");
  }

  private void insert(final String collection, final Document... documents) {
    database.getSchema().getOrCreateDocumentType(collection);
    new MongoDBCollectionWrapper(database, collection).insertDocuments(List.of(documents));
  }

  private Document command(final String command, final String collection, final Document arguments) {
    final Document document = new Document(command, collection);
    document.putAll(arguments);
    return wrapper.handleCommand(null, command, document, null, null);
  }

  private List<Document> find(final String collection, final Document filter) {
    final Document reply = command("find", collection, new Document("filter", filter));
    final List<Document> documents = new ArrayList<>();
    for (final Object document : (List<?>) ((Document) reply.get("cursor")).get("firstBatch"))
      documents.add((Document) document);
    return documents;
  }

  private List<Object> ks(final String collection, final Document filter) {
    final List<Object> ks = new ArrayList<>();
    for (final Document document : find(collection, filter))
      ks.add(document.get("k"));
    return ks;
  }

  /** The codes of the statements an update command refused, which it answers as write errors (issue #11502). */
  private static List<Integer> writeErrorCodes(final Document reply) {
    final List<Integer> codes = new ArrayList<>();
    if (reply.get("writeErrors") instanceof List<?> errors)
      for (final Object error : errors)
        codes.add(((Number) ((Document) error).get("code")).intValue());
    return codes;
  }
}
