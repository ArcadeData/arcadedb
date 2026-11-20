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
import com.arcadedb.event.BeforeRecordUpdateListener;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.TypeIndexBuilder;
import com.arcadedb.utility.FileUtils;
import de.bwaldvogel.mongo.bson.BinData;
import de.bwaldvogel.mongo.bson.BsonJavaScript;
import de.bwaldvogel.mongo.bson.BsonRegularExpression;
import de.bwaldvogel.mongo.bson.BsonTimestamp;
import de.bwaldvogel.mongo.bson.Decimal128;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.LegacyUUID;
import de.bwaldvogel.mongo.bson.MaxKey;
import de.bwaldvogel.mongo.bson.MinKey;
import de.bwaldvogel.mongo.bson.ObjectId;
import de.bwaldvogel.mongo.exception.ErrorCode;
import de.bwaldvogel.mongo.exception.MongoServerError;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

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
  void anIndexOnAKeyPropertyIsRefused() {
    // a client index on the property the plugin keeps its unique key in would make the plugin's own index impossible to build
    for (final boolean unique : new boolean[] { true, false })
      assertThatThrownBy(() -> command("createIndexes", "Reserved2", new Document("indexes",
          List.of(new Document("key", new Document(MongoKeys.ID_KEY, 1)).append("name", "reserved").append("unique", unique))))).isInstanceOf(
          MongoServerError.class);
    assertThat(database.getSchema().existsType("Reserved2")).isFalse();
  }

  @Test
  void anEmbeddedIdThatSpellsAnObjectIdStaysTheStringItWas() {
    // only the _id of the document is an ObjectId when it spells one: an _id inside an embedded document is a field like any other
    insert("Embedded", new Document("_id", 1).append("e", new Document("_id", HEX)), new Document("_id", 2).append("e", new Document("_id", OID)),
        new Document("_id", 3).append("l", List.of(new Document("_id", HEX))));

    assertThat(((Document) find("Embedded", new Document("_id", 1)).getFirst().get("e")).get("_id")).isEqualTo(HEX);
    assertThat(((Document) find("Embedded", new Document("_id", 2)).getFirst().get("e")).get("_id")).isEqualTo(OID);
    assertThat(((Document) ((List<?>) find("Embedded", new Document("_id", 3)).getFirst().get("l")).getFirst()).get("_id")).isEqualTo(HEX);

    // and the prefix of the derived keys is reserved for the fields of the document itself: inside an embedded document it is a name
    insert("Embedded", new Document("_id", 4).append("e", new Document(MongoKeys.PREFIX + "x", 1)));
    assertThat(find("Embedded", new Document("_id", 4)).getFirst().get("e")).isEqualTo(new Document(MongoKeys.PREFIX + "x", 1));
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

  @Test
  void issuingTheSameUniqueCreateIndexesAgainRewritesNoRecord() {
    // an ORM issues createIndexes at every start of the application: an index that exists must cost nothing
    insert("Again", new Document("_id", 1).append("a", 1), new Document("_id", 2).append("a", 2), new Document("_id", 3).append("a", 3));
    createUniqueIndex("Again", new Document("a", 1));

    final AtomicInteger updated = countUpdates("Again");
    createUniqueIndex("Again", new Document("a", 1));
    assertThat(updated.get()).isZero();
    assertThat(uniqueIndexProperties("Again")).containsExactlyInAnyOrder("$unique:a", MongoKeys.ID_KEY);
    assertThatThrownBy(() -> insert("Again", new Document("_id", 4).append("a", 2.0))).isInstanceOf(DuplicatedKeyException.class);
  }

  @Test
  void aBuildThatFailedOnDuplicatesCanBeRetriedAndRewritesNoKey() {
    insert("Retry", new Document("_id", 1).append("a", 1), new Document("_id", 2).append("a", 1), new Document("_id", 3).append("a", 3));
    assertThatThrownBy(() -> createUniqueIndex("Retry", new Document("a", 1))).isInstanceOf(RuntimeException.class);
    // the failed build leaves no index (the engine refused it) but the keys it wrote: they are what the retry finds
    assertThat(uniqueIndexProperties("Retry")).containsExactly(MongoKeys.ID_KEY);

    command("delete", "Retry", new Document("deletes", List.of(new Document("q", new Document("_id", 2)).append("limit", 0))));
    final AtomicInteger updated = countUpdates("Retry");
    createUniqueIndex("Retry", new Document("a", 1));
    assertThat(updated.get()).as("the keys the failed build wrote are not written again").isZero();
    assertThat(uniqueIndexProperties("Retry")).containsExactlyInAnyOrder("$unique:a", MongoKeys.ID_KEY);
    assertThatThrownBy(() -> insert("Retry", new Document("_id", 4).append("a", 1))).isInstanceOf(DuplicatedKeyException.class);
  }

  @Test
  void aUniqueIndexOnAPathThroughAnArrayKeysWhatItCollects() {
    createUniqueIndex("Items", new Document("items.sku", 1));
    insert("Items", new Document("_id", 1).append("items", List.of(new Document("sku", 1), new Document("sku", 2))),
        new Document("_id", 2).append("items", List.of(new Document("sku", 3))));
    assertThatThrownBy(() -> insert("Items", new Document("_id", 3).append("items", List.of(new Document("sku", 1), new Document("sku", 2)))))
        .isInstanceOf(DuplicatedKeyException.class);

    // an embedded document keeps working as it did
    insert("Items", new Document("_id", 4).append("items", new Document("sku", 9)));
    assertThatThrownBy(() -> insert("Items", new Document("_id", 5).append("items", new Document("sku", 9.0)))).isInstanceOf(
        DuplicatedKeyException.class);
    // and a missing path is a null, once
    insert("Items", new Document("_id", 6));
    assertThatThrownBy(() -> insert("Items", new Document("_id", 7))).isInstanceOf(DuplicatedKeyException.class);
  }

  @Test
  void aCompoundUniqueIndexOverAnEmbeddedField() {
    createUniqueIndex("Compound", new Document("a", 1).append("b.c", -1));
    insert("Compound", new Document("_id", 1).append("a", 1).append("b", new Document("c", 1)),
        new Document("_id", 2).append("a", 1).append("b", new Document("c", "1")),
        new Document("_id", 3).append("a", 2).append("b", new Document("c", 1)), new Document("_id", 4).append("a", 1));
    assertThatThrownBy(() -> insert("Compound", new Document("_id", 5).append("a", 1.0).append("b", new Document("c", 1)))).isInstanceOf(
        DuplicatedKeyException.class);
    assertThatThrownBy(() -> insert("Compound", new Document("_id", 6).append("a", 1))).isInstanceOf(DuplicatedKeyException.class);

    // an update that moves a document onto the pair another holds is refused, the one that frees a pair is not
    assertThat(writeErrorCodes(command("update", "Compound", new Document("updates", List.of(new Document("q", new Document("_id", 2))
        .append("u", new Document("$set", new Document("b.c", 1)))))))).containsExactly(ErrorCode.DuplicateKey.getValue());
    command("update", "Compound", new Document("updates", List.of(new Document("q", new Document("_id", 2)).append("u",
        new Document("$set", new Document("b.c", 5))))));
    insert("Compound", new Document("_id", 7).append("a", 1).append("b", new Document("c", "1")));
    assertThat(find("Compound", new Document())).hasSize(5);
  }

  @Test
  void aLegacyUniqueIndexOnAnUndeclaredFieldGivesWayToTheDerivedOne() {
    // what createIndexes built before #12073: a unique index on the plain field, its keys read as strings
    database.getSchema().createDocumentType("OldUnique");
    final TypeIndexBuilder builder = database.getSchema().buildTypeIndex("OldUnique", new String[] { "a" });
    builder.withType(Schema.INDEX_TYPE.LSM_TREE);
    builder.withUnique(true);
    builder.withDefaultKeyTypesForUndeclaredProperties(new Type[] { Type.STRING });
    builder.create();
    database.transaction(() -> database.newDocument("OldUnique").set("_id", 1).set("a", 1).save());

    createUniqueIndex("OldUnique", new Document("a", 1));

    assertThat(uniqueIndexProperties("OldUnique")).containsExactly("$unique:a");
    insert("OldUnique", new Document("_id", 2).append("a", "1"));
    assertThat(uniqueIndexProperties("OldUnique")).containsExactlyInAnyOrder("$unique:a", MongoKeys.ID_KEY);
    assertThatThrownBy(() -> insert("OldUnique", new Document("_id", 3).append("a", 1.0))).isInstanceOf(DuplicatedKeyException.class);
  }

  @Test
  void everyWritePathKeepsTheKeysCurrent() {
    createUniqueIndex("Paths", new Document("a", 1));
    // an insert without an _id is given one, as its first field, and its key
    insert("Paths", new Document("a", 1), new Document("a", 2));
    final List<Document> generated = find("Paths", new Document());
    assertThat(generated).extracting(d -> d.keySet().iterator().next()).containsOnly("_id");
    assertThat(generated).extracting(d -> d.get("_id")).allMatch(ObjectId.class::isInstance);
    assertThat(keyedRecords("Paths", MongoKeys.ID_KEY)).isEqualTo(2L);

    // an update without a filter: the keys follow the new values (it is not the SQL UPDATE that leaves them as they were)
    command("update", "Paths", new Document("updates", List.of(new Document("q", new Document("a", 1)).append("u", new Document("$set", new Document("a", 5))))));
    command("update", "Paths", new Document("updates", List.of(new Document("q", new Document()).append("multi", false).append("u",
        new Document("$set", new Document("b", 1))))));
    assertThatThrownBy(() -> insert("Paths", new Document("_id", "x").append("a", 5.0))).isInstanceOf(DuplicatedKeyException.class);
    insert("Paths", new Document("_id", "y").append("a", 1));

    // a replacement is the new fields and their keys
    command("update", "Paths", new Document("updates", List.of(new Document("q", new Document("_id", "y")).append("u", new Document("a", 7)))));
    assertThatThrownBy(() -> insert("Paths", new Document("_id", "z").append("a", 7))).isInstanceOf(DuplicatedKeyException.class);
    insert("Paths", new Document("_id", "z").append("a", 1));

    // an upsert that inserts writes the keys of what it stores
    command("update", "Paths", new Document("updates", List.of(new Document("q", new Document("_id", "up")).append("u",
        new Document("$set", new Document("a", 9))).append("upsert", true))));
    assertThatThrownBy(() -> insert("Paths", new Document("_id", "w").append("a", 9L))).isInstanceOf(DuplicatedKeyException.class);
    assertThatThrownBy(() -> insert("Paths", new Document("_id", "up"))).isInstanceOf(DuplicatedKeyException.class);
    assertThat(keyedRecords("Paths", "$unique:a")).isEqualTo((long) find("Paths", new Document()).size());
  }

  @Test
  void aUniqueCreateIndexesOnTheIdIsTheIdIndex() {
    database.getSchema().createDocumentType("OnId");
    database.transaction(() -> {
      database.newDocument("OnId").set("_id", 1).save();
      database.newDocument("OnId").set("k", "no id").save();
      database.newDocument("OnId").set("k", "no id either").save();
    });

    createUniqueIndex("OnId", new Document("_id", 1));

    // the index every collection has, on the keys of the records that have an _id: the ones without are not duplicates of each other
    assertThat(uniqueIndexProperties("OnId")).containsExactly(MongoKeys.ID_KEY);
    assertThat(keyedRecords("OnId", MongoKeys.ID_KEY)).isEqualTo(1L);
    assertThatThrownBy(() -> insert("OnId", new Document("_id", 1))).isInstanceOf(DuplicatedKeyException.class);
    insert("OnId", new Document("_id", "1"));
  }

  @Test
  void aUniqueIndexOnADeclaredFieldIsNotTheLegacyOneAndStays() {
    database.command("sql", "CREATE DOCUMENT TYPE Declared");
    database.command("sql", "CREATE PROPERTY Declared.a STRING");
    database.command("sql", "CREATE INDEX ON Declared (a) UNIQUE");

    createUniqueIndex("Declared", new Document("a", 1));

    assertThat(uniqueIndexProperties("Declared")).containsExactlyInAnyOrder("a", "$unique:a");
  }

  @Test
  void everyBsonTypeHasAStableKeyOfItsOwn() {
    final List<Object> samples = List.of(1, "1", true, 1.5, OID, HEX, new Date(1_700_000_000_123L), new BsonTimestamp(5L << 32 | 7),
        new BsonTimestamp(5L << 32 | 8), MinKey.getInstance(), MaxKey.getInstance(), new BsonJavaScript("x"), new BsonJavaScript("y"),
        LegacyUUID.fromString("00000000-0000-0001-0000-000000000002"), UUID.fromString("00000000-0000-0001-0000-000000000002"),
        new BsonRegularExpression("^a", "i"), new BsonRegularExpression("^a", ""), new BinData(new byte[] { 1, 2, 3 }),
        new Document("x", 1), List.of(1), Decimal128.NaN);
    final List<Object> twins = List.of(1.0, "1", true, 1.5, new ObjectId(HEX), HEX, Instant.ofEpochMilli(1_700_000_000_123L),
        new BsonTimestamp(5L << 32 | 7), new BsonTimestamp(5L << 32 | 8), MinKey.getInstance(), MaxKey.getInstance(),
        new BsonJavaScript("x"), new BsonJavaScript("y"), LegacyUUID.fromString("00000000-0000-0001-0000-000000000002"),
        UUID.fromString("00000000-0000-0001-0000-000000000002"), new BsonRegularExpression("^a", "i"), new BsonRegularExpression("^a", ""),
        new BinData(new byte[] { 1, 2, 3 }), new Document("x", 1.0), List.of(1L), Decimal128.NaN);

    final List<String> keys = new ArrayList<>();
    for (int i = 0; i < samples.size(); i++) {
      final String key = MongoKeys.key(samples.get(i));
      // a value is the same key whichever object stands for it, and never one that depends on the identity of an object
      assertThat(MongoKeys.key(twins.get(i))).as("%s", samples.get(i)).isEqualTo(key);
      assertThat(key).as("%s", samples.get(i)).doesNotContain("@");
      keys.add(key);
    }
    // a Double 1.0 is the Integer 1, whatever else: the samples of the other kinds are all distinct
    assertThat(keys).doesNotHaveDuplicates();
  }

  @Test
  void aValueIsTheSameKeyWhicheverPathWroteIt() {
    // the key of an insert is made from the client's document, the one of an update or of a backfill from the stored record read back:
    // a value that is not the same key on both is not found by its own index, or refused for a value it does not share
    final List<Object> values = new ArrayList<>(List.of(new Date(1_700_000_000_123L), Instant.ofEpochMilli(1_700_000_000_123L),
        new Decimal128(new BigDecimal("1.50")), Decimal128.NEGATIVE_ZERO, Decimal128.NaN, Decimal128.POSITIVE_INFINITY,
        new BinData(new byte[] { 1, 2, 3 }), new BsonTimestamp(5L << 32 | 7), MinKey.getInstance(), MaxKey.getInstance(),
        new BsonJavaScript("x"), LegacyUUID.fromString("00000000-0000-0001-0000-000000000002"), new BsonRegularExpression("^a", "i"), OID, HEX,
        "$oid:" + HEX, "$str:" + HEX, "", 5L, 5.5, new Document("o", OID).append("d", new Date(1_700_000_000_123L)),
        new Document("_id", HEX), new Document("_id", OID), List.of(1, "a", OID, new Document("b", 2))));

    final List<String> inconsistent = new ArrayList<>();
    for (int i = 0; i < values.size(); i++) {
      final Object value = values.get(i);
      final String described = value.getClass().getSimpleName() + " " + value;

      // insert, then an update that rewrites the keys: the key the update leaves is the one the insert wrote
      final String updated = "Updated" + i;
      createUniqueIndex(updated, new Document("a", 1));
      insert(updated, new Document("_id", 1).append("a", value));
      command("update", updated,
          new Document("updates", List.of(new Document("q", new Document("_id", 1)).append("u", new Document("$set", new Document("k", 1))))));
      if (!isRefused(() -> insert(updated, new Document("_id", 2).append("a", value))))
        inconsistent.add("insert then update: " + described);

      // an index built on what is stored, then an insert of the same value
      final String backfilled = "Backfilled" + i;
      insert(backfilled, new Document("_id", 1).append("a", value));
      createUniqueIndex(backfilled, new Document("a", 1));
      if (!isRefused(() -> insert(backfilled, new Document("_id", 2).append("a", value))))
        inconsistent.add("insert then backfill: " + described);
    }
    assertThat(inconsistent).isEmpty();
  }

  @Test
  void anIdFilterReadsTheIndexOfTheDerivedKey() {
    insert("Explained", new Document("_id", 1), new Document("_id", "1"));
    final DocumentType type = database.getSchema().getType("Explained");
    for (final Document filter : List.of(new Document("_id", 1), new Document("_id", new Document("$in", List.of(1, "1"))),
        new Document("$or", List.of(new Document("_id", 1), new Document("_id", "1"))))) {
      final Map<String, Object> params = new HashMap<>();
      final StringBuilder sql = new StringBuilder("select from Explained");
      new MongoFilter(database, filter).appendCandidateWhere(sql, params, type);

      assertThat(sql.toString()).as("%s", filter).contains("`" + MongoKeys.ID_KEY + "`");
      try (final ResultSet rs = database.query("sql", "EXPLAIN " + sql, params)) {
        assertThat(rs.next().<String>getProperty("executionPlanAsString")).as("%s", filter).contains("FETCH FROM INDEX");
      }
    }
  }

  @Tag("slow")
  @Test
  void aCollectionOfSeveralBackfillBatchesGetsEveryKeyAndTheRetryWritesNone() {
    // written outside the plugin, as a collection that predates it is: no key on any record
    final int total = 2 * MongoDBCollectionWrapper.BACKFILL_BATCH + 123;
    database.getSchema().createDocumentType("Big");
    for (int from = 0; from < total; from += 5_000) {
      final int start = from;
      database.transaction(() -> {
        for (int i = start; i < Math.min(start + 5_000, total); i++)
          database.newDocument("Big").set("_id", i).set("a", i).save();
      });
    }

    // the first insert builds the key index: three batches, each its own transaction
    insert("Big", new Document("_id", "new").append("a", -1));
    assertThat(keyedRecords("Big", MongoKeys.ID_KEY)).isEqualTo(total + 1L);
    for (final int id : new int[] { 0, MongoDBCollectionWrapper.BACKFILL_BATCH - 1, MongoDBCollectionWrapper.BACKFILL_BATCH, total - 1 }) {
      assertThat(find("Big", new Document("_id", id))).as("_id %d", id).hasSize(1);
      assertThatThrownBy(() -> insert("Big", new Document("_id", id))).isInstanceOf(DuplicatedKeyException.class);
    }
    insert("Big", new Document("_id", String.valueOf(total - 1)));

    // a createIndexes unique goes over the same batches, and doing it again costs nothing
    createUniqueIndex("Big", new Document("a", 1));
    assertThat(keyedRecords("Big", "$unique:a")).isEqualTo(total + 2L);
    final AtomicInteger updated = countUpdates("Big");
    MongoDBCollectionWrapper.backfillKeys(database, "Big", "$unique:a", new String[] { "a" });
    MongoDBCollectionWrapper.backfillKeys(database, "Big", MongoKeys.ID_KEY, null);
    assertThat(updated.get()).isZero();
  }

  @Tag("slow")
  @Test
  void aBackfillThatFailsBetweenBatchesLeavesTheCollectionConsistentAndTheRetryFinishesIt() {
    final int total = 2 * MongoDBCollectionWrapper.BACKFILL_BATCH + 123;
    database.getSchema().createDocumentType("Half");
    for (int from = 0; from < total; from += 5_000) {
      final int start = from;
      database.transaction(() -> {
        for (int i = start; i < Math.min(start + 5_000, total); i++)
          database.newDocument("Half").set("_id", i).save();
      });
    }

    // the write of one record in the second batch fails: the first batch is committed, the second is not
    final AtomicInteger writes = new AtomicInteger();
    final BeforeRecordUpdateListener failing = record -> {
      if (writes.incrementAndGet() == MongoDBCollectionWrapper.BACKFILL_BATCH + 5)
        throw new IllegalStateException("the write failed");
      return true;
    };
    database.getSchema().getType("Half").getEvents().registerListener(failing);
    assertThatThrownBy(() -> insert("Half", new Document("_id", "new"))).isInstanceOf(RuntimeException.class);
    database.getSchema().getType("Half").getEvents().unregisterListener(failing);

    // no index yet, so nothing reads the keys that are there; the records are intact and the plugin still writes (by-hand check)
    assertThat(MongoKeys.hasIdKeyIndex(database.getSchema().getType("Half"))).isFalse();
    assertThat(keyedRecords("Half", MongoKeys.ID_KEY)).isEqualTo((long) MongoDBCollectionWrapper.BACKFILL_BATCH);
    assertThat(database.countType("Half", false)).isEqualTo(total);

    // the next insert finishes it: the committed batch is not written again
    final AtomicInteger retried = countUpdates("Half");
    insert("Half", new Document("_id", "new"));
    assertThat(retried.get()).isEqualTo(total - MongoDBCollectionWrapper.BACKFILL_BATCH);
    assertThat(keyedRecords("Half", MongoKeys.ID_KEY)).isEqualTo(total + 1L);
    assertThat(MongoKeys.hasIdKeyIndex(database.getSchema().getType("Half"))).isTrue();
    assertThatThrownBy(() -> insert("Half", new Document("_id", total - 1))).isInstanceOf(DuplicatedKeyException.class);
  }

  @Test
  void equivalentIdsInsertedAtTheSameTimeAreOneDocument() throws Exception {
    insert("Racing", new Document("_id", "seed"));
    final List<Object> equivalent = List.of(1, 1L, 1.0, new Decimal128(new BigDecimal("1.00")), 1, 1L, 1.0, new Decimal128(BigDecimal.ONE));
    final AtomicInteger stored = new AtomicInteger();
    final AtomicInteger refused = new AtomicInteger();
    final List<Throwable> unexpected = new ArrayList<>();
    final CountDownLatch start = new CountDownLatch(1);

    final List<Thread> threads = new ArrayList<>();
    for (final Object id : equivalent) {
      final Thread thread = new Thread(() -> {
        try {
          start.await();
          new MongoDBCollectionWrapper(database, "Racing").insertDocuments(List.of(new Document("_id", id)));
          stored.incrementAndGet();
        } catch (final DuplicatedKeyException e) {
          refused.incrementAndGet();
        } catch (final Throwable e) {
          synchronized (unexpected) {
            unexpected.add(e);
          }
        }
      });
      threads.add(thread);
      thread.start();
    }
    start.countDown();
    for (final Thread thread : threads)
      thread.join();

    assertThat(unexpected).isEmpty();
    assertThat(stored.get()).as("one of the equivalent ids is stored").isEqualTo(1);
    assertThat(refused.get()).isEqualTo(equivalent.size() - 1);
    assertThat(find("Racing", new Document("_id", 1))).hasSize(1);
  }

  private void createUniqueIndex(final String collection, final Document key) {
    command("createIndexes", collection,
        new Document("indexes", List.of(new Document("key", key).append("name", "unique_" + key.keySet()).append("unique", true))));
  }

  /** The properties the unique indexes of a collection are on. */
  private List<String> uniqueIndexProperties(final String collection) {
    final List<String> properties = new ArrayList<>();
    for (final TypeIndex index : database.getSchema().getType(collection).getAllIndexes(false))
      if (index.isUnique())
        properties.add(index.getPropertyNames().getFirst());
    return properties;
  }

  /** Counts the record updates of a collection from now on. */
  private AtomicInteger countUpdates(final String collection) {
    final AtomicInteger updated = new AtomicInteger();
    database.getSchema().getType(collection).getEvents().registerListener((BeforeRecordUpdateListener) record -> {
      updated.incrementAndGet();
      return true;
    });
    return updated;
  }

  private long keyedRecords(final String collection, final String property) {
    try (final ResultSet rs = database.query("sql", "select count(*) as c from `" + collection + "` where `" + property + "` is not null")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private static boolean isRefused(final Runnable insert) {
    try {
      insert.run();
      return false;
    } catch (final DuplicatedKeyException e) {
      return true;
    }
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
