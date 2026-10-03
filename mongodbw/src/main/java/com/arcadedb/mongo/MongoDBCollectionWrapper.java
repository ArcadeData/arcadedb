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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.TypeIndexBuilder;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.Identifier;
import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.ArrayFilters;
import de.bwaldvogel.mongo.backend.Index;
import de.bwaldvogel.mongo.backend.QueryParameters;
import de.bwaldvogel.mongo.backend.QueryResult;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.ObjectId;
import de.bwaldvogel.mongo.oplog.Oplog;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.stream.Stream;

public class MongoDBCollectionWrapper implements MongoCollection<Long> {
  private final Database database;
  //  private final int      collectionId;
  private final String   collectionName;
  private final UUID     uuid = UUID.randomUUID();

  protected MongoDBCollectionWrapper(final Database database, final String collectionName) {
    this.database = database;
    this.collectionName = collectionName;
    //this.collectionId = database.getSchema().getType(collectionName).getBuckets(false).get(0).getId();
  }

//  protected Document getDocument(final Long aLong) {
//    final com.arcadedb.database.Document record = (com.arcadedb.database.Document) database.lookupByRID(new RID(database, collectionId, aLong), true);
//
//    final Document result = new Document();
//
//    for (String p : record.getPropertyNames())
//      result.put(p, record.get(p));
//
//    return result;
//  }

  @Override
  public UUID getUuid() {
    return uuid;
  }

  @Override
  public MongoDatabase getDatabase() {
    return null;
  }

  @Override
  public String getDatabaseName() {
    return database.getName();
  }

  @Override
  public String getFullName() {
    return null;
  }

  @Override
  public String getCollectionName() {
    return collectionName;
  }

  @Override
  public void addIndex(final Index<Long> index) {
    // TODO
  }

  @Override
  public void dropIndex(final String s) {
    // TODO

  }

  @Override
  public void renameTo(final MongoDatabase mongoDatabase, final String s) {
    // TODO
  }

  @Override
  public void addDocument(final Document document) {
    // TODO
  }

  @Override
  public void addDocuments(Stream<Document> documents) {
    MongoCollection.super.addDocuments(documents);
  }

  @Override
  public void removeDocument(final Document document) {
    // TODO
  }

  @Override
  public void addDocumentIfMissing(Document document) {
    MongoCollection.super.addDocumentIfMissing(document);
  }

  @Override
  public Iterable<Document> queryAll() {
    return MongoCollection.super.queryAll();
  }

  @Override
  public Stream<Document> queryAllAsStream() {
    return MongoCollection.super.queryAllAsStream();
  }

  @Override
  public Iterable<Document> handleQuery(Document query) {
    return MongoCollection.super.handleQuery(query);
  }

  @Override
  public Stream<Document> handleQueryAsStream(Document query) {
    return MongoCollection.super.handleQueryAsStream(query);
  }

  @Override
  public QueryResult handleQuery(Document query, int numberToSkip, int limit) {
    return MongoCollection.super.handleQuery(query, numberToSkip, limit);
  }

  @Override
  public QueryResult handleQuery(final QueryParameters queryParameters) {
    int numberToReturn = queryParameters.getLimit();
    if (numberToReturn < 0)
      numberToReturn = -numberToReturn;

    final int numberToSkip = queryParameters.getNumberToSkip();

    final Document queryObject = queryParameters.getQuerySelector();

    Document query = null;
    Document orderBy = null;
    if (queryObject != null) {
      if (queryObject.containsKey("query")) {
        query = (Document) queryObject.get("query");
      } else if (queryObject.containsKey("$query")) {
        query = (Document) queryObject.get("$query");
      } else {
        query = queryObject;
      }

      orderBy = (Document) queryObject.remove("$orderBy");
    }

    if (this.count() == 0)
      return new QueryResult();

    final Iterable<Document> objs = this.queryDocuments(query, orderBy, numberToSkip, numberToReturn);

    return new QueryResult(objs);
  }

  @Override
  public void insertDocuments(final List<Document> list) {
    // a type without the unique index (made through SQL or Studio, or already holding duplicates) is checked by hand
    final List<Object> ids = new ArrayList<>(list.size());
    for (final Document d : list)
      if (d.containsKey("_id"))
        ids.add(d.get("_id"));
    ensureIdIndex(database, collectionName, ids);
    final boolean checkByHand = !hasUniqueIdIndex(database.getSchema().getType(collectionName));

    database.begin();
    try {
      for (final Document d : list) {
        if (checkByHand && d.containsKey("_id"))
          checkIdIsFree(d.get("_id"));

        final MutableDocument record = database.newDocument(collectionName);

        for (final Map.Entry<String, Object> p : d.entrySet()) {
          final Object value = p.getValue();
          if (value instanceof ObjectId id)
            record.set(p.getKey(), id.getHexData());
          else
            record.set(p.getKey(), value);
        }

        record.save();
      }

      database.commit();
    } finally {
      // a failed insert (a duplicated _id, a constraint) must not leave the transaction open on this thread
      if (database.isTransactionActive())
        database.rollback();
    }
  }

  private void checkIdIsFree(final Object id) {
    final Object bound = id instanceof ObjectId objectId ? objectId.getHexData() : id;
    try (final ResultSet rs = database.query("SQL", "select @rid from " + Identifier.quote(collectionName) + " where _id = :id limit 1",
        Map.of("id", bound))) {
      if (rs.hasNext())
        throw new DuplicatedKeyException(collectionName + "[_id]", String.valueOf(bound), rs.next().getIdentity().orElse(null));
    }
  }

  /**
   * MongoDB guarantees the {@code _id} of a collection is unique through an index every collection has. ArcadeDB has no implicit
   * one, so the plugin creates a unique index on {@code _id} when the first document arrives, because only then the type of the
   * key is known: the index orders its keys by their type, so a numeric {@code _id} needs a numeric key (a string key would
   * answer {@code {_id: {$gt: 5}}} and a sort lexicographically, "10" before "5"). A collection whose {@code _id} is of another
   * kind, or that already holds duplicated values, gets no index and {@link #insertDocuments} checks by hand instead.
   */
  static void ensureIdIndex(final Database database, final String collectionName, final Object sampleId) {
    ensureIdIndex(database, collectionName, sampleId == null ? List.of() : List.of(sampleId));
  }

  /**
   * @param ids the {@code _id} values about to be stored. When they (or an earlier insert) mix kinds, a numeric key cannot hold
   *            them all: the index is rebuilt with string keys, which accept anything and keep the uniqueness check fast, at
   *            the price of the ordering of a collection whose {@code _id} has no single order anyway.
   */
  static void ensureIdIndex(final Database database, final String collectionName, final Collection<?> ids) {
    Type needed = null;
    boolean mixed = false;
    for (final Object id : ids) {
      final Type type = idKeyType(id);
      if (needed == null)
        needed = type;
      else if (needed != type)
        mixed = true;
      if (type == null)
        mixed = true;
    }
    if (ids.isEmpty())
      return;
    final Type keyType = mixed ? Type.STRING : needed;

    final DocumentType type = database.getSchema().getType(collectionName);
    final TypeIndex existing = findUniqueIdIndex(type);
    if (existing != null) {
      final Type current = existing.getKeyTypes()[0];
      if (current == Type.STRING || keyType == current)
        return;
      // the existing index cannot hold this key: rebuild it with string keys
      database.getSchema().dropIndex(existing.getName());
      createIdIndex(database, collectionName, Type.STRING);
      return;
    }

    if (keyType != null)
      createIdIndex(database, collectionName, keyType);
  }

  private static void createIdIndex(final Database database, final String collectionName, final Type keyType) {
    try {
      final TypeIndexBuilder builder = database.getSchema().buildTypeIndex(collectionName, new String[] { "_id" });
      builder.withType(Schema.INDEX_TYPE.LSM_TREE);
      builder.withUnique(true);
      builder.withIgnoreIfExists(true);
      builder.withDefaultKeyTypesForUndeclaredProperties(new Type[] { keyType });
      builder.create();
    } catch (final RuntimeException e) {
      LogManager.instance().log(MongoDBCollectionWrapper.class, Level.WARNING,
          "Cannot create the unique index on _id of collection '%s': duplicates of _id will be checked on insert (%s)", null,
          collectionName, e.getMessage());
    }
  }

  private static TypeIndex findUniqueIdIndex(final DocumentType type) {
    for (final TypeIndex index : type.getAllIndexes(false))
      if (index.isUnique() && index.getPropertyNames().size() == 1 && "_id".equals(index.getPropertyNames().getFirst()))
        return index;
    return null;
  }

  /**
   * The index key type for an {@code _id}: an ObjectId is stored as its hex string. {@code null} for anything else, which
   * cannot be ordered by a single key type.
   */
  private static Type idKeyType(final Object id) {
    if (id instanceof String || id instanceof ObjectId)
      return Type.STRING;
    if (id instanceof Integer || id instanceof Long || id instanceof Short || id instanceof Byte)
      return Type.LONG;
    if (id instanceof Double || id instanceof Float)
      return Type.DOUBLE;
    return null;
  }

  static boolean hasUniqueIdIndex(final DocumentType type) {
    return findUniqueIdIndex(type) != null;
  }

  @Override
  public List<Document> insertDocuments(final List<Document> list, final boolean b) {
    return null;
  }

  @Override
  public Document updateDocuments(final Document document, final Document document1, final ArrayFilters filters, final boolean b, final boolean b1,
      final Oplog opLog) {
    return null;
  }

  @Override
  public int deleteDocuments(final Document document, final int limit) {
    return 0;
  }

  @Override
  public int deleteDocuments(final Document document, final int i, final Oplog oplog) {
    return 0;
  }

  @Override
  public Document handleDistinct(final Document document) {
    return null;
  }

  @Override
  public Document getStats() {
    return null;
  }

  @Override
  public Document validate() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Document findAndModify(final Document document) {
    return null;
  }

  @Override
  public int count(final Document document, final int skip, final int limit) {
    // The count command's own default for an unspecified limit is -1 (MongoDBDatabaseWrapper#countCollection), so
    // limit <= 0 here means "no limit" - deliberately, not incidentally lining up with an explicit limit: 0.
    final boolean hasFilter = document != null && !document.isEmpty();
    if (!hasFilter && skip <= 0 && limit <= 0)
      return (int) database.countType(collectionName, false);

    int counted;

    if (skip <= 0 && limit <= 0) {
      // No pagination to apply: let the engine aggregate instead of materializing every matching row.
      final Map<String, Object> params = new HashMap<>();
      final StringBuilder sql = new StringBuilder("select count(*) as count from ").append(Identifier.quote(collectionName));
      if (hasFilter) {
        sql.append(" where ");
        MongoDBToSqlTranslator.buildExpression(sql, params, document);
      }

      try (final ResultSet rs = database.query("SQL", sql.toString(), params)) {
        counted = rs.hasNext() ? ((Number) rs.next().getProperty("count")).intValue() : 0;
      }
    } else {
      // Push skip/limit into the query itself - @rid is enough to count a row, no need to materialize the record.
      final Map<String, Object> params = new HashMap<>();
      final StringBuilder sql = new StringBuilder("select @rid from ").append(Identifier.quote(collectionName));
      if (hasFilter) {
        sql.append(" where ");
        MongoDBToSqlTranslator.buildExpression(sql, params, document);
      }
      if (skip > 0)
        sql.append(" SKIP ").append(skip);
      if (limit > 0)
        sql.append(" LIMIT ").append(limit);

      counted = 0;
      try (final ResultSet rs = database.query("SQL", sql.toString(), params)) {
        while (rs.hasNext()) {
          rs.next();
          counted++;
        }
      }
    }

    return counted;
  }

  @Override
  public boolean isEmpty() {
    return MongoCollection.super.isEmpty();
  }

  @Override
  public int count() {
    return (int) database.countType(getCollectionName(), false);
  }

  @Override
  public int getNumIndexes() {
    return 0;
  }

  @Override
  public List<Index<Long>> getIndexes() {
    return null;
  }

  @Override
  public void drop() {
    database.getSchema().dropType(collectionName);
  }

  private Iterable<Document> queryDocuments(final Document query, final Document orderBy, final int numberToSkip, final int numberToReturn) {
    final List<Document> result = new ArrayList<>();

    final boolean hasFilter = query != null && !query.isEmpty();
    final boolean hasOrderBy = orderBy != null && !orderBy.isEmpty();

    if (!hasFilter && !hasOrderBy) {
      // SCAN
      MongoDBToSqlTranslator.fillResultSet(numberToSkip, numberToReturn, result, database.iterateType(collectionName, false));
    } else {
      // EXECUTE A SQL QUERY. A sort-only find() (no filter) still has to reach here rather than the scan above,
      // otherwise the order-by would be silently dropped.
      final Map<String, Object> params = new HashMap<>();
      final StringBuilder sql = new StringBuilder("select from ").append(Identifier.quote(collectionName));

      if (hasFilter) {
        sql.append(" where ");
        MongoDBToSqlTranslator.buildExpression(sql, params, query);
      }

      if (hasOrderBy) {
        sql.append(" order by ");
        int i = 0;
        for (final String p : orderBy.keySet()) {
          if (i > 0)
            sql.append(", ");
          sql.append(MongoDBToSqlTranslator.quoteFieldPath(p));
          sql.append(' ');
          sql.append(((Number) orderBy.get(p)).intValue() == 1 ? "asc" : "desc");
          ++i;
        }
      }

      try (final ResultSet rs = database.query("SQL", sql.toString(), params)) {
        MongoDBToSqlTranslator.fillResultSet(numberToSkip, numberToReturn, result, rs);
      }
    }

    return result;
  }
}
