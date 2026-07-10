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
import com.arcadedb.database.ProtocolContext;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.TypeIndexBuilder;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.CollectionOptions;
import de.bwaldvogel.mongo.backend.Cursor;
import de.bwaldvogel.mongo.backend.CursorRegistry;
import de.bwaldvogel.mongo.backend.DatabaseResolver;
import de.bwaldvogel.mongo.backend.QueryResult;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.backend.aggregation.Aggregation;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.ObjectId;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.oplog.Oplog;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import io.netty.channel.Channel;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

import static de.bwaldvogel.mongo.backend.Utils.markOkay;

public class MongoDBDatabaseWrapper implements MongoDatabase {
  protected final Database                           database;
  protected final MongoDBProtocolPlugin              plugin;
  protected final MongoBackend                       backend;
  protected final Map<String, MongoCollection<Long>> collections    = new ConcurrentHashMap();
  protected final Map<Channel, List<Document>>       lastResults    = new ConcurrentHashMap();
  protected final CursorRegistry                     cursorRegistry = new CursorRegistry();

  public MongoDBDatabaseWrapper(final Database database, final MongoDBProtocolPlugin plugin, final MongoBackend backend) {
    this.database = database;
    this.plugin = plugin;
    this.backend = backend;
  }

  /**
   * Resolves a collection wrapper lazily against the live schema. This database wrapper is shared and cached across all
   * client connections, so the collection set must be looked up on demand: types created after the wrapper was built
   * (e.g. via Studio, SQL or another connection) must be visible too.
   * <p>
   * The cache is keyed by type name and entries are never evicted, but it stays bounded by the number of distinct type
   * names (one entry per name, reused on drop/recreate). A stale entry after a drop/recreate is harmless because
   * {@link MongoDBCollectionWrapper} keeps only the type name and re-resolves it against the database on every operation.
   *
   * @return the collection wrapper, or {@code null} if no type with that name exists.
   */
  private MongoCollection<Long> getCollection(final String collectionName) {
    if (!database.getSchema().existsType(collectionName))
      return null;
    return collections.computeIfAbsent(collectionName, name -> new MongoDBCollectionWrapper(database, name));
  }

  @Override
  public String getDatabaseName() {
    return database.getName();
  }

  @Override
  public void handleClose(final Channel channel) {
    // THIS IS CALLED FROM THE CLIENT TO FREE CONNECTION RESOURCES. DO NOT CLOSE THE DATABASE BECAUSE OTHER CONNECTIONS MAY
    // NEED IT. ONLY THE CLOSING CHANNEL'S STATE MUST BE REMOVED: THE COLLECTION CACHE AND OTHER CONNECTIONS' RESULTS ARE
    // SHARED ACROSS ALL CLIENTS (THIS WRAPPER IS CACHED PER-DATABASE IN THE BACKEND).
    lastResults.remove(channel);
  }

  @Override
  public Document handleCommand(final Channel channel, final String command, final Document document,
      final DatabaseResolver databaseResolver,
      final Oplog opLog) {
    ProtocolContext.set("mongo");
    try {
      if ("find".equalsIgnoreCase(command))
        return find(document);
      else if ("create".equalsIgnoreCase(command))
        return createCollection(document);
      else if ("count".equalsIgnoreCase(command))
        return countCollection(document);
      else if ("insert".equalsIgnoreCase(command))
        return insertDocument(channel, document);
      else if ("delete".equalsIgnoreCase(command))
        return deleteDocuments(document);
      else if ("update".equalsIgnoreCase(command))
        return updateDocuments(document);
      else if ("aggregate".equalsIgnoreCase(command))
        return aggregateCollection(command, document, opLog);
      else if ("createIndexes".equalsIgnoreCase(command))
        return createIndexes(document);
      else {
        LogManager.instance()
            .log(this, Level.SEVERE, "Received unsupported command from MongoDB client '%s', (document=%s)", null, command,
                document);
        throw new UnsupportedOperationException(
          "Received unsupported command from MongoDB client '%s', (document=%s)".formatted(command, document));
      }
    } catch (final Exception e) {
      throw new MongoServerException("Error on executing MongoDB '" + command + "' command", e);
    } finally {
      ProtocolContext.clear();
    }
  }

  public ResultSet query(final String query) throws MongoServerException {
    final JSONObject queryJson = new JSONObject(query);

    final String collection = queryJson.getString("collection");
    final int numberToSkip = queryJson.has("numberToSkip") ? queryJson.getInt("numberToSkip") : 0;
    final int numberToReturn = queryJson.has("numberToSkip") ? queryJson.getInt("numberToReturn") : 0;
    final JSONObject q = queryJson.getJSONObject("query");

    final Document transformedQuery = json2Document(q);

    final MongoQuery mongoQuery = new MongoQuery(null, null, collection, numberToSkip, numberToReturn, transformedQuery, null);

    final QueryResult result = handleQuery(mongoQuery);

    // TRANSFORM THE RESULT INTO ARCADEDB RESULT SET
    final IteratorResultSet resultset = new IteratorResultSet(result.iterator()) {
      @Override
      public Result next() {
        final Map doc = (Map) ResultInternal.wrap(super.next().getProperty("value"));
        return new ResultInternal(doc);
      }
    };

    return resultset;
  }

  private Document json2Document(final JSONObject map) {
    final Document doc = new Document();

    for (final String k : map.keySet()) {
      Object v = map.get(k);
      if (v instanceof JSONObject object)
        v = json2Document(object);
      else if (v instanceof JSONArray nArray) {
        final List<Object> array = new ArrayList<>(nArray.length());

        for (int i = 0; i < nArray.length(); i++) {
          Object a = nArray.get(i);
          if (a instanceof JSONObject object)
            a = json2Document(object);

          array.add(a);
        }

        v = array;
      }
      doc.append(k, v);
    }

    return doc;
  }

  @Override
  public QueryResult handleQuery(final MongoQuery query) throws MongoServerException {
    try {
      this.clearLastStatus(query.getChannel());
      final String collectionName = query.getCollectionName();
      final MongoCollection<Long> collection = getCollection(collectionName);
      if (collection == null) {
        return new QueryResult();
      } else {
        final int numSkip = query.getNumberToSkip();
        final int numReturn = query.getNumberToReturn();
        return collection.handleQuery(query.getQuery(), numSkip, numReturn);
      }
    } catch (final Exception e) {
      throw new MongoServerException("Error on executing MongoDB query", e);
    }
  }

  private Document aggregateCollection(final String command, final Document document, final Oplog oplog)
      throws MongoServerException {
    final String collectionName = document.get("aggregate").toString();
    database.countType(collectionName, false);

    final MongoCollection<Long> collection = getCollection(collectionName);

    final Object pipelineObject = Aggregation.parse(document.get("pipeline"));
    final List<Document> pipeline = Aggregation.parse(pipelineObject);
    if (!pipeline.isEmpty()) {
      final Document changeStream = (Document) pipeline.getFirst().get("$changeStream");
      if (changeStream != null) {
        final Aggregation aggregation = Aggregation.fromPipeline(pipeline.subList(1, pipeline.size()), plugin, this, collection,
            oplog);
        aggregation.validate(document);
        return commandChangeStreamPipeline(document, oplog, collectionName, changeStream, aggregation);
      }
    }
    final Aggregation aggregation = Aggregation.fromPipeline(pipeline, plugin, this, collection, oplog);
    aggregation.validate(document);

    return firstBatchCursorResponse(collectionName, "firstBatch", aggregation.computeResult(), 0);
  }

  private Document firstBatchCursorResponse(final String ns, final String key, final List<Document> documents,
      final long cursorId) {
    final Document cursorResponse = new Document();
    cursorResponse.put("id", cursorId);
    cursorResponse.put("ns", getFullCollectionNamespace(ns));
    cursorResponse.put(key, documents);

    final Document response = new Document();
    response.put("cursor", cursorResponse);
    markOkay(response);
    return response;
  }

  protected String getFullCollectionNamespace(final String collectionName) {
    return getDatabaseName() + "." + collectionName;
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public MongoCollection<?> createCollectionOrThrowIfExists(final String s, final CollectionOptions collectionOptions) {
    return null;
  }

  @Override
  public MongoCollection<?> resolveCollection(final String collectionName, final boolean throwExceptionIfNotFound) {
    return null;
  }

  @Override
  public void drop(final Oplog opLog) {
    database.drop();
  }

  @Override
  public void dropCollection(final String collectionName, final Oplog opLog) {
    database.getSchema().dropType(collectionName);
  }

  @Override
  public void moveCollection(final MongoDatabase mongoDatabase, final MongoCollection<?> mongoCollection, final String s) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void unregisterCollection(final String collectionName) {
    database.getSchema().dropBucket(collectionName);
  }

  private Document commandChangeStreamPipeline(final Document query, final Oplog oplog, final String collectionName,
      final Document changeStreamDocument,
      final Aggregation aggregation) {
    final Document cursorDocument = (Document) query.get("cursor");
    final int batchSize = (int) cursorDocument.getOrDefault("batchSize", 0);

    final String namespace = getFullCollectionNamespace(collectionName);
    final Cursor cursor = oplog.createCursor(changeStreamDocument, namespace, aggregation);
    return firstBatchCursorResponse(namespace, "firstBatch", cursor.takeDocuments(batchSize), cursor.getId());
  }

  private Document createCollection(final Document document) {
    database.getSchema().buildDocumentType().withName((String) document.get("create")).withTotalBuckets(1).create();
    return responseOk();
  }

  /**
   * Handles the MongoDB {@code createIndexes} command (also used by the driver's {@code createIndex}
   * helper). Each requested index maps to an ArcadeDB {@code LSM_TREE} type index over the same
   * property list. MongoDB collections are schemaless, so properties not yet declared on the type
   * are left free-form and the index falls back to {@code STRING} key serialisation (the same
   * approach the native OpenCypher engine uses for {@code CREATE INDEX}, issue #4222), preserving
   * the stored Java type. Re-creating an existing index is a no-op, matching MongoDB semantics.
   */
  private Document createIndexes(final Document document) {
    final String collectionName = document.get("createIndexes").toString();

    final boolean createdCollectionAutomatically;
    if (!database.getSchema().existsType(collectionName)) {
      database.getSchema().buildDocumentType().withName(collectionName).withTotalBuckets(1).create();
      createdCollectionAutomatically = true;
    } else
      createdCollectionAutomatically = false;

    // ArcadeDB has no implicit _id index: MongoDB always reports one, so offset both counts by 1 to
    // keep numIndexesAfter == numIndexesBefore + <new indexes> consistent for clients that check it.
    final int numIndexesBefore = database.getSchema().getType(collectionName).getAllIndexes(false).size() + 1;

    final List<Document> indexes = (List<Document>) document.get("indexes");
    if (indexes != null)
      for (final Document index : indexes)
        createSingleIndex(collectionName, index);

    final int numIndexesAfter = database.getSchema().getType(collectionName).getAllIndexes(false).size() + 1;

    final Document response = responseOk();
    response.put("createdCollectionAutomatically", createdCollectionAutomatically);
    response.put("numIndexesBefore", numIndexesBefore);
    response.put("numIndexesAfter", numIndexesAfter);
    return response;
  }

  private void createSingleIndex(final String collectionName, final Document index) {
    final Document key = (Document) index.get("key");
    if (key == null || key.isEmpty())
      return;

    final DocumentType type = database.getSchema().getType(collectionName);
    final String[] propertyNames = key.keySet().toArray(new String[0]);

    // Schemaless collections: keep undeclared properties free-form and let the index serialise
    // their keys as STRING. Declared properties (null entry) keep their own type.
    final Type[] defaultKeyTypes = new Type[propertyNames.length];
    boolean anyUndeclared = false;
    for (int i = 0; i < propertyNames.length; i++) {
      if (type.getPolymorphicPropertyIfExists(propertyNames[i]) != null)
        continue;
      defaultKeyTypes[i] = Type.STRING;
      anyUndeclared = true;
    }

    final boolean unique = Utils.isTrue(index.get("unique"));

    final TypeIndexBuilder builder = database.getSchema().buildTypeIndex(collectionName, propertyNames);
    builder.withType(Schema.INDEX_TYPE.LSM_TREE);
    builder.withUnique(unique);
    builder.withIgnoreIfExists(true);
    if (anyUndeclared)
      builder.withDefaultKeyTypesForUndeclaredProperties(defaultKeyTypes);
    builder.create();
  }

  private Document countCollection(final Document document) throws MongoServerException {
    final String collectionName = document.get("count").toString();
    database.countType(collectionName, false);

    final Document response = responseOk();

    final MongoCollection<Long> collection = getCollection(collectionName);

    if (collection == null) {
      response.put("missing", Boolean.TRUE);
      response.put("n", 0);
    } else {
      final Document queryObject = (Document) document.get("query");
      final int limit = this.getOptionalNumber(document, "limit", -1);
      final int skip = this.getOptionalNumber(document, "skip", 0);
      response.put("n", collection.count(queryObject, skip, limit));
    }

    return response;
  }

  private Document find(final Document document) throws MongoServerException {
    final Document filter = (Document) document.get("filter");
    // A missing/0 limit means "no limit" (a -1 sentinel here would be interpreted as a legacy single-batch limit of 1)
    final int limit = this.getOptionalNumber(document, "limit", 0);
    final int skip = this.getOptionalNumber(document, "skip", 0);
    final String collectionName = (String) document.get("find");

    final MongoQuery mongoQuery = new MongoQuery(null, null, collectionName, skip, limit, filter, null);

    final QueryResult result = handleQuery(mongoQuery);

    final List<Document> documents = new ArrayList<>();
    for (Iterator<Document> it = result.iterator(); it.hasNext(); )
      documents.add(it.next());

    return firstBatchCursorResponse(collectionName, "firstBatch", documents, 0);
  }

  private Document insertDocument(final Channel channel, final Document query) throws MongoServerException {
    final String collectionName = query.get("insert").toString();
    final boolean isOrdered = Utils.isTrue(query.get("ordered"));
    final List<Document> documents = (List) query.get("documents");
    final List<Document> writeErrors = new ArrayList();

    int n = 0;
    try {
      this.clearLastStatus(channel);

      try {
        if (collectionName.startsWith("system.")) {
          throw new MongoServerError(16459, "attempt to insert in system namespace");
        } else {
          final MongoCollection<Long> collection = getOrCreateCollection(collectionName);
          collection.insertDocuments(documents);
          n = documents.size();

          assert n == documents.size();

          final Document result = new Document("n", n);
          this.putLastResult(channel, result);
        }
      } catch (final MongoServerError var7) {
        this.putLastError(channel, var7);
        throw var7;
      }

      ++n;
    } catch (final MongoServerError e) {
      final Document error = new Document();
      error.put("index", n);
      error.put("errmsg", e.getMessage());
      error.put("code", e.getCode());
      error.putIfNotNull("codeName", e.getCodeName());
      writeErrors.add(error);
    }

    final Document result = new Document();
    result.put("n", n);
    if (!writeErrors.isEmpty()) {
      result.put("writeErrors", writeErrors);
    }

    markOkay(result);
    return result;
  }

  private MongoCollection<Long> getOrCreateCollection(final String collectionName) {
    MongoCollection<Long> collection = collections.get(collectionName);
    if (collection == null) {
      collection = new MongoDBCollectionWrapper(database, collectionName);
      collections.put(collectionName, collection);
    }
    return collection;
  }

  /**
   * Handles the MongoDB {@code delete} command (used by the driver's {@code deleteOne} /
   * {@code deleteMany} helpers). Each delete spec carries a filter {@code q} and a {@code limit}:
   * a limit of 1 deletes only the first match ({@code deleteOne}), 0 deletes all matches
   * ({@code deleteMany}). Filters are translated to the same SQL {@code WHERE} clause used by find.
   */
  private Document deleteDocuments(final Document document) {
    final String collectionName = document.get("delete").toString();
    final List<Document> deletes = (List<Document>) document.get("deletes");

    int n = 0;
    if (database.getSchema().existsType(collectionName) && deletes != null) {
      database.begin();
      try {
        for (final Document del : deletes) {
          final Document q = (Document) del.get("q");
          final Number limit = (Number) del.get("limit");
          final boolean single = limit != null && limit.intValue() == 1;

          final StringBuilder sql = new StringBuilder("DELETE FROM `").append(collectionName).append('`');
          appendWhere(sql, q);
          if (single)
            sql.append(" LIMIT 1");

          n += executeCount(sql.toString());
        }
        database.commit();
      } catch (final RuntimeException e) {
        database.rollback();
        throw e;
      }
    }

    final Document response = new Document("n", n);
    markOkay(response);
    return response;
  }

  /**
   * Handles the MongoDB {@code update} command (used by {@code updateOne}, {@code updateMany},
   * {@code replaceOne} and {@code replaceMany}). The update document {@code u} is either a full
   * replacement (no {@code $}-prefixed keys, mapped to SQL {@code CONTENT}) or a set of update
   * operators ({@code $set}, {@code $unset}, {@code $inc}). The {@code multi} flag selects between
   * updating the first match only ({@code LIMIT 1}) and all matches. When {@code upsert} is set and
   * nothing matched, a new document seeded from the filter's equalities and the update is inserted.
   */
  private Document updateDocuments(final Document document) {
    final String collectionName = document.get("update").toString();
    final List<Document> updates = (List<Document>) document.get("updates");

    int n = 0;
    int nModified = 0;
    final List<Document> upserted = new ArrayList<>();

    if (updates != null) {
      database.begin();
      try {
        int index = 0;
        for (final Document upd : updates) {
          final Document q = (Document) upd.get("q");
          final Document u = (Document) upd.get("u");
          final boolean multi = Utils.isTrue(upd.get("multi"));
          final boolean upsert = Utils.isTrue(upd.get("upsert"));

          final int updated = executeUpdate(collectionName, q, u, multi);
          n += updated;
          nModified += updated;

          if (updated == 0 && upsert) {
            final ObjectId id = executeUpsert(collectionName, q, u);
            final Document upDoc = new Document("index", index);
            upDoc.put("_id", id);
            upserted.add(upDoc);
          }
          ++index;
        }
        database.commit();
      } catch (final RuntimeException e) {
        database.rollback();
        throw e;
      }
    }

    final Document response = new Document();
    response.put("n", n + upserted.size());
    response.put("nModified", nModified);
    if (!upserted.isEmpty())
      response.put("upserted", upserted);
    markOkay(response);
    return response;
  }

  private int executeUpdate(final String collectionName, final Document q, final Document u, final boolean multi) {
    if (!database.getSchema().existsType(collectionName) || u == null)
      return 0;

    final StringBuilder sql = new StringBuilder("UPDATE `").append(collectionName).append('`');
    appendUpdateOperations(sql, u);
    appendWhere(sql, q);
    if (!multi)
      sql.append(" LIMIT 1");

    return executeCount(sql.toString());
  }

  private ObjectId executeUpsert(final String collectionName, final Document q, final Document u) {
    final MutableDocument record = database.newDocument(database.getSchema().getOrCreateDocumentType(collectionName).getName());

    // Seed the new document with the filter's top-level equalities, mirroring MongoDB upsert.
    if (q != null)
      for (final Map.Entry<String, Object> entry : q.entrySet()) {
        final String field = entry.getKey();
        if (field.startsWith("$"))
          continue;
        final Object value = entry.getValue();
        if (value instanceof Document opDoc) {
          if (opDoc.containsKey("$eq"))
            record.set(field, opDoc.get("$eq"));
        } else
          record.set(field, value);
      }

    if (isReplacement(u)) {
      for (final Map.Entry<String, Object> entry : u.entrySet())
        record.set(entry.getKey(), entry.getValue());
    } else {
      applyOperatorsToDocument(record, u);
    }

    final ObjectId id = new ObjectId();
    record.set("_id", toHexString(id));
    record.save();
    return id;
  }

  private void applyOperatorsToDocument(final MutableDocument record, final Document u) {
    for (final Map.Entry<String, Object> entry : u.entrySet()) {
      final String op = entry.getKey();
      final Document operand = (Document) entry.getValue();
      switch (op) {
      case "$set" -> {
        for (final Map.Entry<String, Object> f : operand.entrySet())
          record.set(f.getKey(), f.getValue());
      }
      case "$unset" -> {
        for (final String f : operand.keySet())
          record.set(f, null);
      }
      case "$inc" -> {
        for (final Map.Entry<String, Object> f : operand.entrySet()) {
          final Number current = (Number) record.get(f.getKey());
          final Number delta = (Number) f.getValue();
          record.set(f.getKey(), current == null ? delta : current.doubleValue() + delta.doubleValue());
        }
      }
      default -> throw new UnsupportedOperationException("Unsupported update operator '" + op + "'");
      }
    }
  }

  private void appendUpdateOperations(final StringBuilder sql, final Document u) {
    if (isReplacement(u)) {
      sql.append(" CONTENT ").append(documentToJson(u));
      return;
    }

    for (final Map.Entry<String, Object> entry : u.entrySet()) {
      final String op = entry.getKey();
      final Document operand = (Document) entry.getValue();
      switch (op) {
      case "$set" -> sql.append(" MERGE ").append(documentToJson(operand));
      case "$unset" -> {
        sql.append(" REMOVE ");
        int i = 0;
        for (final String field : operand.keySet()) {
          if (i++ > 0)
            sql.append(", ");
          sql.append('`').append(field).append('`');
        }
      }
      case "$inc" -> {
        for (final Map.Entry<String, Object> f : operand.entrySet())
          sql.append(" SET `").append(f.getKey()).append("` += ").append((Number) f.getValue());
      }
      default -> throw new UnsupportedOperationException("Unsupported update operator '" + op + "'");
      }
    }
  }

  private static boolean isReplacement(final Document u) {
    for (final String key : u.keySet())
      if (key.startsWith("$"))
        return false;
    return true;
  }

  private void appendWhere(final StringBuilder sql, final Document q) {
    if (q != null && !q.isEmpty()) {
      sql.append(" WHERE ");
      MongoDBToSqlTranslator.buildExpression(sql, q);
    }
  }

  private int executeCount(final String sql) {
    try (final ResultSet rs = database.command("sql", sql)) {
      if (rs.hasNext()) {
        final Number count = rs.next().getProperty("count");
        if (count != null)
          return count.intValue();
      }
    }
    return 0;
  }

  private static JSONObject documentToJson(final Document doc) {
    final JSONObject json = new JSONObject();
    for (final Map.Entry<String, Object> entry : doc.entrySet())
      json.put(entry.getKey(), toJsonValue(entry.getValue()));
    return json;
  }

  private static Object toJsonValue(final Object value) {
    if (value instanceof Document document)
      return documentToJson(document);
    else if (value instanceof List<?> list) {
      final JSONArray array = new JSONArray();
      for (final Object item : list)
        array.put(toJsonValue(item));
      return array;
    } else if (value instanceof ObjectId id)
      return toHexString(id);
    return value;
  }

  private static String toHexString(final ObjectId id) {
    final byte[] bytes = id.toByteArray();
    final StringBuilder s = new StringBuilder(bytes.length * 2);
    for (final byte b : bytes)
      s.append("%02x".formatted(b));
    return s.toString();
  }

  private Document responseOk() {
    final Document response = new Document();
    markOkay(response);
    return response;
  }

  private int getOptionalNumber(final Document query, final String fieldName, final int defaultValue) {
    final Number limitNumber = (Number) query.get(fieldName);
    return limitNumber != null ? limitNumber.intValue() : defaultValue;
  }

  private synchronized void clearLastStatus(final Channel channel) {
    if (channel == null)
      // EMBEDDED CALL WITHOUT THE SERVER
      return;

    final List<Document> results = this.lastResults.computeIfAbsent(channel, k -> new ArrayList<>(10));
    results.add(null);
  }

  private synchronized void putLastResult(final Channel channel, final Document result) {
    final List<Document> results = this.lastResults.get(channel);
    final Document last = results.getLast();
    if (last != null)
      throw new IllegalStateException("last result already set: " + last);
    results.set(results.size() - 1, result);
  }

  private void putLastError(final Channel channel, final MongoServerException ex) {
    final Document error = new Document();
    if (ex instanceof MongoServerError err) {
      error.put("err", err.getMessage());
      error.put("code", err.getCode());
      error.putIfNotNull("codeName", err.getCodeName());
    } else {
      error.put("err", ex.getMessage());
    }

    error.put("connectionId", channel.id().asShortText());
    this.putLastResult(channel, error);
  }
}
