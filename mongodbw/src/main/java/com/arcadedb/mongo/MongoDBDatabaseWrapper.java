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
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoCollection;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.CollectionOptions;
import de.bwaldvogel.mongo.backend.Cursor;
import de.bwaldvogel.mongo.backend.CursorRegistry;
import de.bwaldvogel.mongo.backend.QueryResult;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.backend.aggregation.Aggregation;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.oplog.Oplog;
import de.bwaldvogel.mongo.wire.message.MongoDelete;
import de.bwaldvogel.mongo.wire.message.MongoInsert;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import de.bwaldvogel.mongo.wire.message.MongoUpdate;
import io.netty.channel.Channel;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

import static de.bwaldvogel.mongo.backend.Utils.markOkay;

public class MongoDBDatabaseWrapper implements MongoDatabase {
  protected final Database                           database;
  protected final MongoBackend                       backend;
  protected final Map<String, MongoCollection<Long>> collections    = new ConcurrentHashMap();
  protected final Map<Channel, List<Document>>       lastResults    = new ConcurrentHashMap();
  protected final CursorRegistry                     cursorRegistry = new CursorRegistry();

  public MongoDBDatabaseWrapper(final Database database, final MongoBackend backend) {
    this.database = database;
    this.backend = backend;

    for (final DocumentType dt : database.getSchema().getTypes()) {
      collections.put(dt.getName(), new MongoDBCollectionWrapper(database, dt.getName()));
    }
  }

  public static MongoDBDatabaseWrapper open(final Database database) {
    return new MongoDBDatabaseWrapper(database, null);
  }

  @Override
  public String getDatabaseName() {
    return database.getName();
  }

  @Override
  public void handleClose(final Channel channel) {
    database.close();
  }

  @Override
  public Document handleCommand(final Channel channel, final String command, final Document document, final Oplog opLog) throws MongoServerException {
    try {
      if (command.equalsIgnoreCase("create"))
        return createCollection(document);
      else if (command.equalsIgnoreCase("count"))
        return countCollection(document);
      else if (command.equalsIgnoreCase("insert"))
        return insertDocument(channel, document);
      else if (command.equalsIgnoreCase("aggregate"))
        return aggregateCollection(command, document, opLog);
      else {
        LogManager.instance().log(this, Level.SEVERE, "Received unsupported command from MongoDB client '%s', (document=%s)", null, command, document);
        throw new UnsupportedOperationException(String.format("Received unsupported command from MongoDB client '%s', (document=%s)", command, document));
      }
    } catch (final Exception e) {
      throw new MongoServerException("Error on executing MongoDB '" + command + "' command", e);
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
        final Document doc = super.next().getProperty("value");

        final Map<String, Object> values = new HashMap<>(doc.size());
        values.putAll(doc);

        return new ResultInternal(values);
      }
    };

    return resultset;
  }

  private Document json2Document(final JSONObject map) {
    final Document doc = new Document();

    for (final String k : map.keySet()) {
      Object v = map.get(k);
      if (v instanceof JSONObject)
        v = json2Document((JSONObject) v);
      else if (v instanceof JSONArray) {
        final List<Object> array = new ArrayList<>(((JSONArray) v).length());

        for (int i = 0; i < ((JSONArray) v).length(); i++) {
          Object a = ((JSONArray) v).get(i);
          if (a instanceof JSONObject)
            a = json2Document((JSONObject) a);

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
      final MongoCollection<Long> collection = collections.get(collectionName);
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

  @Override
  public void handleInsert(final MongoInsert mongoInsert, final Oplog opLog) {
    // TODO
  }

  @Override
  public void handleDelete(final MongoDelete mongoDelete, final Oplog opLog) {
    // TODO
  }

  @Override
  public void handleUpdate(final MongoUpdate mongoUpdate, final Oplog opLog) {
    // TODO
  }

  private Document aggregateCollection(final String command, final Document document, final Oplog oplog) throws MongoServerException {
    final String collectionName = document.get("aggregate").toString();
    database.countType(collectionName, false);

    final MongoCollection<Long> collection = collections.get(collectionName);

    final Object pipelineObject = Aggregation.parse(document.get("pipeline"));
    final List<Document> pipeline = Aggregation.parse(pipelineObject);
    if (!pipeline.isEmpty()) {
      final Document changeStream = (Document) pipeline.get(0).get("$changeStream");
      if (changeStream != null) {
        final Aggregation aggregation = Aggregation.fromPipeline(pipeline.subList(1, pipeline.size()), this, collection, oplog);
        aggregation.validate(document);
        return commandChangeStreamPipeline(document, oplog, collectionName, changeStream, aggregation);
      }
    }
    final Aggregation aggregation = Aggregation.fromPipeline(pipeline, this, collection, oplog);
    aggregation.validate(document);

    return firstBatchCursorResponse(collectionName, "firstBatch", aggregation.computeResult(), 0);
  }

  private Document firstBatchCursorResponse(final String ns, final String key, final List<Document> documents, final long cursorId) {
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

  private Document commandChangeStreamPipeline(final Document query, final Oplog oplog, final String collectionName, final Document changeStreamDocument,
      final Aggregation aggregation) {
    final Document cursorDocument = (Document) query.get("cursor");
    final int batchSize = (int) cursorDocument.getOrDefault("batchSize", 0);

    final String namespace = getFullCollectionNamespace(collectionName);
    final Cursor cursor = oplog.createCursor(changeStreamDocument, namespace, aggregation);
    return firstBatchCursorResponse(namespace, "firstBatch", cursor.takeDocuments(batchSize), cursor.getId());
  }

  private Document createCollection(final Document document) {
    database.getSchema().createDocumentType((String) document.get("create"), 1);
    return responseOk();
  }

  private Document countCollection(final Document document) throws MongoServerException {
    final String collectionName = document.get("count").toString();
    database.countType(collectionName, false);

    final Document response = responseOk();

    final MongoCollection<Long> collection = collections.get(collectionName);

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
          n = collection.insertDocuments(documents).size();

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
    final Document last = results.get(results.size() - 1);
    if (last != null)
      throw new IllegalStateException("last result already set: " + last);
    results.set(results.size() - 1, result);
  }

  private void putLastError(final Channel channel, final MongoServerException ex) {
    final Document error = new Document();
    if (ex instanceof MongoServerError) {
      final MongoServerError err = (MongoServerError) ex;
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
