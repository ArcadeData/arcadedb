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
import com.arcadedb.database.RID;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.TypeIndexBuilder;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
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
import de.bwaldvogel.mongo.bson.BsonRegularExpression;
import de.bwaldvogel.mongo.bson.Decimal128;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.ObjectId;
import de.bwaldvogel.mongo.exception.ErrorCode;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.oplog.Oplog;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import io.netty.channel.Channel;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;

import static de.bwaldvogel.mongo.backend.Utils.markOkay;

public class MongoDBDatabaseWrapper implements MongoDatabase {
  /**
   * The largest array index {@code $set} may reach: the gap is padded with nulls in memory, so an unbounded index is a way to
   * exhaust the heap with one request.
   */
  private static final int MAX_ARRAY_PADDING = 100_000;

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
      throw wireException("Error on executing MongoDB '" + command + "' command", e);
    } finally {
      ProtocolContext.clear();
    }
  }

  public ResultSet query(final String query) throws MongoServerException {
    final JSONObject queryJson = new JSONObject(query);

    final String collection = queryJson.getString("collection");
    final int numberToSkip = queryJson.getInt("numberToSkip", 0);
    final int numberToReturn = queryJson.getInt("numberToReturn", 0);
    final JSONObject q = queryJson.getJSONObject("query");

    final Document transformedQuery = json2Document(q);

    final MongoQuery mongoQuery = new MongoQuery(null, null, getFullCollectionNamespace(collection), numberToSkip, numberToReturn,
        transformedQuery, null);

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
      throw wireException("Error on executing MongoDB query", e);
    }
  }

  /**
   * Wraps a failure so the client is told whose fault it was (issue #5628). Every failure used to become a bare
   * {@link MongoServerException}, and {@link #putLastError} can only emit {@code code}/{@code codeName} for a
   * {@link MongoServerError} - so a caller who divided by zero or hit a unique index got an uncoded error
   * indistinguishable from the server having broken.
   * <p>
   * The classification itself lives in {@link ErrorCategory} so every wire protocol answers it the same way; only
   * the translation into MongoDB's error codes is here. The categories MongoDB has no code for keep the uncoded
   * exception they already had.
   * <p>
   * WriteConflict, NamespaceNotFound, Unauthorized and MaxTimeMSExpired are given as literals because the bundled
   * {@code de.bwaldvogel} {@link ErrorCode} enum does not define them; the rest come from the enum.
   */
  static MongoServerException wireException(final String message, final Exception e) {
    // The bundled backend assigns its own codes - an aggregation stage rejecting a pipeline, for instance. Those
    // are already more precise than anything classification could infer, and re-wrapping them dropped the code
    // entirely, so the client saw an uncoded error. Keep what the backend decided.
    if (e instanceof MongoServerError alreadyCoded)
      return alreadyCoded;

    return switch (ErrorCategory.of(e)) {
      case RETRY -> new MongoServerError(112, "WriteConflict", message, e);
      case ARITHMETIC, VALIDATION -> new MongoServerError(ErrorCode.BadValue.getValue(), ErrorCode.BadValue.getName(), message, e);
      case DUPLICATED_KEY ->
          new MongoServerError(ErrorCode.DuplicateKey.getValue(), ErrorCode.DuplicateKey.getName(), message, e);
      case SCHEMA -> new MongoServerError(26, "NamespaceNotFound", message, e);
      case SECURITY -> new MongoServerError(13, "Unauthorized", message, e);
      case PARSING -> new MongoServerError(ErrorCode.FailedToParse.getValue(), ErrorCode.FailedToParse.getName(), message, e);
      case TIMEOUT -> new MongoServerError(50, "MaxTimeMSExpired", message, e);
      case NOT_FOUND, SERVER -> new MongoServerException(message, e);
    };
  }

  private Document aggregateCollection(final String command, final Document document, final Oplog oplog)
      throws MongoServerException {
    final String collectionName = document.get("aggregate").toString();
    // like MongoDB, a pipeline over a collection that does not exist is answered with no documents (countDocuments is one). A
    // pipeline that writes ($out, $merge) is not read-only: it keeps failing on the missing collection, as it always did
    if (!database.getSchema().existsType(collectionName)) {
      if (writesTo(document.get("pipeline")) || startsWithChangeStream(document.get("pipeline")))
        throw new MongoServerError(26, "NamespaceNotFound", "ns does not exist: " + getFullCollectionNamespace(collectionName));
      // accepted deviation: a stage that needs no input collection ($documents, $collStats) answers empty here
      // the pipeline is still validated: a malformed stage is an error whether or not the collection exists
      final List<Document> missingPipeline = Aggregation.parse(Aggregation.parse(document.get("pipeline")));
      Aggregation.fromPipeline(missingPipeline, plugin, this, null, oplog).validate(document);
      return firstBatchCursorResponse(collectionName, "firstBatch", new ArrayList<>(), 0);
    }

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

  private static boolean startsWithChangeStream(final Object pipeline) {
    return pipeline instanceof List<?> stages && !stages.isEmpty() && stages.getFirst() instanceof Document first
        && first.containsKey("$changeStream");
  }

  private static boolean writesTo(final Object pipeline) {
    if (pipeline instanceof List<?> stages)
      for (final Object stage : stages)
        if (stage instanceof Document document && (document.containsKey("$out") || document.containsKey("$merge")))
          return true;
    return false;
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
    MongoDBCollectionWrapper.forgetIdIndexes(database);
    database.drop();
  }

  @Override
  public void dropCollection(final String collectionName, final Oplog opLog) {
    database.getSchema().dropType(collectionName);
    MongoDBCollectionWrapper.forgetIdIndex(database, collectionName);
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

    // MongoDB always reports the _id index. The collections this plugin creates carry a real one once a document is
    // inserted, but a type made through SQL or Studio may not: offset the count by 1 in that case so numIndexesAfter == numIndexesBefore +
    // <new indexes> stays consistent for clients that check it.
    final int numIndexesBefore = countIndexes(collectionName);

    final List<Document> indexes = (List<Document>) document.get("indexes");
    if (indexes != null)
      for (final Document index : indexes)
        createSingleIndex(collectionName, index);

    final int numIndexesAfter = countIndexes(collectionName);

    final Document response = responseOk();
    response.put("createdCollectionAutomatically", createdCollectionAutomatically);
    response.put("numIndexesBefore", numIndexesBefore);
    response.put("numIndexesAfter", numIndexesAfter);
    return response;
  }

  private int countIndexes(final String collectionName) {
    final DocumentType type = database.getSchema().getType(collectionName);
    return type.getAllIndexes(false).size() + (MongoDBCollectionWrapper.hasUniqueIdIndex(type) ? 0 : 1);
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
    final Document sort = (Document) document.get("sort");
    // A missing/0 limit means "no limit" (a -1 sentinel here would be interpreted as a legacy single-batch limit of 1)
    final int limit = this.getOptionalNumber(document, "limit", 0);
    final int skip = this.getOptionalNumber(document, "skip", 0);
    final String collectionName = (String) document.get("find");

    // MongoDBCollectionWrapper#handleQuery(QueryParameters) only ever reads an order-by out of the legacy
    // "$orderBy" wire modifier embedded in the query document itself; the modern find command's own "sort" field
    // is otherwise never consulted. Thread it through using that same convention.
    final Document queryPayload;
    if (sort != null && !sort.isEmpty()) {
      queryPayload = new Document();
      queryPayload.put("$query", filter != null ? filter : new Document());
      queryPayload.put("$orderBy", sort);
    } else
      queryPayload = filter;

    final Document projection = document.get("projection") instanceof Document p && !p.isEmpty() ? p : null;
    if (projection != null)
      // reject an invalid projection up front, even when no document would be projected
      MongoDBToSqlTranslator.isInclusionProjection(projection, "_id");

    final MongoQuery mongoQuery = new MongoQuery(null, null, getFullCollectionNamespace(collectionName), skip, limit, queryPayload, null);

    final QueryResult result = handleQuery(mongoQuery);

    final List<Document> documents = new ArrayList<>();
    for (Iterator<Document> it = result.iterator(); it.hasNext(); ) {
      final Document next = it.next();
      documents.add(projection != null ? MongoDBToSqlTranslator.projectDocument(next, projection, "_id") : next);
    }

    return firstBatchCursorResponse(collectionName, "firstBatch", documents, 0);
  }

  private Document insertDocument(final Channel channel, final Document query) throws MongoServerException {
    final String collectionName = query.get("insert").toString();
    // MongoDB's default is an ordered insert: it stops at the first failing document
    final boolean isOrdered = query.get("ordered") == null || Utils.isTrue(query.get("ordered"));
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
          try {
            // FAST PATH: the whole batch in one transaction
            collection.insertDocuments(documents);
            n = documents.size();
          } catch (final RuntimeException e) {
            if (ErrorCategory.of(e) != ErrorCategory.DUPLICATED_KEY)
              throw e;

            // The batch was rolled back. MongoDB is not atomic across a batch, so insert the documents one by one to
            // report each duplicate by index and keep the others (ordered: only the ones before the first failure)
            for (int i = 0; i < documents.size(); i++) {
              final Document doc = documents.get(i);
              try {
                collection.insertDocuments(List.of(doc));
                ++n;
              } catch (final RuntimeException duplicate) {
                if (ErrorCategory.of(duplicate) != ErrorCategory.DUPLICATED_KEY)
                  throw duplicate;

                final Document error = new Document();
                error.put("index", i);
                error.put("code", ErrorCode.DuplicateKey.getValue());
                error.put("codeName", ErrorCode.DuplicateKey.getName());
                error.put("errmsg", duplicateKeyMessage(collectionName, duplicate));
                writeErrors.add(error);
                if (isOrdered)
                  break;
              }
            }
          }

          final Document result = new Document("n", n);
          this.putLastResult(channel, result);
        }
      } catch (final MongoServerError var7) {
        this.putLastError(channel, var7);
        throw var7;
      }
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

  /**
   * Built from the index and key the engine reports, so a violation of a user-defined unique index is not presented as an
   * {@code _id} one.
   */
  private String duplicateKeyMessage(final String collectionName, final Throwable error) {
    for (Throwable t = error; t != null; t = t.getCause())
      if (t instanceof DuplicatedKeyException duplicated)
        return "E11000 duplicate key error collection: " + getFullCollectionNamespace(collectionName) + " index: " + duplicated.getIndexName()
            + " dup key: " + (plugin != null && plugin.isProductionMode() ? ArcadeDBServer.CONCEALED_DUPLICATED_KEYS : duplicated.getKeys());
    return "E11000 duplicate key error collection: " + getFullCollectionNamespace(collectionName);
  }

  private MongoCollection<Long> getOrCreateCollection(final String collectionName) {
    MongoCollection<Long> collection = collections.get(collectionName);
    if (collection == null || !database.getSchema().existsType(collectionName)) {
      // like MongoDB, the first insert creates the collection, with the unique index on _id every collection has
      if (!database.getSchema().existsType(collectionName))
        database.getSchema().buildDocumentType().withName(collectionName).withTotalBuckets(1).withIgnoreIfExists(true).create();
      collection = new MongoDBCollectionWrapper(database, collectionName);
      collections.put(collectionName, collection);
    }
    return collection;
  }

  /**
   * Handles the MongoDB {@code delete} command (used by the driver's {@code deleteOne} /
   * {@code deleteMany} helpers). Each delete spec carries a filter {@code q} and a {@code limit}:
   * a limit of 1 deletes only the first match ({@code deleteOne}), 0 deletes all matches
   * ({@code deleteMany}). The records to delete are the ones the filter selects, as in find (see {@link MongoFilter}).
   */
  private Document deleteDocuments(final Document document) {
    final String collectionName = document.get("delete").toString();
    final List<Document> deletes = (List<Document>) document.get("deletes");

    int n = 0;
    if (database.getSchema().existsType(collectionName) && deletes != null) {
      final Lock indexLock = MongoDBCollectionWrapper.idIndexLock(database, collectionName).readLock();
      indexLock.lock();
      database.begin();
      try {
        // one regex budget for the whole command, whatever the number of entries
        final MongoFilter.RegexBudget budget = MongoFilter.RegexBudget.of(database);
        for (final Document del : deletes) {
          final Document q = (Document) del.get("q");
          final Number limit = (Number) del.get("limit");
          final boolean single = limit != null && limit.intValue() == 1;

          final MongoFilter filter = new MongoFilter(database, q, budget);
          if (filter.isEmpty()) {
            final Map<String, Object> params = new HashMap<>();
            final StringBuilder sql = new StringBuilder("DELETE FROM ").append(Identifier.quote(collectionName));
            filter.appendWhere(sql, params);
            if (single)
              sql.append(" LIMIT 1");

            n += executeCount(sql.toString(), params);
          } else
            n += deleteRecords(filter.select(database, collectionName, single ? 1 : 0));
        }
        database.commit();
      } catch (final RuntimeException e) {
        database.rollback();
        throw e;
      } finally {
        indexLock.unlock();
      }
    } else if (deletes != null) {
      // nothing to delete from, but an invalid filter is an error whether or not the collection exists
      final MongoFilter.RegexBudget budget = MongoFilter.RegexBudget.of(database);
      for (final Document del : deletes)
        new MongoFilter(database, (Document) del.get("q"), budget);
    }

    final Document response = new Document("n", n);
    markOkay(response);
    return response;
  }

  /**
   * Handles the MongoDB {@code update} command (used by {@code updateOne}, {@code updateMany},
   * {@code replaceOne} and {@code replaceMany}). The update document {@code u} is either a full
   * replacement (no {@code $}-prefixed keys) or a set of update operators ({@code $set}, {@code $unset},
   * {@code $inc}). The {@code multi} flag selects between updating the first match only and all matches; the matches are the
   * records the filter selects (see {@link MongoFilter}), each updated in turn. When {@code upsert} is set and
   * nothing matched, a new document seeded from the filter's equalities and the update is inserted.
   */
  private Document updateDocuments(final Document document) {
    final String collectionName = document.get("update").toString();
    final List<Document> updates = (List<Document>) document.get("updates");

    int n = 0;
    int nModified = 0;
    final List<Document> upserted = new ArrayList<>();

    if (updates != null) {
      // one regex budget for the whole command, whatever the number of entries
      final MongoFilter.RegexBudget budget = MongoFilter.RegexBudget.of(database);
      // a schema change cannot happen inside the transaction: an upsert creates the collection with its _id index up front
      prepareUpsertIdIndex(collectionName, updates, budget);

      // the transaction maintains the unique _id index, which must not be dropped and rebuilt under it
      final Lock indexLock = MongoDBCollectionWrapper.idIndexLock(database, collectionName).readLock();
      indexLock.lock();
      database.begin();
      try {
        int index = 0;
        for (final Document upd : updates) {
          final Document q = (Document) upd.get("q");
          final Document u = (Document) upd.get("u");
          final boolean multi = Utils.isTrue(upd.get("multi"));
          final boolean upsert = Utils.isTrue(upd.get("upsert"));

          final int updated = executeUpdate(collectionName, q, u, multi, budget);
          n += updated;
          nModified += updated;

          if (updated == 0 && upsert) {
            final Object id = executeUpsert(collectionName, q, u);
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
      } finally {
        indexLock.unlock();
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

  /**
   * A schema change cannot happen inside the transaction, so an upsert creates the collection and makes the {@code _id} index
   * able to hold the key the upsert is about to store, up front: the filter's scalar {@code _id}, or the generated ObjectId (a
   * string kind) of an upsert without one. Only an upsert that matches nothing inserts, so the others leave the index alone
   * (a rebuild is a full copy of it).
   */
  private void prepareUpsertIdIndex(final String collectionName, final List<Document> updates, final MongoFilter.RegexBudget budget) {
    List<Object> samples = null;
    for (final Document upd : updates) {
      if (!Utils.isTrue(upd.get("upsert")))
        continue;

      final Document q = upd.get("q") instanceof Document filter ? filter : new Document();
      Object id = q.get("_id");
      boolean hasId = q.containsKey("_id");
      // the stored _id can also come from the update itself: a replacement's, or $set's
      final Document u = upd.get("u") instanceof Document update ? update : new Document();
      final Document setter = isReplacement(u) ? u : u.get("$set") instanceof Document set ? set : null;
      if (setter != null && setter.containsKey("_id")) {
        id = setter.get("_id");
        hasId = true;
      }
      // an operator document says nothing about the key of the document that would be inserted, but for $eq: without it the
      // upsert stores a generated ObjectId
      if (id instanceof Document operators) {
        id = operators.get("$eq");
        hasId = operators.containsKey("$eq");
      }

      database.getSchema().getOrCreateDocumentType(collectionName);
      final Object sample = hasId && id != null ? id : new ObjectId();
      // nothing to do when the index already holds this kind of key: no need to look for a match either
      // for a key other than the _id this is a scan until an index can narrow it (issue #9162), and the update scans again
      if (MongoDBCollectionWrapper.idIndexSatisfies(database, collectionName, List.of(sample)) || matchesAny(collectionName, q, budget))
        continue;

      if (samples == null)
        samples = new ArrayList<>();
      samples.add(sample);
    }

    if (samples != null)
      MongoDBCollectionWrapper.ensureIdIndexLocked(database, collectionName, samples);
  }

  /**
   * Advisory: it only decides whether to touch the index ahead of the transaction, the match can change before it starts.
   */
  private boolean matchesAny(final String collectionName, final Document q, final MongoFilter.RegexBudget budget) {
    return !new MongoFilter(database, q, budget).select(database, collectionName, 1).isEmpty();
  }

  private int executeUpdate(final String collectionName, final Document q, final Document u, final boolean multi,
      final MongoFilter.RegexBudget budget) {
    // built first: an invalid filter is an error whether or not the collection exists
    final MongoFilter filter = new MongoFilter(database, q, budget);
    if (!database.getSchema().existsType(collectionName) || u == null)
      return 0;

    // Every filtered update is applied to each record the filter selects (the matcher verifies the candidates, so the answer is exact
    // for any shape of data). Only an update without a filter stays one SQL UPDATE.
    // a filter the SQL cannot answer exactly (see MongoFilter) also selects the records itself
    if (isReplacement(u) || setsDottedPath(u) || touchesId(u) || !filter.isEmpty())
      return executeUpdateOnRecords(collectionName, filter, u, multi);

    final Map<String, Object> params = new HashMap<>();
    final StringBuilder sql = new StringBuilder("UPDATE ").append(Identifier.quote(collectionName));
    appendUpdateOperations(sql, params, u);
    filter.appendWhere(sql, params);
    if (!multi)
      sql.append(" LIMIT 1");

    return executeCount(sql.toString(), params);
  }

  /**
   * Whether an update operator targets the {@code _id}: MongoDB's {@code _id} is immutable, which only the record path can check
   * against the stored value.
   */
  private static boolean touchesId(final Document u) {
    for (final Object operand : u.values())
      if (operand instanceof Document fields && fields.containsKey("_id"))
        return true;
    return false;
  }

  /**
   * An operator may set the {@code _id} to the value it already has, but never change or remove it (error 66).
   */
  private static void checkIdUntouched(final MutableDocument record, final Document u) {
    final Object stored = record.get("_id");
    for (final Map.Entry<String, Object> op : u.entrySet()) {
      if (!(op.getValue() instanceof Document fields) || !fields.containsKey("_id"))
        continue;
      if (!"$set".equals(op.getKey()) || (stored != null && !sameId(stored, idValue(fields.get("_id")))))
        throw new MongoServerError(66, "ImmutableField", "Performing an update on the path '_id' would modify the immutable field '_id'");
    }
  }

  /**
   * Whether any operator targets a dotted path. All of them go through the record path together, so {@code $set}, {@code $unset}
   * and {@code $inc} behave the same (nested paths created, integral {@code $inc} kept integral) however they are combined.
   */
  private static boolean setsDottedPath(final Document u) {
    for (final Object operand : u.values())
      if (operand instanceof Document fields)
        for (final String field : fields.keySet())
          if (field.indexOf('.') >= 0)
            return true;
    return false;
  }

  private int executeUpdateOnRecords(final String collectionName, final MongoFilter filter, final Document u, final boolean multi) {
    // collect the RIDs first (the records are modified while the result set would still be open on them), and load each record
    // in turn: only identities are held on the heap, not every matching document
    final List<RID> rids = filter.select(database, collectionName, multi ? 0 : 1);

    final boolean replacement = isReplacement(u);
    int vanished = 0;
    for (final RID rid : rids) {
      final MutableDocument record;
      try {
        record = rid.asDocument().modify();
      } catch (final RecordNotFoundException e) {
        // deleted since the select: it no longer matches, like a single statement would not have touched it
        ++vanished;
        continue;
      }
      if (replacement)
        replaceContent(record, u);
      else {
        checkIdUntouched(record, u);
        applyOperatorsToDocument(record, u);
      }
      record.save();
    }
    return rids.size() - vanished;
  }

  /**
   * Replaces every property of the record with the replacement's, but the {@code _id}: MongoDB's {@code _id} is immutable, so a
   * replacement without one keeps the stored value and a replacement with a different one is refused (error 66).
   */
  private static void replaceContent(final MutableDocument record, final Document replacement) {
    final Object storedId = record.get("_id");
    final Object newId = replacement.containsKey("_id") ? idValue(replacement.get("_id")) : storedId;
    if (storedId != null && !sameId(storedId, newId))
      throw new MongoServerError(66, "ImmutableField", "After applying the update, the (immutable) field '_id' was found to have been altered");

    for (final String name : new ArrayList<>(record.getPropertyNames()))
      if (!"_id".equals(name))
        record.remove(name);

    if (newId != null)
      record.set("_id", newId);
    for (final Map.Entry<String, Object> entry : replacement.entrySet())
      if (!"_id".equals(entry.getKey()))
        record.set(entry.getKey(), toMapValue(entry.getValue()));
  }

  private Object executeUpsert(final String collectionName, final Document q, final Document u) {
    final MutableDocument record = database.newDocument(database.getSchema().getOrCreateDocumentType(collectionName).getName());

    // Tracks the _id's own BSON type as seeded, independently of the hex string it ends up stored as: a client
    // that filtered on an ObjectId gets an ObjectId back, but a client that filtered on a plain 24-hex-char String
    // (indistinguishable from an ObjectId's hex once stored) must get that String type back, not a promoted one.
    boolean idIsObjectId = false;

    // Seed the new document with the filter's top-level equalities, mirroring MongoDB upsert.
    if (q != null)
      for (final Map.Entry<String, Object> entry : q.entrySet()) {
        final String field = entry.getKey();
        if (field.startsWith("$"))
          continue;
        final Object value = entry.getValue();
        if (value instanceof Document opDoc) {
          if (opDoc.containsKey("$eq")) {
            final Object eqValue = opDoc.get("$eq");
            // a regex in the filter is a pattern to match, not a value to insert
            if (eqValue instanceof BsonRegularExpression)
              continue;
            if ("_id".equals(field) && eqValue instanceof ObjectId)
              idIsObjectId = true;
            record.set(field, MongoBsonValues.toStored(field, eqValue));
          }
        } else {
          if (value instanceof BsonRegularExpression)
            continue;
          if ("_id".equals(field) && value instanceof ObjectId)
            idIsObjectId = true;
          record.set(field, MongoBsonValues.toStored(field, value));
        }
      }

    if (isReplacement(u)) {
      for (final Map.Entry<String, Object> entry : u.entrySet()) {
        final Object value = entry.getValue();
        // Last write wins, matching record.set() itself: a replacement re-specifying "_id" overrides whatever type
        // the filter seeded, rather than only ever promoting it to true and leaking a stale ObjectId flag if the
        // replacement's own _id is some other type.
        if ("_id".equals(entry.getKey()))
          idIsObjectId = value instanceof ObjectId;
        record.set(entry.getKey(), MongoBsonValues.toStored(entry.getKey(), value));
      }
    } else {
      final Boolean setIdIsObjectId = applyOperatorsToDocument(record, u);
      if (setIdIsObjectId != null)
        idIsObjectId = setIdIsObjectId;
    }

    // has(), not get() == null: an explicit "_id": null in the filter/update is a legal (if unusual) BSON _id and
    // must be preserved, whereas a genuinely absent _id needs one generated.
    if (!record.has("_id")) {
      record.set("_id", new ObjectId().getHexData());
      idIsObjectId = true;
    }

    record.save();

    final Object id = record.get("_id");
    return idIsObjectId && id instanceof String hex ? new ObjectId(hex) : id;
  }

  /**
   * Applies the update operators to the record being upserted, returning whether an ObjectId-typed {@code _id} was
   * seeded through {@code $set} - mirroring the same tracking the filter-seeding loop and the replacement branch in
   * {@link #executeUpsert} already do, so the response reports {@code _id} back as an ObjectId regardless of which
   * branch supplied it. {@code null} means {@code $set} never touched {@code _id} at all, so the caller should keep
   * whatever the filter-seeding loop already determined instead of overriding it - unlike a plain {@code boolean}
   * OR, this lets a non-{@code ObjectId} {@code $set}-supplied {@code _id} correctly override (rather than leak
   * through) a {@code true} the filter already seeded.
   */
  private Boolean applyOperatorsToDocument(final MutableDocument record, final Document u) {
    Boolean idIsObjectId = null;
    for (final Map.Entry<String, Object> entry : u.entrySet()) {
      final String op = entry.getKey();
      final Document operand = (Document) entry.getValue();
      switch (op) {
      case "$set" -> {
        for (final Map.Entry<String, Object> f : operand.entrySet()) {
          final Object value = f.getValue();
          if ("_id".equals(f.getKey()))
            idIsObjectId = value instanceof ObjectId;
          setPath(record, f.getKey(), "_id".equals(f.getKey()) ? idValue(value) : toMapValue(value));
        }
      }
      case "$unset" -> {
        for (final String f : operand.keySet())
          unsetPath(record, f);
      }
      case "$inc" -> {
        for (final Map.Entry<String, Object> f : operand.entrySet())
          // toStored: a Decimal128 delta added to a missing field is a Number that must still become a DECIMAL
          setPath(record, f.getKey(), MongoBsonValues.toStored(add(numberOf(getPath(record, f.getKey())), numberOf(f.getValue()))));
      }
      default -> throw new UnsupportedOperationException("Unsupported update operator '" + op + "'");
      }
    }
    return idIsObjectId;
  }

  /**
   * Appends the update clauses for a BSON update document, binding every value it carries into {@code params} rather than
   * spelling it into {@code sql}. Field names are the exception: SQL cannot bind a property name, so {@code $unset} and
   * {@code $inc} quote theirs as identifiers.
   * <p>
   * {@code params} must be empty on entry and hold only generated placeholders afterwards: names are derived from the map's
   * current size, so a pre-seeded map would silently reuse one. A caller that also appends a WHERE clause must share this
   * same map, which continues the numbering instead of colliding with it.
   */
  static void appendUpdateOperations(final StringBuilder sql, final Map<String, Object> params, final Document u) {
    assert params.isEmpty();

    if (isReplacement(u)) {
      sql.append(" CONTENT ");
      MongoDBToSqlTranslator.bindStored(sql, params, documentToMap(u, true));
      return;
    }

    for (final Map.Entry<String, Object> entry : u.entrySet()) {
      final String op = entry.getKey();
      final Document operand = (Document) entry.getValue();
      switch (op) {
      case "$set" -> {
        sql.append(" MERGE ");
        MongoDBToSqlTranslator.bindStored(sql, params, documentToMap(operand, true));
      }
      case "$unset" -> {
        sql.append(" REMOVE ");
        int i = 0;
        for (final String field : operand.keySet()) {
          if (i++ > 0)
            sql.append(", ");
          sql.append(MongoDBToSqlTranslator.quoteFieldPath(field));
        }
      }
      case "$inc" -> {
        for (final Map.Entry<String, Object> f : operand.entrySet()) {
          sql.append(" SET ").append(MongoDBToSqlTranslator.quoteFieldPath(f.getKey())).append(" += ");
          MongoDBToSqlTranslator.buildValue(sql, params, (Number) f.getValue());
        }
      }
      default -> throw new UnsupportedOperationException("Unsupported update operator '" + op + "'");
      }
    }
  }

  /**
   * Sets a value on a possibly dotted path, creating the embedded documents on the way like MongoDB's {@code $set}. An embedded
   * map of the record is never modified in place: it is copied, so the record sees a changed property.
   */
  private static void setPath(final MutableDocument record, final String path, final Object value) {
    final int dot = path.indexOf('.');
    if (dot < 0)
      record.set(path, value);
    else {
      final String head = path.substring(0, dot);
      record.set(head, setNested(record.get(head), path.substring(dot + 1), value));
    }
  }

  @SuppressWarnings("unchecked")
  private static Object setNested(final Object container, final String path, final Object value) {
    final int dot = path.indexOf('.');
    final String head = dot < 0 ? path : path.substring(0, dot);

    if (container instanceof List<?> list && isArrayIndex(head)) {
      final int index = Integer.parseInt(head);
      if (index > MAX_ARRAY_PADDING)
        throw new MongoServerError(ErrorCode.BadValue.getValue(), ErrorCode.BadValue.getName(),
            "Cannot create field '" + head + "' in an array: the index is too large");
      final List<Object> copy = new ArrayList<>(list);
      while (copy.size() <= index)
        copy.add(null);
      copy.set(index, dot < 0 ? value : setNested(copy.get(index), path.substring(dot + 1), value));
      return copy;
    }

    requireEmbedded(container, head);
    final Map<String, Object> copy = copyOfEmbedded(container);
    copy.put(head, dot < 0 ? value : setNested(copy.get(head), path.substring(dot + 1), value));
    return copy;
  }

  private static void unsetPath(final MutableDocument record, final String path) {
    final int dot = path.indexOf('.');
    if (dot < 0)
      record.remove(path);
    else {
      final String head = path.substring(0, dot);
      final Object container = record.get(head);
      if (container != null)
        record.set(head, unsetNested(container, path.substring(dot + 1)));
    }
  }

  @SuppressWarnings("unchecked")
  private static Object unsetNested(final Object container, final String path) {
    final int dot = path.indexOf('.');
    final String head = dot < 0 ? path : path.substring(0, dot);

    if (container instanceof List<?> list) {
      if (!isArrayIndex(head) || Integer.parseInt(head) >= list.size())
        return container;
      // MongoDB sets an unset array element to null instead of shifting the others
      final List<Object> copy = new ArrayList<>(list);
      final int index = Integer.parseInt(head);
      copy.set(index, dot < 0 ? null : unsetNested(copy.get(index), path.substring(dot + 1)));
      return copy;
    }

    // MongoDB leaves a scalar alone on $unset of a path below it
    if (!isEmbedded(container))
      return container;

    final Map<String, Object> copy = copyOfEmbedded(container);
    if (dot < 0)
      copy.remove(head);
    else if (copy.get(head) != null)
      copy.put(head, unsetNested(copy.get(head), path.substring(dot + 1)));
    return copy;
  }

  private static Object getPath(final MutableDocument record, final String path) {
    final int dot = path.indexOf('.');
    if (dot < 0)
      return record.get(path);

    Object current = record.get(path.substring(0, dot));
    for (final String segment : path.substring(dot + 1).split("\\.")) {
      if (current instanceof Map<?, ?> map)
        current = map.get(segment);
      else if (current instanceof List<?> list && isArrayIndex(segment) && Integer.parseInt(segment) < list.size())
        current = list.get(Integer.parseInt(segment));
      else if (current instanceof com.arcadedb.database.Document embedded)
        current = embedded.get(segment);
      else
        return null;
    }
    return current;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> copyOfEmbedded(final Object container) {
    if (container instanceof Map<?, ?> map)
      return new LinkedHashMap<>((Map<String, Object>) map);
    if (container instanceof com.arcadedb.database.Document embedded) {
      final Map<String, Object> copy = new LinkedHashMap<>(embedded.toMap());
      copy.keySet().removeIf(MongoDBToSqlTranslator::isRecordMetadata);
      return copy;
    }
    return new LinkedHashMap<>();
  }

  /**
   * MongoDB refuses to create a field below a scalar or an array (with a non-numeric key) instead of replacing it: doing so
   * here would silently destroy the stored value.
   */
  private static void requireEmbedded(final Object container, final String field) {
    if (container != null && !isEmbedded(container))
      throw new MongoServerError(28, "PathNotViable", "Cannot create field '" + field + "' in element {" + container + "}");
  }

  /**
   * MongoDB compares numbers by value, so an {@code _id} of 2 and one of 2L or 2.0 are the same.
   */
  private static boolean sameId(final Object stored, final Object replacement) {
    if (stored instanceof Number a && replacement instanceof Number b)
      return a.doubleValue() == b.doubleValue();
    return Objects.equals(stored, replacement);
  }

  private static Number numberOf(final Object value) {
    if (value == null || value instanceof Number)
      return (Number) value;
    throw new MongoServerError(14, "TypeMismatch", "Cannot apply $inc to a value of non-numeric type");
  }

  private static boolean isEmbedded(final Object value) {
    return value instanceof Map || value instanceof com.arcadedb.database.Document;
  }

  private static boolean isArrayIndex(final String segment) {
    // like MongoDB, "01" is a field name and not the index 1
    if (segment.isEmpty() || segment.length() > 9 || (segment.length() > 1 && segment.charAt(0) == '0'))
      return false;
    for (int i = 0; i < segment.length(); i++)
      if (segment.charAt(i) < '0' || segment.charAt(i) > '9')
        return false;
    return true;
  }

  /**
   * {@code $inc}: integral operands stay integral (an int that overflows becomes a long), anything else is a double.
   */
  private static Number add(final Number current, final Number delta) {
    if (current == null)
      return delta;
    if (isIntegral(current) && isIntegral(delta)) {
      final long sum;
      try {
        sum = Math.addExact(current.longValue(), delta.longValue());
      } catch (final ArithmeticException e) {
        // like MongoDB, an overflowing long becomes a double
        return current.doubleValue() + delta.doubleValue();
      }
      return current instanceof Long || delta instanceof Long || sum > Integer.MAX_VALUE || sum < Integer.MIN_VALUE ? (Number) sum : (Number) (int) sum;
    }
    if (current instanceof BigDecimal || delta instanceof BigDecimal || current instanceof Decimal128 || delta instanceof Decimal128)
      return MongoBsonValues.toBigDecimal(current).add(MongoBsonValues.toBigDecimal(delta));
    return current.doubleValue() + delta.doubleValue();
  }

  private static boolean isIntegral(final Number number) {
    return number instanceof Integer || number instanceof Long || number instanceof Short || number instanceof Byte;
  }

  private static boolean isReplacement(final Document u) {
    for (final String key : u.keySet())
      if (key.startsWith("$"))
        return false;
    return true;
  }

  private int deleteRecords(final List<RID> rids) {
    int deleted = 0;
    for (final RID rid : rids)
      try {
        rid.asDocument().delete();
        deleted++;
      } catch (final RecordNotFoundException e) {
        // deleted since the selection: it is not this command's to count
      }
    return deleted;
  }

  private int executeCount(final String sql, final Map<String, Object> params) {
    try (final ResultSet rs = database.command("sql", sql, params)) {
      if (rs.hasNext()) {
        final Number count = rs.next().getProperty("count");
        if (count != null)
          return count.intValue();
      }
    }
    return 0;
  }

  /**
   * An {@code _id} value in its stored form: an ObjectId is its hex string.
   */
  private static Object idValue(final Object value) {
    return MongoBsonValues.toStored("_id", value);
  }

  /**
   * Converts a BSON document into the map bound as the payload of {@code UPDATE ... MERGE} / {@code ... CONTENT}. Insertion
   * order is preserved so a replacement document reaches the record in wire order.
   */
  private static Map<String, Object> documentToMap(final Document doc, final boolean topLevel) {
    final Map<String, Object> map = LinkedHashMap.newLinkedHashMap(doc.size());
    for (final Map.Entry<String, Object> entry : doc.entrySet())
      map.put(entry.getKey(), topLevel && "_id".equals(entry.getKey()) ? MongoBsonValues.toStored("_id", entry.getValue()) : toMapValue(entry.getValue()));
    return map;
  }

  private static Object toMapValue(final Object value) {
    if (value instanceof Document document) {
      MongoBsonValues.checkNotReserved(document);
      return documentToMap(document, false);
    } else if (value instanceof List<?> list) {
      final List<Object> converted = new ArrayList<>(list.size());
      for (final Object item : list)
        converted.add(toMapValue(item));
      return converted;
    }
    return MongoBsonValues.toStored(value);
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
