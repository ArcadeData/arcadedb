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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.OwnTransaction;
import com.arcadedb.exception.CauseChain;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.log.LogManager;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.TimeSeriesType;
import com.arcadedb.schema.TypeIndexBuilder;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ErrorConcealment;
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
import de.bwaldvogel.mongo.backend.aggregation.stage.ProjectStage;
import de.bwaldvogel.mongo.bson.BsonRegularExpression;
import de.bwaldvogel.mongo.bson.Decimal128;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.ObjectId;
import de.bwaldvogel.mongo.exception.ErrorCode;
import de.bwaldvogel.mongo.exception.FailedToOptimizePipelineError;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.exception.TypeMismatchException;
import de.bwaldvogel.mongo.oplog.Oplog;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import io.netty.channel.Channel;

import java.math.BigDecimal;
import java.time.temporal.Temporal;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.logging.Level;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static de.bwaldvogel.mongo.backend.Utils.markOkay;

public class MongoDBDatabaseWrapper implements MongoDatabase {
  // how many times a command on a single record selects again when its candidate was changed or deleted by a concurrent commit in between
  private static final int SINGLE_RECORD_ATTEMPTS = 3;

  /**
   * The largest array index {@code $set} may reach: the gap is padded with nulls in memory, so an unbounded index is a way to
   * exhaust the heap with one request.
   */
  private static final int MAX_ARRAY_PADDING = 100_000;

  /** What {@code executeUpsert} answers for a document a listener refused: {@code null} is a legal {@code _id}, so it cannot. */
  private static final Object UPSERT_VETOED = new Object();
  /** What {@code equalityOf} answers for a condition that infers no value: {@code null} is a value an upsert seeds. */
  private static final Object NO_EQUALITY = new Object();
  /** Stands for a property the record does not have, which differs from one it has with the value {@code null}. */
  private static final Object ABSENT        = new Object();
  /** What {@link #getPath} answers for a path that reaches no stored value, told apart from a stored null. */
  private static final Object MISSING       = new Object();
  /**
   * The fields of a {@code createIndexes} specification this plugin honours ({@code key}, {@code name}, {@code unique}) or that
   * carry no semantics a built index could miss ({@code background} is a build-scheduling hint, {@code v} the index format
   * version, {@code ns} the legacy namespace echo).
   */
  private static final Set<String> SUPPORTED_INDEX_OPTIONS = Set.of("key", "name", "unique", "background", "v", "ns");
  /** Boolean index options whose {@code false} is MongoDB's default, so asking for it asks for nothing this plugin lacks. */
  private static final Set<String> INDEX_OPTIONS_ACCEPTED_WHEN_FALSE = Set.of("sparse", "hidden");

  private static final int          DOCUMENT_VALIDATION_FAILURE     = 121;
  private static final int          NOT_IMPLEMENTED                 = 238;
  private static final int          COMMAND_NOT_FOUND               = 59;
  private static final int          TYPE_MISMATCH                   = 14;
  /** What mongod answers a command that lacks a field it requires. */
  private static final int          MISSING_FIELD                   = 40414;
  private static final int          PATH_NOT_VIABLE                 = 28;
  private static final int          IMMUTABLE_FIELD                 = 66;
  private static final int          CONFLICTING_UPDATE_OPERATORS    = 40;
  /** What the query matcher answers a {@code $and}/{@code $or}/{@code $nor} whose elements are not documents. */
  private static final int          MALFORMED_LOGICAL_OPERATOR      = 14817;
  /** The update operators this plugin applies. */
  private static final Set<String>  SUPPORTED_UPDATE_OPERATORS      = Set.of("$set", "$unset", "$inc");
  private static final String       SUPPORTED_UPDATE_OPERATORS_TEXT = "the supported ones are $set, $unset and $inc";
  /** The update operators mongod has and this plugin does not apply: refused as not implemented, not as unknown (issue #11449). */
  private static final Set<String>  MONGO_UPDATE_OPERATORS          = Set.of("$currentDate", "$min", "$max", "$mul", "$rename",
      "$setOnInsert", "$addToSet", "$pop", "$pull", "$push", "$pullAll", "$bit");
  /**
   * The codes of a refusal mongod reports as an entry of {@code writeErrors} of an {@code insert} or {@code update}, going on with the
   * other statements (issue #11444): the statement's own documents or values are refused (BadValue, FailedToParse, TypeMismatch,
   * PathNotViable, ImmutableField, DocumentValidationFailure, NotImplemented, DuplicateKey, a malformed {@code $and}/{@code $or},
   * ConflictingUpdateOperators, and NotSingleValueField of an upsert whose filter cannot infer the document it would insert, issue
   * #12071).
   */
  private static final Set<Integer> STATEMENT_ERROR_CODES           = Set.of(ErrorCode.BadValue.getValue(), ErrorCode.FailedToParse.getValue(),
      TYPE_MISMATCH, PATH_NOT_VIABLE, IMMUTABLE_FIELD, DOCUMENT_VALIDATION_FAILURE, NOT_IMPLEMENTED, ErrorCode.DuplicateKey.getValue(),
      MALFORMED_LOGICAL_OPERATOR, CONFLICTING_UPDATE_OPERATORS, ErrorCode.NotSingleValueField.getValue());

  // THE INSTANCE THE SERVER SERVES UNDER THIS NAME: REPLACED BY bind() WHEN A RESTORE, A CLOSE + OPEN OR A DROP + CREATE SWAPS IT
  protected volatile Database                        database;
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
   * Points this wrapper at {@code live}, the instance the server serves under this name now (issue #10562). The backend caches
   * one wrapper per name for as long as it runs, while the server replaces the instance behind the name on a restore with
   * {@code overwrite}, on a {@code close database} followed by {@code open database} and on a {@code drop database} followed by
   * a {@code create database}: a wrapper that kept the instance it was built around answered every later command with
   * {@code DatabaseIsClosedException} until the server restarted. The backend calls this with the instance it resolved for the
   * request, before the request runs.
   * <p>
   * The collection wrappers and the {@code _id} index bookkeeping are kept per instance and are dropped with the old one; the
   * index bookkeeping is keyed by the database path, which the new instance shares. Costs a volatile read when nothing changed.
   */
  void bind(final Database live) {
    if (database == live)
      return;
    synchronized (this) {
      final Database previous = database;
      if (previous == live)
        return;
      // FORGOTTEN BEFORE THE NEW INSTANCE IS PUBLISHED: A REQUEST THAT SEES IT MUST NOT TAKE AN _id INDEX LOCK THIS THEN DISCARDS
      MongoDBCollectionWrapper.forgetIdIndexes(previous);
      database = live;
      // CLEARED AFTER: A COLLECTION WRAPPER CACHED MEANWHILE WAS BUILT AROUND ONE OF THE TWO, AND GOES EITHER WAY
      collections.clear();
    }
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
    // NOT TAGGED HERE: MongoDBBackend TAGS EVERY ENTRY POINT THE LIBRARY CALLS WITH A CLIENT'S REQUEST, THIS ONE AMONG THEM
    // (ISSUE #10390)
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
        // a client's request, not a server fault: the operator learns which commands clients ask for, without the document, which can
        // carry stored data
        LogManager.instance().log(this, Level.INFO, "Received unsupported command '%s' from MongoDB client", null, command);
        // what mongod answers a command it does not have, and what drivers detect a command missing from a server by (issue #11449)
        throw new MongoServerError(COMMAND_NOT_FOUND, "CommandNotFound", "no such command: '" + command + "'");
      }
    } catch (final Exception e) {
      throw wireException("Error on executing MongoDB '" + command + "' command", e);
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
   * WriteConflict, NotWritablePrimary, NamespaceNotFound, Unauthorized, MaxTimeMSExpired, TemporarilyUnavailable and
   * DocumentValidationFailure are given as literals
   * because the bundled {@code de.bwaldvogel} {@link ErrorCode} enum does not define them; the rest come from the enum.
   */
  static MongoServerException wireException(final String message, final Exception e) {
    // The bundled backend assigns its own codes - an aggregation stage rejecting a pipeline, for instance. Those
    // are already more precise than anything classification could infer, and re-wrapping them dropped the code
    // entirely, so the client saw an uncoded error. Keep what the backend decided.
    if (e instanceof MongoServerError alreadyCoded)
      return alreadyCoded;

    // A follower refusing leader-only work, naming the leader (issue #9845). Its NeedRetryException ancestry would make
    // it a WriteConflict, which a driver retries in place - on the one node that will refuse again. NotWritablePrimary is
    // what MongoDB itself answers a write sent to a secondary with, and a driver answers it by marking this server
    // Unknown and rescanning the topology. The wording keeps "not primary", which older drivers match on, and names the
    // leader so a client without topology discovery, or an operator, has the address to act on.
    final ServerIsNotTheLeaderException notTheLeader = ServerIsNotTheLeaderException.redirectIn(e);
    if (notTheLeader != null)
      return new MongoServerError(10107, "NotWritablePrimary",
          message + ": not primary. " + notTheLeader.leaderSentence(), e);

    // A value a constraint the schema declares refuses (issue #11314): what MongoDB answers a document its collection's validator
    // refuses, where the category alone says BadValue
    final ErrorCategory category = ErrorCategory.of(e);
    if (category == ErrorCategory.VALIDATION && CauseChain.find(e, ValidationException.class) instanceof ValidationException refused
        && refused.getKind() != null && refused.getKind().isDeclaredConstraint())
      return new MongoServerError(DOCUMENT_VALIDATION_FAILURE, "DocumentValidationFailure", message, e);

    return switch (category) {
      case RETRY -> new MongoServerError(112, "WriteConflict", message, e);
      case ARITHMETIC, VALIDATION -> new MongoServerError(ErrorCode.BadValue.getValue(), ErrorCode.BadValue.getName(), message, e);
      case DUPLICATED_KEY ->
          new MongoServerError(ErrorCode.DuplicateKey.getValue(), ErrorCode.DuplicateKey.getName(), message, e);
      // A taken name (issue #10153): what MongoDB answers a createCollection on an existing collection
      case ALREADY_EXISTS -> new MongoServerError(48, "NamespaceExists", message, e);
      case SCHEMA -> new MongoServerError(26, "NamespaceNotFound", message, e);
      case SECURITY -> new MongoServerError(13, "Unauthorized", message, e);
      case PARSING -> new MongoServerError(ErrorCode.FailedToParse.getValue(), ErrorCode.FailedToParse.getName(), message, e);
      case TIMEOUT -> new MongoServerError(50, "MaxTimeMSExpired", message, e);
      // Stopped on request (issue #9689): what MongoDB answers for an operation a killOp interrupted
      case TERMINATED -> new MongoServerError(11601, "Interrupted", message, e);
      // A database the server has and cannot serve now (issue #10553): what MongoDB answers while it cannot take the request, and
      // the one code a driver may retry later without the topology changing (NotWritablePrimary and the like would have it drop
      // the server, and with it every other database that is perfectly fine)
      case UNAVAILABLE -> new MongoServerError(365, "TemporarilyUnavailable", message, e);
      // A name that is no database (issue #10795): MongoDB creates a database on first write and has no code of its own for one
      // that is missing, so it is the namespace one, the code MongoDB answers a command on a namespace that does not exist with
      case DATABASE_NOT_FOUND -> new MongoServerError(26, "NamespaceNotFound", message, e);
      case NOT_FOUND, SERVER -> new MongoServerException(message, e);
    };
  }

  private Document aggregateCollection(final String command, final Document document, final Oplog oplog)
      throws MongoServerException {
    MongoCommandFields.check("aggregate", document);
    final String collectionName = document.get("aggregate").toString();
    // like MongoDB, a pipeline over a collection that does not exist is answered with no documents (countDocuments is one). A
    // pipeline that writes ($out, $merge) is not read-only: it keeps failing on the missing collection, as it always did
    if (!database.getSchema().existsType(collectionName)) {
      if (writesTo(document.get("pipeline")) || startsWithChangeStream(document.get("pipeline")))
        throw new MongoServerError(26, "NamespaceNotFound", "ns does not exist: " + getFullCollectionNamespace(collectionName));
      // accepted deviation: a stage that needs no input collection ($documents, $collStats) answers empty here
      // the pipeline is still validated: a malformed stage is an error whether or not the collection exists
      final List<Document> missingPipeline = boundedPipeline(document.get("pipeline"));
      Aggregation.fromPipeline(missingPipeline, plugin, this, null, oplog).validate(document);
      return firstBatchCursorResponse(collectionName, "firstBatch", new ArrayList<>(), 0);
    }

    final MongoCollection<Long> collection = getCollection(collectionName);

    final List<Document> pipeline = boundedPipeline(document.get("pipeline"));
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

    return firstBatchCursorResponse(collectionName, "firstBatch", computeResult(aggregation, pipeline, collection, oplog), 0);
  }

  /**
   * The documents of a pipeline, its top-level {@code $match} stages tested with {@link MongoQueryMatcher} (issue #12074): the
   * library's own match stage answers differently from {@code mongod}, and {@code countDocuments} is a pipeline. The stages between
   * two {@code $match} run as the library runs them, on the stream the previous part produced; the failures are reported as
   * {@link Aggregation#computeResult()} reports them. A {@code $match} nested in a sub-pipeline ({@code $facet}, {@code $lookup},
   * {@code $unionWith}) is still the library's.
   */
  private List<Document> computeResult(final Aggregation aggregation, final List<Document> pipeline, final MongoCollection<?> collection,
      final Oplog oplog) {
    boolean matches = false;
    for (final Document stage : pipeline)
      if (stage.get("$match") instanceof Document) {
        matches = true;
        break;
      }
    if (!matches)
      return aggregation.computeResult();

    // a malformed $match is refused before any document is read, as mongod refuses it when it parses the pipeline
    for (final Document stage : pipeline)
      if (stage.size() == 1 && stage.get("$match") instanceof Document query)
        new MongoQueryMatcher().validate(query);

    try {
      Stream<Document> stream = collection.queryAllAsStream();
      List<Document> segment = new ArrayList<>();
      for (final Document stage : pipeline) {
        if (stage.size() == 1 && stage.get("$match") instanceof Document query) {
          stream = runSegment(segment, stream, collection, oplog);
          segment = new ArrayList<>();
          final MongoQueryMatcher matcher = new MongoQueryMatcher();
          stream = stream.filter(candidate -> matcher.matches(candidate, query));
        } else
          segment.add(stage);
      }
      return runSegment(segment, stream, collection, oplog).collect(Collectors.toList());
    } catch (final TypeMismatchException | FailedToOptimizePipelineError | ProjectStage.InvalidProjectException e) {
      throw e;
    } catch (final MongoServerError e) {
      if (e.hasCode(ErrorCode._15998, ErrorCode._40353))
        throw e;
      throw new MongoServerError(e.getCode(), e.getCodeName(),
          "PlanExecutor error during aggregation :: caused by :: " + e.getMessageWithoutErrorCode(), e);
    }
  }

  private Stream<Document> runSegment(final List<Document> segment, final Stream<Document> stream, final MongoCollection<?> collection,
      final Oplog oplog) {
    return segment.isEmpty() ? stream : Aggregation.fromPipeline(segment, plugin, this, collection, oplog).runStagesAsStream(stream);
  }

  /**
   * The pipeline of an aggregate command with its regular expressions bounded by {@code arcadedb.command.regexTimeout}, one budget
   * for the whole command (#9164).
   */
  private List<Document> boundedPipeline(final Object pipeline) {
    return MongoFilter.boundPipeline(Aggregation.parse(Aggregation.parse(pipeline)), MongoFilter.RegexBudget.ofAggregation(database));
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
    // refused before anything is created: a capped, validated or collated collection would be an ordinary one (issue #11778)
    MongoCommandFields.check("create", document);
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

    final Object indexesValue = document.get("indexes");
    if (indexesValue != null && !(indexesValue instanceof List))
      throw new MongoServerError(ErrorCode.FailedToParse.getValue(), ErrorCode.FailedToParse.name(),
          "The 'indexes' field of createIndexes must be an array of index specifications");
    final List<Document> indexes = (List<Document>) indexesValue;
    // VALIDATE EVERY SPECIFICATION BEFORE TOUCHING THE SCHEMA: A REFUSED BATCH CREATES NEITHER THE COLLECTION NOR ANY OF ITS
    // INDEXES, SO THE CLIENT IS NOT LEFT WITH HALF OF WHAT IT ASKED FOR (ISSUE #11446)
    if (indexes != null)
      for (final Object index : indexes) {
        if (!(index instanceof Document specification))
          throw new MongoServerError(ErrorCode.FailedToParse.getValue(), ErrorCode.FailedToParse.name(),
              "Each element of the 'indexes' field of createIndexes must be a document, found: " + index);
        validateIndexSpecification(specification);
      }

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

  /**
   * Refuses an index specification this plugin cannot honour, instead of acknowledging it with {@code ok:1} and building a plain
   * ascending index that does something else (issue #11446). Only {@link #SUPPORTED_INDEX_OPTIONS} are accepted, plus the
   * boolean options in {@link #INDEX_OPTIONS_ACCEPTED_WHEN_FALSE} when they ask for the default; every key must be an ascending
   * or descending direction, since the special index kinds ({@code text}, {@code hashed}, {@code 2dsphere}, ...) would be built
   * as ordinary range indexes. A partial filter, a TTL or a collation would change which documents the index holds or how it
   * compares them, so building the index without them makes it reject documents MongoDB accepts or keep documents MongoDB
   * removes.
   */
  static void validateIndexSpecification(final Document index) {
    final Object keyValue = index.get("key");
    if (!(keyValue instanceof Document key) || key.isEmpty())
      throw new MongoServerError(ErrorCode.FailedToParse.getValue(), ErrorCode.FailedToParse.name(),
          "The 'key' field is a required property of an index specification and must be a non-empty document");

    for (final String option : index.keySet()) {
      if (SUPPORTED_INDEX_OPTIONS.contains(option))
        continue;
      if (INDEX_OPTIONS_ACCEPTED_WHEN_FALSE.contains(option) && Boolean.FALSE.equals(index.get(option)))
        continue;
      throw new MongoServerError(67, "CannotCreateIndex",
          "Index option '" + option + "' is not supported by ArcadeDB: the index would be created without it");
    }

    for (final String field : key.keySet()) {
      // the property the plugin keeps the key of a unique index in is not a field a client indexes (issue #12073)
      MongoKeys.checkNotKey(field);
      final Object direction = key.get(field);
      if (!(direction instanceof Number number) || number.doubleValue() == 0D || Double.isNaN(number.doubleValue()))
        throw new MongoServerError(67, "CannotCreateIndex",
            "Index kind " + (direction instanceof String ? "'" + direction + "'" : String.valueOf(direction)) + " on field '" + field
                + "' is not supported by ArcadeDB: only ascending (1) and descending (-1) keys are");
    }
  }

  private void createSingleIndex(final String collectionName, final Document index) {
    final Document key = (Document) index.get("key");

    final DocumentType type = database.getSchema().getType(collectionName);
    final String[] propertyNames = key.keySet().toArray(new String[0]);

    if (Utils.isTrue(index.get("unique"))) {
      // a unique index is built on the derived key of its fields (issue #12073): an index on the fields themselves converts every
      // value to one key type, so 1 and "1" would be one key. The documents already stored get their keys first, under the
      // collection's exclusive lock so no write of the plugin goes in between
      final String property = MongoKeys.propertyOf(propertyNames);
      final Lock lock = MongoDBCollectionWrapper.idIndexLock(database, collectionName).writeLock();
      lock.lock();
      try {
        // the unique index of the _id is the one every collection has, built on its own key
        if (MongoKeys.ID_KEY.equals(property))
          MongoDBCollectionWrapper.ensureIdIndex(database, collectionName);
        else
          MongoDBCollectionWrapper.ensureUniqueKeyIndex(database, collectionName, property, propertyNames);
      } finally {
        lock.unlock();
      }
      return;
    }

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

    final TypeIndexBuilder builder = database.getSchema().buildTypeIndex(collectionName, propertyNames);
    builder.withType(Schema.INDEX_TYPE.LSM_TREE);
    builder.withIgnoreIfExists(true);
    if (anyUndeclared)
      builder.withDefaultKeyTypesForUndeclaredProperties(defaultKeyTypes);
    builder.create();
  }

  /**
   * Handles the MongoDB {@code count} command (what {@code count()} and {@code estimatedDocumentCount()} send). A collection that does
   * not exist counts 0, as on {@code mongod}, with its filter still validated (issue #11449). The count is a long: a type holds more
   * documents than an int counts, and the reply carries it as {@code mongod} does, an int32 while it fits and an int64 past it.
   */
  private Document countCollection(final Document document) throws MongoServerException {
    MongoCommandFields.check("count", document);
    final String collectionName = document.get("count").toString();
    final Document queryObject = documentField(document, "query");
    // read first: an invalid option is an error whether or not the collection exists
    final long limit = getOptionalLong(document, "limit", -1L);
    final long skip = getOptionalLong(document, "skip", 0L);

    final Document response = responseOk();
    if (getCollection(collectionName) instanceof MongoDBCollectionWrapper collection)
      response.put("n", wireNumber(collection.countDocuments(queryObject, skip, limit)));
    else {
      // nothing to count, but an invalid filter is an error whether or not the collection exists
      new MongoFilter(database, queryObject);
      response.put("n", 0);
    }
    return response;
  }

  /** A count as {@code mongod} puts it in a reply: an int32 while it fits, an int64 past it. */
  static Number wireNumber(final long value) {
    return value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE ? (Number) (int) value : (Number) value;
  }

  private Document find(final Document document) throws MongoServerException {
    // a field this would not read is refused, not ignored: the answer would be the one of another command (issue #11778)
    MongoCommandFields.check("find", document);
    final Document filter = documentField(document, "filter");
    final Document sort = documentField(document, "sort");
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

    // parsed up front, once: an invalid projection is refused even when no document would be projected (issue #12072)
    final Document projectionFields = documentField(document, "projection");
    final MongoProjection projection = projectionFields != null && !projectionFields.isEmpty() ? new MongoProjection(projectionFields) : null;

    final MongoQuery mongoQuery = new MongoQuery(null, null, getFullCollectionNamespace(collectionName), skip, limit, queryPayload, null);

    final QueryResult result = handleQuery(mongoQuery);

    final List<Document> documents = new ArrayList<>();
    for (Iterator<Document> it = result.iterator(); it.hasNext(); ) {
      final Document next = it.next();
      documents.add(projection != null ? projection.apply(next) : next);
    }

    return firstBatchCursorResponse(collectionName, "firstBatch", documents, 0);
  }

  /**
   * An optional field of a command that must be a document, as {@code mongod} declares it: a value of another type is refused
   * (TypeMismatch), instead of being ignored or failing with an uncoded error.
   */
  private static Document documentField(final Document command, final String field) {
    final Object value = command.get(field);
    if (value == null || value instanceof Document)
      return (Document) value;
    throw new MongoServerError(TYPE_MISMATCH, "TypeMismatch",
        "BSON field '" + field + "' is the wrong type '" + typeName(value) + "', expected type 'object'");
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
        } else if (database.getSchema().getTypeOrNull(collectionName) instanceof TimeSeriesType timeSeries) {
          // its samples are not records, so there is no _id index to keep: they go to the engine of the type (issue #10476)
          n = MongoTimeSeriesInserts.insert(database, timeSeries, documents, isOrdered, writeErrors);
          this.putLastResult(channel, new Document("n", n));
        } else {
          final MongoDBCollectionWrapper collection = getOrCreateCollection(collectionName);
          try {
            // FAST PATH: the whole batch in one transaction. n is what the engine stored, not what it was given (issue #9974)
            n = collection.insertDocumentsCountingStored(documents);
          } catch (final RuntimeException e) {
            // a document refused on its own (a duplicated key, a declared constraint, a value that cannot be stored) is reported in
            // writeErrors with the others kept, as mongod does (issue #11444): anything else is the command's failure
            if (statementErrorCode(e) == null)
              throw e;

            // The batch was rolled back. MongoDB is not atomic across a batch, so insert the documents one by one to
            // report each refusal by index and keep the others (ordered: only the ones before the first failure)
            for (int i = 0; i < documents.size(); i++) {
              final Document doc = documents.get(i);
              try {
                n += collection.insertDocumentsCountingStored(List.of(doc));
              } catch (final RuntimeException refused) {
                final Document error = writeError(i, collectionName, refused);
                if (error == null)
                  throw refused;
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

  private MongoDBCollectionWrapper getOrCreateCollection(final String collectionName) {
    if (collections.get(collectionName) instanceof MongoDBCollectionWrapper cached && database.getSchema().existsType(collectionName))
      return cached;

    // like MongoDB, the first insert creates the collection, with the unique index on _id every collection has
    if (!database.getSchema().existsType(collectionName))
      database.getSchema().buildDocumentType().withName(collectionName).withTotalBuckets(1).withIgnoreIfExists(true).create();
    final MongoDBCollectionWrapper collection = new MongoDBCollectionWrapper(database, collectionName);
    collections.put(collectionName, collection);
    return collection;
  }

  /**
   * Handles the MongoDB {@code delete} command (used by the driver's {@code deleteOne} /
   * {@code deleteMany} helpers). Each delete spec carries a filter {@code q} and a {@code limit}:
   * a limit of 1 deletes only the first match ({@code deleteOne}), 0 deletes all matches
   * ({@code deleteMany}). The records to delete are the ones the filter selects, as in find (see {@link MongoFilter}).
   * <p>
   * Every entry is checked before any runs, as {@code mongod} parses the command before it executes it (issue #11778): a field the
   * plugin would ignore (a {@code collation}) and a {@code limit} that is neither 0 nor 1, which used to delete every match, refuse
   * the whole command with nothing deleted.
   */
  private Document deleteDocuments(final Document document) {
    MongoCommandFields.check("delete", document);
    final String collectionName = document.get("delete").toString();
    final List<Document> deletes = (List<Document>) document.get("deletes");
    if (deletes != null)
      for (final Object entry : deletes) {
        final Document del = statementOf("delete", "deletes", entry);
        MongoCommandFields.check("delete.deletes", del);
        deletesOne(del);
      }

    int n = 0;
    if (database.getSchema().existsType(collectionName) && deletes != null) {
      final Lock indexLock = MongoDBCollectionWrapper.idIndexLock(database, collectionName).readLock();
      indexLock.lock();
      // the command's OWN transaction: a failure rolls back this one, never one the caller holds (issue #9977)
      final OwnTransaction tx = OwnTransaction.begin((DatabaseInternal) database);
      try {
        // one regex budget for the whole command, whatever the number of entries
        final MongoFilter.RegexBudget budget = MongoFilter.RegexBudget.of(database);
        // the samples of a TIMESERIES type have no identity for the record path to select: they are deleted by the engine of the type,
        // by time range and tags, or the delete is refused naming the type (issue #10464)
        if (database.getSchema().getTypeOrNull(collectionName) instanceof TimeSeriesType timeSeries)
          n = deleteSamples(timeSeries, deletes, budget);
        else
          for (final Document del : deletes) {
            final Document q = (Document) del.get("q");
            final boolean single = deletesOne(del);

            final MongoFilter filter = new MongoFilter(database, q, budget);
            if (filter.isEmpty()) {
              final Map<String, Object> params = new HashMap<>();
              final StringBuilder sql = new StringBuilder("DELETE FROM ").append(Identifier.quote(collectionName));
              if (single)
                sql.append(" LIMIT 1");

              n += executeCount(sql.toString(), params);
            } else
              n += deleteMatching(collectionName, filter, single);
          }
        tx.commit();
      } finally {
        tx.rollbackIfMine();
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
   * Whether an entry of {@code delete} removes one document ({@code limit: 1}) or every match ({@code limit: 0}), the two values
   * {@code mongod} has: any other one is refused as it refuses it, where it used to mean "every match" (issue #11778).
   */
  private static boolean deletesOne(final Document entry) {
    final Object limit = entry.get("limit");
    if (limit == null)
      throw new MongoServerError(MISSING_FIELD, "Location" + MISSING_FIELD, "BSON field 'delete.deletes.limit' is missing but a required field");
    if (!(limit instanceof Number number))
      throw new MongoServerError(TYPE_MISMATCH, "TypeMismatch",
          "BSON field 'delete.deletes.limit' is the wrong type '" + typeName(limit) + "', expected type 'number'");
    final boolean none = isExactly(number, Decimal128.POSITIVE_ZERO, 0D);
    if (!none && !isExactly(number, Decimal128.ONE, 1D))
      throw new MongoServerError(ErrorCode.FailedToParse.getValue(), ErrorCode.FailedToParse.getName(),
          "The limit field in delete objects must be 0 or 1. Got " + limit);
    return !none;
  }

  /**
   * Whether a number of any BSON type is exactly {@code value}. A Decimal128 is compared as a decimal: narrowed to a double, one as
   * small as 1E-6176 is 0, which would turn a refused limit into "delete every match".
   */
  private static boolean isExactly(final Number number, final Decimal128 decimalValue, final double value) {
    if (number instanceof Decimal128 decimal)
      // compareTo, not equals: 1.0 and 1 are the same number. NaN and the infinities compare as the doubles they are, equal to neither
      return decimal.compareTo(decimalValue) == 0;
    return number.doubleValue() == value;
  }

  /** A statement of a write command, which must be a document: {@code mongod} refuses anything else when it parses the command. */
  private static Document statementOf(final String command, final String field, final Object statement) {
    if (statement instanceof Document document)
      return document;
    throw new MongoServerError(TYPE_MISMATCH, "TypeMismatch",
        "BSON field '" + command + "." + field + "' is the wrong type '" + typeName(statement) + "', expected type 'object'");
  }

  /**
   * Deletes the samples of a TIMESERIES type each entry names (issue #10464), see {@link MongoTimeSeriesWrites}. Every entry is
   * translated before any runs: the engine's delete is not part of the transaction, so an entry refused after another ran would
   * leave that one applied. A failure while an entry runs (an I/O error of the engine) still leaves the entries before it applied:
   * the command is not atomic across its entries, as MongoDB's own unordered deletes are not.
   */
  private int deleteSamples(final TimeSeriesType type, final List<Document> deletes, final MongoFilter.RegexBudget budget) {
    final List<String> statements = new ArrayList<>(deletes.size());
    final List<Map<String, Object>> statementParams = new ArrayList<>(deletes.size());
    for (final Document del : deletes) {
      final Document q = (Document) del.get("q");
      // an invalid filter is the error MongoDB answers first, whatever the collection
      new MongoFilter(database, q, budget);
      final Map<String, Object> params = new HashMap<>();
      statements.add(MongoTimeSeriesWrites.deleteStatement(type, q, deletesOne(del), params));
      statementParams.add(params);
    }

    int deleted = 0;
    for (int i = 0; i < statements.size(); i++)
      deleted += executeCount(statements.get(i), statementParams.get(i));
    return deleted;
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
    MongoCommandFields.check("update", document);
    final String collectionName = document.get("update").toString();
    final List<Document> updates = (List<Document>) document.get("updates");
    // every statement is checked before any runs, as mongod parses the command before it executes it: a field the plugin would
    // ignore (a collation, a hint, array filters) refuses the whole command with nothing updated (issue #11778)
    if (updates != null)
      for (final Object entry : updates)
        MongoCommandFields.check("update.updates", statementOf("update", "updates", entry));
    // MongoDB's default is an ordered update: it stops at the first refused statement
    final boolean ordered = document.get("ordered") == null || Utils.isTrue(document.get("ordered"));

    int n = 0;
    int nModified = 0;
    final List<Document> upserted = new ArrayList<>();
    final List<Document> writeErrors = new ArrayList<>();

    if (updates != null) {
      // one regex budget for the whole command, whatever the number of entries
      final MongoFilter.RegexBudget budget = MongoFilter.RegexBudget.of(database);
      // a sample of a TIMESERIES type is never updated, and has no identity for the record path to select: an update, a replacement
      // or an upsert is refused naming the type, before the upsert would prepare an _id index on it (issue #10464). The filters are
      // still validated first: an invalid one is an error whatever the collection
      if (!updates.isEmpty() && database.getSchema().getTypeOrNull(collectionName) instanceof TimeSeriesType timeSeries) {
        for (final Document upd : updates)
          new MongoFilter(database, (Document) upd.get("q"), budget);
        throw MongoTimeSeriesWrites.updateRefusal(timeSeries);
      }
      // a schema change cannot happen inside the transaction: an upsert creates the collection with its _id index up front
      prepareUpsertIdIndex(collectionName, updates);

      // the transaction maintains the unique _id index, which must not be dropped and rebuilt under it
      final Lock indexLock = MongoDBCollectionWrapper.idIndexLock(database, collectionName).readLock();
      indexLock.lock();
      try {
        // FAST PATH: every statement in the command's OWN transaction, so a failure rolls back this one, never one the caller holds
        // (issue #9977)
        RuntimeException refused = null;
        final OwnTransaction tx = OwnTransaction.begin((DatabaseInternal) database);
        try {
          for (int i = 0; i < updates.size(); i++) {
            final UpdateCount updated = executeUpdateStatement(collectionName, updates.get(i), i, budget, upserted);
            n += updated.matched();
            nModified += updated.modified();
          }
          tx.commit();
        } catch (final RuntimeException e) {
          // a statement refused on its own documents is reported in writeErrors with the others kept, as mongod does (issue #11444):
          // anything else is the command's failure
          if (statementErrorCode(e) == null)
            throw e;
          refused = e;
        } finally {
          tx.rollbackIfMine();
        }

        if (refused != null) {
          // The batch was rolled back. MongoDB is not atomic across the statements of a command, so run them again one by one, each
          // in its own transaction, to report each refusal by index and keep the others (ordered: only the ones before the first).
          // The statements that ran in the rolled-back transaction run again, so a record listener is called again for them: what it
          // wrote in the transaction went with it, only state it keeps outside the transaction sees the call twice (the insert replay
          // has always done the same)
          n = 0;
          nModified = 0;
          upserted.clear();
          for (int i = 0; i < updates.size(); i++) {
            final int upsertedBefore = upserted.size();
            final OwnTransaction statementTx = OwnTransaction.begin((DatabaseInternal) database);
            try {
              final UpdateCount updated = executeUpdateStatement(collectionName, updates.get(i), i, budget, upserted);
              statementTx.commit();
              n += updated.matched();
              nModified += updated.modified();
            } catch (final RuntimeException e) {
              final Document error = writeError(i, collectionName, e);
              if (error == null)
                throw e;
              // the statement's upsert, if any, went with its transaction
              while (upserted.size() > upsertedBefore)
                upserted.removeLast();
              writeErrors.add(error);
              if (ordered)
                break;
            } finally {
              statementTx.rollbackIfMine();
            }
          }
        }
      } finally {
        indexLock.unlock();
      }
    }

    final Document response = new Document();
    response.put("n", n + upserted.size());
    response.put("nModified", nModified);
    if (!upserted.isEmpty())
      response.put("upserted", upserted);
    if (!writeErrors.isEmpty())
      response.put("writeErrors", writeErrors);
    markOkay(response);
    return response;
  }

  /**
   * Runs one statement of an {@code update} command in the transaction the caller holds: the update of the documents its filter
   * selects or, when it matched none and asks for it, the upsert, added to {@code upserted}.
   */
  private UpdateCount executeUpdateStatement(final String collectionName, final Document statement, final int index,
      final MongoFilter.RegexBudget budget, final List<Document> upserted) {
    final Document q = (Document) statement.get("q");
    final Document u = updateOf(statement.get("u"));
    final boolean multi = Utils.isTrue(statement.get("multi"));
    final boolean upsert = Utils.isTrue(statement.get("upsert"));

    final UpdateCount updated = executeUpdate(collectionName, q, u, multi, budget);

    // only a filter that matched nothing upserts: a match whose update a listener vetoed is not "no match", or the upsert would insert
    // a second document for it (issue #9974)
    if (updated.matched() == 0 && upsert) {
      final Object id = executeUpsert(collectionName, q, u);
      // a create a BeforeRecordCreateListener vetoed upserted nothing (issue #9974)
      if (id != UPSERT_VETOED) {
        final Document upDoc = new Document("index", index);
        upDoc.put("_id", id);
        upserted.add(upDoc);
      }
    }
    return updated;
  }

  /**
   * The update document of a statement, checked before any record is read, as mongod parses it before running it (issue #11449): a
   * replacement, or operators this plugin applies. An operator mongod has and this plugin lacks is {@code NotImplemented}, naming it;
   * an operator mongod has not is {@code FailedToParse}, with mongod's own text.
   */
  static Document updateOf(final Object update) {
    if (update instanceof List)
      throw new MongoServerError(NOT_IMPLEMENTED, "NotImplemented", "Pipeline-style updates are not supported by ArcadeDB: " + SUPPORTED_UPDATE_OPERATORS_TEXT);
    if (!(update instanceof Document u))
      throw new MongoServerError(ErrorCode.FailedToParse.getValue(), ErrorCode.FailedToParse.getName(),
          "The update of a statement must be a document, found: " + (update == null ? "null" : update.getClass().getSimpleName()));
    if (isReplacement(u))
      return u;

    for (final Map.Entry<String, Object> entry : u.entrySet()) {
      final String op = entry.getKey();
      if (!SUPPORTED_UPDATE_OPERATORS.contains(op)) {
        if (MONGO_UPDATE_OPERATORS.contains(op))
          throw new MongoServerError(NOT_IMPLEMENTED, "NotImplemented",
              "Update operator '" + op + "' is not supported by ArcadeDB: " + SUPPORTED_UPDATE_OPERATORS_TEXT);
        throw new MongoServerError(ErrorCode.FailedToParse.getValue(), ErrorCode.FailedToParse.getName(),
            "Unknown modifier: " + op + ". Expected a valid update modifier or pipeline-style update specified as an array");
      }
    }
    // the operands and the paths they name are checked by checkUpdate (issue #12070)
    return u;
  }

  /**
   * The code and name mongod reports a statement refused on its own documents with, as an entry of {@code writeErrors}: a duplicated
   * key, a document a declared constraint refuses, a value an operator cannot take ({@link #STATEMENT_ERROR_CODES}). {@code null} when
   * the failure is the command's (a conflict to retry, a refused permission, a server fault), which fails the whole command.
   */
  private static MongoServerError statementErrorCode(final RuntimeException e) {
    return wireException("", e) instanceof MongoServerError coded && STATEMENT_ERROR_CODES.contains(coded.getCode()) ? coded : null;
  }

  /**
   * The {@code writeErrors} entry of a statement refused on its own documents (see {@link #statementErrorCode}), or {@code null} when
   * the failure is the command's.
   */
  private Document writeError(final int index, final String collectionName, final RuntimeException e) {
    final MongoServerError coded = statementErrorCode(e);
    if (coded == null)
      return null;

    final String message;
    if (ErrorCategory.of(e) == ErrorCategory.DUPLICATED_KEY)
      message = duplicateKeyMessage(collectionName, e);
    else if (e instanceof MongoServerError own)
      // text this plugin wrote about the request: it carries no stored data
      message = own.getMessageWithoutErrorCode();
    else {
      // the engine's text can carry the stored value it refused: concealed in production mode
      final Throwable refusal = CauseChain.find(e, ValidationException.class) instanceof ValidationException validation ? validation : e;
      final String text = ErrorConcealment.clientMessage(plugin != null && plugin.isProductionMode(), this, "MongoDB", refusal.getMessage(), e);
      message = coded.getCode() == DOCUMENT_VALIDATION_FAILURE ? "Document failed validation: " + text : text;
    }

    final Document error = new Document();
    error.put("index", index);
    error.put("code", coded.getCode());
    error.putIfNotNull("codeName", coded.getCodeName());
    error.put("errmsg", message);
    return error;
  }

  /**
   * A schema change cannot happen inside the transaction, so an upsert creates the collection and its {@code _id} index up front.
   */
  private void prepareUpsertIdIndex(final String collectionName, final List<Document> updates) {
    for (final Document upd : updates)
      if (Utils.isTrue(upd.get("upsert"))) {
        database.getSchema().getOrCreateDocumentType(collectionName);
        if (!MongoDBCollectionWrapper.idIndexReady(database, collectionName))
          MongoDBCollectionWrapper.ensureIdIndexLocked(database, collectionName);
        return;
      }
  }

  /**
   * What an update did, as MongoDB replies it: {@code n} the documents the filter matched, {@code nModified} the ones the engine
   * changed. They differ when the update leaves a document as it was (issue #11445) and when a {@code BeforeRecordUpdateListener}
   * vetoes a write (issue #9974).
   */
  private record UpdateCount(int matched, int modified) {
    private static final UpdateCount NONE = new UpdateCount(0, 0);
  }

  private UpdateCount executeUpdate(final String collectionName, final Document q, final Document u, final boolean multi,
      final MongoFilter.RegexBudget budget) {
    // built first: an invalid filter is an error whether or not the collection exists
    final MongoFilter filter = new MongoFilter(database, q, budget);
    // so is an invalid update, whether or not a document would be updated (issue #12070)
    if (u != null)
      checkUpdate(u);
    if (!database.getSchema().existsType(collectionName) || u == null)
      return UpdateCount.NONE;

    // Every update, with or without a filter, is applied to each record the filter selects (the matcher verifies the candidates, so
    // the answer is exact for any shape of data): only there can an update that leaves a document as it was be told from one that
    // changed it, and skip the write (issue #11445). An update without a filter used to be one SQL UPDATE, which answered every
    // matched record as modified and applied the operators with semantics of its own (SQL "+=" concatenated a string, issue #12070)
    return executeUpdateOnRecords(collectionName, filter, u, multi);
  }

  /**
   * Refuses an operator update {@code mongod} refuses when it parses it, before any document is touched (issue #12070): an operand
   * that is not a document, an {@code $inc} by an amount that is not a number, and two paths of which one is the other or lies below
   * it ({@code {$set: {a: 1}, $unset: {a: ''}}}, {@code {$set: {a: 1}, $inc: {'a.b': 1}}}), which would otherwise be applied one after
   * the other, the last one winning.
   */
  private static void checkUpdate(final Document u) {
    if (isReplacement(u))
      return;
    // every path named so far, and every proper prefix of them: a conflict is a path met in either set
    final Set<String> paths = new HashSet<>();
    final Set<String> prefixes = new HashSet<>();
    for (final Map.Entry<String, Object> entry : u.entrySet()) {
      final String op = entry.getKey();
      if (!(entry.getValue() instanceof Document operand))
        throw new MongoServerError(ErrorCode.FailedToParse.getValue(), ErrorCode.FailedToParse.getName(),
            "Modifiers operate on fields but we found type " + typeName(entry.getValue()) + " instead. For example: {$mod: {<field>: ...}} not {"
                + op + ": " + entry.getValue() + "}");
      for (final Map.Entry<String, Object> field : operand.entrySet()) {
        final String path = field.getKey();
        // a key property is the plugin's, not the client's (issue #12073)
        MongoKeys.checkNotKey(path);
        if ("$inc".equals(op) && !(field.getValue() instanceof Number))
          throw new MongoServerError(TYPE_MISMATCH, "TypeMismatch", "Cannot increment with non-numeric argument: {" + path + ": " + field.getValue() + "}");

        if (paths.contains(path) || prefixes.contains(path))
          throw conflict(path, path);
        for (int dot = path.indexOf('.'); dot > 0; dot = path.indexOf('.', dot + 1)) {
          final String prefix = path.substring(0, dot);
          if (paths.contains(prefix))
            throw conflict(path, prefix);
          prefixes.add(prefix);
        }
        paths.add(path);
      }
    }
  }

  private static MongoServerError conflict(final String path, final String at) {
    return new MongoServerError(CONFLICTING_UPDATE_OPERATORS, "ConflictingUpdateOperators", "Updating the path '" + path + "' would create a conflict at '" + at + "'");
  }

  /** The BSON type name of a value, for an error message. */
  private static String typeName(final Object value) {
    return switch (value) {
      case null -> "null";
      case String ignored -> "string";
      case Boolean ignored -> "bool";
      case Number ignored -> "number";
      case List<?> ignored -> "array";
      case Map<?, ?> ignored -> "object";
      case com.arcadedb.database.Document ignored -> "object";
      case Date ignored -> "date";
      case Temporal ignored -> "date";
      default -> value.getClass().getSimpleName();
    };
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
        throw new MongoServerError(IMMUTABLE_FIELD, "ImmutableField", "Performing an update on the path '_id' would modify the immutable field '_id'");
    }
  }

  private UpdateCount executeUpdateOnRecords(final String collectionName, final MongoFilter filter, final Document u, final boolean multi) {
    // collect the RIDs first (the records are modified while the result set would still be open on them), and load each record
    // in turn: only identities are held on the heap, not every matching document. An update of one record selects again, a few times,
    // when its candidate stopped matching since the selection while another record still matches. A candidate that still matched
    // but whose update a listener vetoed is not retried: selecting again would find it again and ask the listener again
    final List<String[]> unique = MongoKeys.uniquePaths(database.getSchema().getType(collectionName));
    UpdateCount count = UpdateCount.NONE;
    for (int attempt = 0; attempt < (multi ? 1 : SINGLE_RECORD_ATTEMPTS); attempt++) {
      final List<RID> rids = filter.select(database, collectionName, multi ? 0 : 1);
      count = updateRecords(rids, filter, u, unique);
      if (count.matched() > 0 || rids.isEmpty())
        break;
    }
    return count;
  }

  private UpdateCount updateRecords(final List<RID> rids, final MongoFilter filter, final Document u, final List<String[]> unique) {
    final boolean replacement = isReplacement(u);
    // the top-level properties the operators touch: only their values can change
    final String[] touched = replacement ? null : touchedProperties(u);
    // an operator never changes the _id, so only the keys of the createIndexes unique indexes can change, and only when the update
    // writes a field they are made of (issue #12073): any other update skips converting the document
    final boolean keysAffected = !replacement && !unique.isEmpty() && MongoKeys.affects(unique, touched);
    int vanished = 0;
    int modified = 0;
    for (final RID rid : rids) {
      final MutableDocument record;
      try {
        record = rid.asDocument().modify();
      } catch (final RecordNotFoundException e) {
        // deleted since the select: it no longer matches, like a single statement would not have touched it
        ++vanished;
        continue;
      }
      // the filter is tested again on the record as it is now: one that stopped matching since the selection is left alone
      if (!filter.isEmpty() && !filter.matches(record)) {
        ++vanished;
        continue;
      }
      final boolean changed;
      if (replacement) {
        final Map<String, Object> before = record.toMap(false);
        replaceContent(record, u);
        // the replacement removed the derived keys with every other property: they follow the new fields (issue #12073)
        MongoKeys.setKeys(record, MongoDBToSqlTranslator.convertDocumentToMongoDB(record), unique);
        changed = !sameValue(before, record.propertiesAsMap());
      } else {
        checkIdUntouched(record, u);
        final Object[] before = new Object[touched.length];
        for (int i = 0; i < touched.length; i++)
          before[i] = record.getIfPresent(touched[i], ABSENT);
        applyOperatorsToDocument(record, u);
        boolean anyChanged = false;
        for (int i = 0; i < touched.length && !anyChanged; i++)
          anyChanged = !sameValue(before[i], record.getIfPresent(touched[i], ABSENT));
        changed = anyChanged;
      }
      // matched, but modified only when the update changed the document, as on mongod (issue #11445): a document left as it was is not
      // written at all. And only if the engine applied it: an update a BeforeRecordUpdateListener vetoed is not (issue #9974)
      if (changed) {
        // the derived keys follow the fields they are made of
        if (keysAffected)
          MongoKeys.setKeys(record, MongoDBToSqlTranslator.convertDocumentToMongoDB(record), unique);
        if (record.saveReportingVeto())
          ++modified;
      }
    }
    return new UpdateCount(rids.size() - vanished, modified);
  }

  /** The top-level properties the operators of {@code u} write: the head of each path. */
  private static String[] touchedProperties(final Document u) {
    final Set<String> heads = new LinkedHashSet<>();
    for (final Object operand : u.values())
      if (operand instanceof Document fields)
        for (final String path : fields.keySet()) {
          final int dot = path.indexOf('.');
          heads.add(dot < 0 ? path : path.substring(0, dot));
        }
    return heads.toArray(new String[0]);
  }

  /**
   * Whether two property values are the same, as mongod compares a document before and after an update: of the same type and value, a
   * map or a list element by element. Anything this cannot compare (an embedded record against a map) reads as different, so a change
   * is never missed: at worst a document left as it was is written and counted.
   */
  static boolean sameValue(final Object a, final Object b) {
    // identity on purpose, not equals(): a dotted $set copies only the maps on its path and shares every other value by reference, so a
    // no-op must not walk a large sibling list element by element to find it unchanged
    if (a == b) // NOPMD: identity is the point, a value shared by reference is unchanged without walking it
      return true;
    // maps and lists first: Map.equals() ignores the order of the fields, which mongod does not, and a list compares its elements with it
    if (a instanceof Map<?, ?> mapA && b instanceof Map<?, ?> mapB) {
      if (mapA.size() != mapB.size())
        return false;
      // in iteration order: an embedded document whose fields were reordered is a different document on mongod, written and counted
      final Iterator<? extends Map.Entry<?, ?>> entriesB = mapB.entrySet().iterator();
      for (final Map.Entry<?, ?> entryA : mapA.entrySet()) {
        final Map.Entry<?, ?> entryB = entriesB.next();
        if (!Objects.equals(entryA.getKey(), entryB.getKey()) || !sameValue(entryA.getValue(), entryB.getValue()))
          return false;
      }
      return true;
    }
    if (a instanceof List<?> listA && b instanceof List<?> listB) {
      if (listA.size() != listB.size())
        return false;
      for (int i = 0; i < listA.size(); i++)
        if (!sameValue(listA.get(i), listB.get(i)))
          return false;
      return true;
    }
    if (a instanceof byte[] bytesA && b instanceof byte[] bytesB)
      return Arrays.equals(bytesA, bytesB);
    // the values of one class compare by value (an Integer is never equal to a Long, the same 1 stored with another BSON type), the
    // ABSENT marker and null by identity
    return Objects.equals(a, b);
  }

  /**
   * Replaces every property of the record with the replacement's, but the {@code _id}: MongoDB's {@code _id} is immutable, so a
   * replacement without one keeps the stored value and a replacement with a different one is refused (error 66).
   */
  private static void replaceContent(final MutableDocument record, final Document replacement) {
    final Object storedId = record.get("_id");
    final Object newId = replacement.containsKey("_id") ? idValue(replacement.get("_id")) : storedId;
    if (storedId != null && !sameId(storedId, newId))
      throw new MongoServerError(IMMUTABLE_FIELD, "ImmutableField", "After applying the update, the (immutable) field '_id' was found to have been altered");

    for (final String name : new ArrayList<>(record.getPropertyNames()))
      if (!"_id".equals(name))
        record.remove(name);

    if (newId != null)
      record.set("_id", newId);
    for (final Map.Entry<String, Object> entry : replacement.entrySet())
      if (!"_id".equals(entry.getKey())) {
        MongoKeys.checkNotKey(entry.getKey());
        record.set(entry.getKey(), toMapValue(entry.getValue()));
      }
  }

  /**
   * @return the {@code _id} of the document inserted, which may legitimately be {@code null}, or {@link #UPSERT_VETOED} when a
   * {@code BeforeRecordCreateListener} refused it
   */
  private Object executeUpsert(final String collectionName, final Document q, final Document u) {
    // first: a filter mongod cannot infer the document from is refused (a statement error) before anything is created
    final Document seed = upsertSeed(q);
    final MutableDocument record = database.newDocument(database.getSchema().getOrCreateDocumentType(collectionName).getName());

    // Seed the new document with what the filter's equalities infer, as mongod does (issue #12071): a dotted path makes the embedded
    // documents on its way. A replacement takes the _id alone from the filter, its own fields are the whole document.
    final boolean replacement = isReplacement(u);
    for (final Map.Entry<String, Object> entry : seed.entrySet()) {
      final String path = entry.getKey();
      final boolean id = "_id".equals(path);
      if (replacement && !id)
        continue;
      setPath(record, path, id ? idValue(entry.getValue()) : toMapValue(entry.getValue()));
    }

    if (replacement) {
      // last write wins, matching record.set() itself: a replacement re-specifying "_id" overrides what the filter seeded
      for (final Map.Entry<String, Object> entry : u.entrySet()) {
        MongoKeys.checkNotKey(entry.getKey());
        record.set(entry.getKey(), MongoBsonValues.toStored(entry.getKey(), entry.getValue()));
      }
    } else
      applyOperatorsToDocument(record, u);

    // has(), not get() == null: an explicit "_id": null in the filter/update is a legal (if unusual) BSON _id and
    // must be preserved, whereas a genuinely absent _id needs one generated.
    if (!record.has("_id"))
      record.set("_id", MongoBsonValues.toStored("_id", new ObjectId()));
    moveIdFirst(record);
    MongoKeys.setKeys(record, MongoDBToSqlTranslator.convertDocumentToMongoDB(record), MongoKeys.uniquePaths(record.getType()));

    if (!record.saveReportingVeto())
      return UPSERT_VETOED;

    // the stored _id tells an ObjectId from a String with the same hex (issue #12073): read back as a find would answer it
    return MongoDBToSqlTranslator.convertIdToMongoDB(record.get("_id"));
  }

  /**
   * mongod stores the {@code _id} as the first field of an inserted document, whichever of the filter, the update or the generator
   * supplied it (issue #12071).
   */
  private static void moveIdFirst(final MutableDocument record) {
    final List<String> names = new ArrayList<>(record.getPropertyNames());
    if (names.isEmpty() || "_id".equals(names.getFirst()))
      return;
    final Object[] values = new Object[names.size()];
    for (int i = 0; i < values.length; i++)
      values[i] = record.get(names.get(i));
    for (final String name : names)
      record.remove(name);
    record.set("_id", values[names.indexOf("_id")]);
    for (int i = 0; i < values.length; i++)
      if (!"_id".equals(names.get(i)))
        record.set(names.get(i), values[i]);
  }

  /**
   * The fields an upsert that matches nothing copies from its filter, by path, as mongod infers them (issue #12071): the equality of
   * each path, dotted or not, at the top level, under {@code $and} and under an {@code $or} of a single branch. An equality is a plain
   * value (an embedded document and a list included, a regular expression not), {@code $eq}, or an {@code $in} of a single value, or
   * a value of an {@code $all} (see {@link #equalityOf}). A path matched twice, or a path and one below it, is refused with mongod's
   * error 54, as is an {@code $all} of several values.
   * <p>
   * mongod infers these fields only for a statement that inserts, so the refusal is the insert's: it is raised here and reaches the
   * client from {@link #executeUpsert} as an entry of {@code writeErrors}, while a filter that matches a document updates it.
   */
  static Document upsertSeed(final Document query) {
    final Document seed = new Document();
    if (query != null)
      collectSeed(query, seed);
    return seed;
  }

  private static void collectSeed(final Document query, final Document seed) {
    for (final Map.Entry<String, Object> entry : query.entrySet()) {
      final String key = entry.getKey();
      final Object value = entry.getValue();
      if ("$and".equals(key) || "$or".equals(key)) {
        // an $or of several branches says nothing about which one the inserted document satisfies
        if (value instanceof List<?> items && ("$and".equals(key) || items.size() == 1))
          for (final Object item : items)
            if (item instanceof Document branch)
              collectSeed(branch, seed);
      } else if (!key.startsWith("$")) {
        final Object equality = equalityOf(key, value);
        if (equality != NO_EQUALITY)
          addSeed(seed, key, equality);
      }
    }
  }

  /**
   * The value the conditions of one path infer, {@link #NO_EQUALITY} when none of them is an equality. {@code $eq}, an {@code $in} of
   * one value and each value of an {@code $all} are equalities of their own, and mongod refuses two of them on a path. A regular
   * expression is a pattern to match, never a value (this plugin's matcher does not match a stored one back, so the next run of the
   * upsert would insert again), and an {@code $elemMatch} document of an {@code $all} says nothing about a value either.
   */
  private static Object equalityOf(final String path, final Object value) {
    if (value instanceof BsonRegularExpression)
      return NO_EQUALITY;
    if (!(value instanceof Document operators) || !MongoFilter.isOperatorDocument(operators))
      return value;

    Object equality = NO_EQUALITY;
    for (final Map.Entry<String, Object> operator : operators.entrySet()) {
      final Object operand = operator.getValue();
      switch (operator.getKey()) {
      case "$eq" -> equality = addEquality(path, equality, operand);
      case "$in" -> {
        if (operand instanceof List<?> in && in.size() == 1)
          equality = addEquality(path, equality, in.getFirst());
      }
      case "$all" -> {
        if (operand instanceof List<?> all)
          for (final Object item : all)
            equality = addEquality(path, equality, item);
      }
      default -> {
        // any other operator is no equality
      }
      }
    }
    return equality;
  }

  /** {@code found} with the equality {@code candidate} added: it is refused when the path has one already, ignored when it is a pattern. */
  private static Object addEquality(final String path, final Object found, final Object candidate) {
    if (candidate instanceof BsonRegularExpression || candidate instanceof Document operators && MongoFilter.isOperatorDocument(operators))
      return found;
    if (found != NO_EQUALITY)
      throw notSingleValue("path '" + path + "' is matched twice");
    return candidate;
  }

  private static void addSeed(final Document seed, final String path, final Object value) {
    for (final String other : seed.keySet()) {
      if (other.equals(path))
        throw notSingleValue("path '" + path + "' is matched twice");
      final String shorter = path.length() < other.length() ? path : other;
      final String longer = shorter == path ? other : path;
      if (longer.startsWith(shorter) && longer.charAt(shorter.length()) == '.')
        throw notSingleValue("both paths '" + longer + "' and '" + shorter + "' are matched");
    }
    seed.put(path, value);
  }

  private static MongoServerError notSingleValue(final String what) {
    return new MongoServerError(ErrorCode.NotSingleValueField.getValue(), ErrorCode.NotSingleValueField.getName(),
        "cannot infer query fields to set, " + what);
  }

  /**
   * Applies the update operators to a record, in the order they are given: {@link #checkUpdate} has refused two of them on the same
   * path, so the order cannot change the result.
   */
  private void applyOperatorsToDocument(final MutableDocument record, final Document u) {
    for (final Map.Entry<String, Object> entry : u.entrySet()) {
      final String op = entry.getKey();
      final Document operand = (Document) entry.getValue();
      switch (op) {
      case "$set" -> {
        for (final Map.Entry<String, Object> f : operand.entrySet())
          setPath(record, f.getKey(), "_id".equals(f.getKey()) ? idValue(f.getValue()) : toMapValue(f.getValue()));
      }
      case "$unset" -> {
        for (final String f : operand.keySet())
          unsetPath(record, f);
      }
      case "$inc" -> {
        for (final Map.Entry<String, Object> f : operand.entrySet()) {
          final Object current = getPath(record, f.getKey());
          final Number delta = (Number) f.getValue();
          // toStored: a Decimal128 delta added to a missing field is a Number that must still become a DECIMAL
          setPath(record, f.getKey(), MongoBsonValues.toStored(current == MISSING ? delta : inc(current, delta, record, f.getKey())));
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
    final Map<Object, Object> copy = copyOfEmbedded(container);
    final Object key = keyNamed(copy, head);
    copy.put(key, dot < 0 ? value : setNested(copy.get(key), path.substring(dot + 1), value));
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

    final Map<Object, Object> copy = copyOfEmbedded(container);
    final Object key = keyNamed(copy, head);
    if (dot < 0)
      copy.remove(key);
    else if (copy.get(key) != null)
      copy.put(key, unsetNested(copy.get(key), path.substring(dot + 1)));
    return copy;
  }

  /**
   * The value stored at a possibly dotted path, {@link #MISSING} where there is none: a stored null is a value, which {@code $inc}
   * refuses, while a missing field is set to the amount.
   */
  private static Object getPath(final MutableDocument record, final String path) {
    final int dot = path.indexOf('.');
    if (dot < 0)
      return record.has(path) ? record.get(path) : MISSING;

    final String head = path.substring(0, dot);
    if (!record.has(head))
      return MISSING;
    Object current = record.get(head);
    for (final String segment : path.substring(dot + 1).split("\\.")) {
      if (current instanceof Map<?, ?> map) {
        final Object key = keyNamed(map, segment);
        if (!map.containsKey(key))
          return MISSING;
        current = map.get(key);
      } else if (current instanceof List<?> list && isArrayIndex(segment) && Integer.parseInt(segment) < list.size())
        current = list.get(Integer.parseInt(segment));
      else if (current instanceof com.arcadedb.database.Document embedded) {
        if (!embedded.has(segment))
          return MISSING;
        current = embedded.get(segment);
      } else
        return MISSING;
    }
    return current;
  }

  private static Map<Object, Object> copyOfEmbedded(final Object container) {
    if (container instanceof Map<?, ?> map)
      return new LinkedHashMap<>(map);
    if (container instanceof com.arcadedb.database.Document embedded) {
      final Map<Object, Object> copy = new LinkedHashMap<>(embedded.toMap());
      copy.keySet().removeIf(key -> MongoDBToSqlTranslator.isRecordMetadata(String.valueOf(key)));
      return copy;
    }
    return new LinkedHashMap<>();
  }

  /**
   * The key of {@code map} a path segment names. A {@code MAP} property keeps its keys with their own class (an Integer, a Long, a
   * {@code null}), while a MongoDB path names a field by its text, the text {@code find} shows it under (issue #9973): the stored
   * key whose text is {@code name} is the one updated, keeping its class, and a name no key has is a new String key. A String key
   * equal to the name wins over another key with the same text, the same member {@code find} shows for it. The scan only runs
   * when the name is not itself a key, which is never the case for a map whose keys are all Strings and that holds it.
   */
  private static Object keyNamed(final Map<?, ?> map, final String name) {
    if (map.containsKey(name))
      return name;
    for (final Object key : map.keySet())
      if (name.equals(String.valueOf(key)))
        return key;
    return name;
  }

  /**
   * MongoDB refuses to create a field below a scalar or an array (with a non-numeric key) instead of replacing it: doing so
   * here would silently destroy the stored value. The refusal names the kind of the stored value, never the value: the text reaches
   * the client in production mode too, and the value is stored data.
   */
  private static void requireEmbedded(final Object container, final String field) {
    if (container != null && !isEmbedded(container))
      throw new MongoServerError(PATH_NOT_VIABLE, "PathNotViable",
          "Cannot create field '" + field + "' in an element of type " + (container instanceof List ? "array" : container.getClass().getSimpleName()));
  }

  /**
   * MongoDB compares numbers by value, so an {@code _id} of 2 and one of 2L or 2.0 are the same.
   */
  private static boolean sameId(final Object stored, final Object replacement) {
    if (stored instanceof Number a && replacement instanceof Number b)
      return a.doubleValue() == b.doubleValue();
    return Objects.equals(stored, replacement);
  }

  /**
   * {@code $inc} of a stored value, which must be a number: a null, a string, a boolean, an array, a date or a document is refused,
   * as {@code mongod} refuses it (issue #12070). A Decimal128 stored tagged (a negative zero, NaN, an infinity) is a number.
   */
  private static Number inc(final Object stored, final Number delta, final MutableDocument record, final String path) {
    final Object value = MongoBsonValues.isTagged(stored) ? MongoBsonValues.toBson(stored) : stored;
    if (!(value instanceof Number current))
      throw new MongoServerError(TYPE_MISMATCH, "TypeMismatch",
          "Cannot apply $inc to a value of non-numeric type. {_id: " + record.get("_id") + "} has the field '" + path + "' of non-numeric type "
              + typeName(value));
    return add(current, delta, record);
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
   * {@code $inc}: integral operands stay integral (an int that overflows becomes a long, a long that overflows is refused as
   * {@code mongod} refuses it, issue #12070), a decimal operand makes a decimal, anything else is a double.
   */
  private static Number add(final Number current, final Number delta, final MutableDocument record) {
    if (isIntegral(current) && isIntegral(delta)) {
      final long sum;
      try {
        sum = Math.addExact(current.longValue(), delta.longValue());
      } catch (final ArithmeticException e) {
        throw new MongoServerError(ErrorCode.BadValue.getValue(), ErrorCode.BadValue.getName(),
            "Failed to apply $inc operations to current value ((NumberLong)" + current + ") for document {_id: " + record.get("_id") + "}");
      }
      return current instanceof Long || delta instanceof Long || sum > Integer.MAX_VALUE || sum < Integer.MIN_VALUE ? (Number) sum : (Number) (int) sum;
    }
    if (current instanceof BigDecimal || delta instanceof BigDecimal || current instanceof Decimal128 || delta instanceof Decimal128)
      return MongoBsonValues.addDecimal(current, delta);
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

  private int deleteMatching(final String collectionName, final MongoFilter filter, final boolean single) {
    int deleted = 0;
    for (int attempt = 0; attempt < (single ? SINGLE_RECORD_ATTEMPTS : 1); attempt++) {
      final List<RID> rids = filter.select(database, collectionName, single ? 1 : 0);
      deleted = deleteRecords(rids, filter);
      if (deleted > 0 || rids.isEmpty())
        break;
    }
    return deleted;
  }

  private int deleteRecords(final List<RID> rids, final MongoFilter filter) {
    int deleted = 0;
    for (final RID rid : rids)
      try {
        final com.arcadedb.database.Document record = rid.asDocument();
        // the filter is tested again on the record as it is now: one that stopped matching since the selection is left alone
        // counted by what the engine answered: a delete a BeforeRecordDeleteListener vetoed is not one (issue #9974)
        if (filter.matches(record) && ((DatabaseInternal) database).deleteRecordReportingVeto(record))
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
   * Converts an embedded BSON document into the map a record stores. Insertion order is preserved so the document reaches the record
   * in wire order.
   */
  private static Map<String, Object> documentToMap(final Document doc) {
    final Map<String, Object> map = LinkedHashMap.newLinkedHashMap(doc.size());
    for (final Map.Entry<String, Object> entry : doc.entrySet())
      map.put(entry.getKey(), toMapValue(entry.getValue()));
    return map;
  }

  private static Object toMapValue(final Object value) {
    if (value instanceof Document document) {
      MongoBsonValues.checkNotReserved(document);
      return documentToMap(document);
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

  /**
   * A numeric option of a command, {@code defaultValue} when it is absent or null; any other kind is refused as mongod refuses it.
   */
  private static long getOptionalLong(final Document query, final String fieldName, final long defaultValue) {
    final Object value = query.get(fieldName);
    if (value == null)
      return defaultValue;
    if (value instanceof Number number)
      return number.longValue();
    throw new MongoServerError(TYPE_MISMATCH, "TypeMismatch",
        "BSON field '" + fieldName + "' is the wrong type '" + typeName(value) + "', expected type 'number'");
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
