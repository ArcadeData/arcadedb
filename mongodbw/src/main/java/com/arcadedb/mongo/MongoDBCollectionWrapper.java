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
import com.arcadedb.database.Record;
import com.arcadedb.engine.OwnTransaction;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.TimeSeriesType;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.TypeIndexBuilder;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityHelper;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.stream.Stream;

public class MongoDBCollectionWrapper implements MongoCollection<Long> {
  /** The records a key backfill updates per transaction. */
  static final int BACKFILL_BATCH = 10_000;

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
    // an OP_QUERY answers every document in the one reply: the cursor is drained, and closed with the last row
    final MongoFindCursor cursor = openCursor(queryParameters, null);
    try {
      final List<Document> documents = cursor.takeBatch(Integer.MAX_VALUE, Long.MAX_VALUE);
      return new QueryResult(documents);
    } finally {
      cursor.close();
    }
  }

  /**
   * The documents a query selects, as a cursor that reads and converts them one batch at a time. The caller owns it: it must
   * {@link MongoFindCursor#close() close} it, unless it is drained, which releases it too.
   *
   * @param projection applied to every document the cursor answers, or {@code null}
   */
  MongoFindCursor openCursor(final QueryParameters queryParameters, final MongoProjection projection) {
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

    // built first: building it refuses a malformed filter, as mongod does whether or not the collection holds a document (#12074)
    final MongoFilter filter = new MongoFilter(database, query);
    // so is an invalid sort
    final MongoSortOrder sortOrder = orderBy != null && !orderBy.isEmpty() ? new MongoSortOrder(orderBy) : null;
    if (this.count() == 0)
      return MongoFindCursor.empty();

    return this.queryDocuments(filter, sortOrder, numberToSkip, numberToReturn, projection);
  }

  @Override
  public void insertDocuments(final List<Document> list) {
    insertDocumentsCountingStored(list);
  }

  /**
   * {@link #insertDocuments(List)}, answering how many documents the engine stored: the {@code n} an {@code insert} replies. A
   * document a {@code BeforeRecordCreateListener} vetoed is not stored and is not counted (issue #9974).
   */
  int insertDocumentsCountingStored(final List<Document> list) {
    // before the lock of the unique indexes is taken
    checkAccess(database, collectionName, SecurityDatabaseUser.ACCESS.CREATE_RECORD);

    // Inserts share the read lock; only the creation of the _id index (the first insert) takes the write lock, so no transaction
    // ever runs against an index that is being built
    final ReadWriteLock lock = idIndexLock(database, collectionName);
    lock.readLock().lock();
    try {
      if (!idIndexReady(database, collectionName)) {
        lock.readLock().unlock();
        lock.writeLock().lock();
        try {
          ensureIdIndex(database, collectionName);
        } finally {
          // downgrade: take the read lock before releasing the write one
          lock.readLock().lock();
          lock.writeLock().unlock();
        }
      }
      return insertInTransaction(list);
    } finally {
      lock.readLock().unlock();
    }
  }

  private int insertInTransaction(final List<Document> list) {
    final DocumentType type = database.getSchema().getType(collectionName);
    // a type whose unique index could not be built (it already holds duplicates) is checked by hand
    final boolean checkByHand = !MongoKeys.hasIdKeyIndex(type);
    if (checkByHand && ID_INDEX_SLOW_PATH_LOGGED.add(idIndexKey(database, collectionName)))
      LogManager.instance().log(this, Level.WARNING,
          "Collection '%s' has no unique index on _id: every insert checks the _id with a query, which is slow on a big collection", null,
          collectionName);
    final List<String[]> unique = MongoKeys.uniquePaths(type);

    // The insert's OWN transaction, nested in whatever the caller holds: an ArcadeDB nested transaction is independent, not a
    // savepoint, and the commit pops it. Asking database.isTransactionActive() afterwards answers for the CALLER's, which a
    // rollback guarded by it used to discard on the successful path (issue #9977).
    final OwnTransaction tx = OwnTransaction.begin((DatabaseInternal) database);
    try {
      int stored = 0;
      for (final Document d : list) {
        // like mongod, a document without an _id is given one, as its first field: its key is what makes it unique
        final Document document = d.containsKey("_id") ? d : withGeneratedId(d);
        if (checkByHand)
          checkIdIsFree(document.get("_id"));

        final MutableDocument record = database.newDocument(collectionName);
        for (final Map.Entry<String, Object> p : document.entrySet()) {
          MongoKeys.checkNotKey(p.getKey());
          record.set(p.getKey(), MongoBsonValues.toStored(p.getKey(), p.getValue()));
        }
        MongoKeys.setKeys(record, document, unique);

        if (record.saveReportingVeto())
          ++stored;
      }

      tx.commit();
      return stored;
    } finally {
      // a failed insert (a duplicated _id, a constraint) must not leave its transaction open on this thread
      tx.rollbackIfMine();
    }
  }

  private static Document withGeneratedId(final Document document) {
    final Document withId = new Document("_id", new ObjectId());
    withId.putAll(document);
    return withId;
  }

  /**
   * Best effort, not a uniqueness guarantee: a check followed by an insert, with no lock against other writers, so two
   * concurrent inserts of the same {@code _id} can both pass. It is only the fallback for a collection whose unique index could
   * not be built because it already holds duplicates.
   */
  private void checkIdIsFree(final Object id) {
    try (final ResultSet rs = database.query("SQL",
        "select @rid from " + Identifier.quote(collectionName) + " where " + Identifier.quote(MongoKeys.ID_KEY) + " = :key limit 1",
        Map.of("key", MongoKeys.key(id)))) {
      if (rs.hasNext())
        throw new DuplicatedKeyException(collectionName + "[_id]", String.valueOf(id), rs.next().getIdentity().orElse(null));
    }
  }

  private static final Map<String, ReadWriteLock> ID_INDEX_LOCKS = new ConcurrentHashMap<>();

  /**
   * The collections whose {@code _id} index could not be built (they hold duplicates): they stay on the by-hand check, instead
   * of retrying a failing full build, under the exclusive lock, on every insert.
   */
  private static final Set<String> ID_INDEX_GAVE_UP = ConcurrentHashMap.newKeySet();

  private static final Set<String> ID_INDEX_SLOW_PATH_LOGGED = ConcurrentHashMap.newKeySet();

  private static String idIndexKey(final Database database, final String collectionName) {
    return database.getDatabasePath() + "/" + collectionName;
  }

  /**
   * {@link #forgetIdIndex} for every collection of a database that is dropped.
   */
  static void forgetIdIndexes(final Database database) {
    final String prefix = database.getDatabasePath() + "/";
    ID_INDEX_LOCKS.keySet().removeIf(key -> key.startsWith(prefix));
    ID_INDEX_GAVE_UP.removeIf(key -> key.startsWith(prefix));
    ID_INDEX_SLOW_PATH_LOGGED.removeIf(key -> key.startsWith(prefix));
  }

  /**
   * Forgets what is kept per collection (the lock, the failed build): called when the collection is dropped, so the maps do not
   * grow with every collection ever touched and a recreated collection starts afresh.
   */
  static void forgetIdIndex(final Database database, final String collectionName) {
    final String key = idIndexKey(database, collectionName);
    ID_INDEX_LOCKS.remove(key);
    ID_INDEX_GAVE_UP.remove(key);
    ID_INDEX_SLOW_PATH_LOGGED.remove(key);
  }

  /**
   * The lock guarding the unique indexes of one collection of one database: shared by the writes that rely on them, exclusive for
   * whoever creates one (and fills in the keys it is built on).
   */
  static ReadWriteLock idIndexLock(final Database database, final String collectionName) {
    return ID_INDEX_LOCKS.computeIfAbsent(idIndexKey(database, collectionName), k -> new ReentrantReadWriteLock());
  }

  /**
   * Refuses a user who may not {@code access} the records of a collection. A write command asks it first: before it takes the lock
   * of the unique indexes and before it reads a record, where the engine asks it when a record is saved. A collection that does not
   * exist is not refused here (creating it is a schema change, checked where it is made), nor is a call with no user bound.
   */
  static void checkAccess(final Database database, final String collectionName, final SecurityDatabaseUser.ACCESS access) {
    SecurityHelper.checkAccessOnType((DatabaseInternal) database, database.getSchema().getTypeOrNull(collectionName), access);
  }

  /**
   * Creates the {@code _id} index under the exclusive lock, for a caller outside {@link #insertDocuments} (an upsert, which runs
   * its own transaction afterwards).
   */
  static void ensureIdIndexLocked(final Database database, final String collectionName) {
    final ReadWriteLock lock = idIndexLock(database, collectionName);
    lock.writeLock().lock();
    try {
      ensureIdIndex(database, collectionName);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Whether the {@code _id} index exists, or could not be built: the cheap check of the insert path.
   */
  static boolean idIndexReady(final Database database, final String collectionName) {
    return ID_INDEX_GAVE_UP.contains(idIndexKey(database, collectionName)) || MongoKeys.hasIdKeyIndex(
        database.getSchema().getType(collectionName));
  }

  /**
   * MongoDB guarantees the {@code _id} of a collection is unique through an index every collection has. ArcadeDB has no implicit
   * one, so the plugin creates it when the first document arrives: a unique index on the derived key of the {@code _id}
   * ({@link MongoKeys}), which tells the kinds of {@code _id} apart where an index on the {@code _id} itself converted every key to
   * one type and made {@code 1} and {@code "1"} the same key (issue #12073). A collection that already holds documents (written
   * before #12073, or through another protocol) gets the keys of its documents first, and loses the unique index on the plain
   * {@code _id} the plugin built before. A collection that holds duplicated {@code _id} values cannot get the index: it is left
   * without and {@link #insertDocuments} checks by hand instead. The caller must hold the exclusive lock.
   */
  static void ensureIdIndex(final Database database, final String collectionName) {
    if (idIndexReady(database, collectionName))
      return;

    if (database.countType(collectionName, false) > 0) {
      LogManager.instance().log(MongoDBCollectionWrapper.class, Level.INFO,
          "Building the unique index on _id of the existing collection '%s': writes to it wait until it is done", null, collectionName);
      backfillKeys(database, collectionName, MongoKeys.ID_KEY, null);
    }

    try {
      createKeyIndex(database, collectionName, MongoKeys.ID_KEY);
    } catch (final RuntimeException e) {
      // only duplicates are permanent: any other failure (a timeout, a lock) may well succeed on the next insert
      if (ErrorCategory.of(e) == ErrorCategory.DUPLICATED_KEY)
        ID_INDEX_GAVE_UP.add(idIndexKey(database, collectionName));
      LogManager.instance().log(MongoDBCollectionWrapper.class, Level.WARNING,
          "Cannot create the unique index on _id of collection '%s': duplicates of _id will be checked on insert (%s)", null,
          collectionName, e.getMessage());
      return;
    }

    // the unique index on the plain _id compares its keys as one type: it would refuse "1" next to 1
    final TypeIndex legacy = findUniqueIdIndex(database.getSchema().getType(collectionName));
    if (legacy != null)
      database.getSchema().dropIndex(legacy.getName());
  }

  /**
   * The unique index of a {@code createIndexes} on the fields {@code paths}, on their derived key {@code property}: the keys of the
   * records that exist are written, then the index is built, then the index the plugin built on the plain fields before #12073 is
   * dropped, as it would go on refusing {@code 1} next to {@code "1"}. An index that already exists costs nothing, as on mongod:
   * the keys of the records written since are the plugin's to keep. The caller must hold the exclusive lock.
   */
  static void ensureUniqueKeyIndex(final Database database, final String collectionName, final String property, final String[] paths) {
    if (keyIndexNames(database, collectionName, property).isEmpty()) {
      if (database.countType(collectionName, false) > 0)
        backfillKeys(database, collectionName, property, paths);
      createKeyIndex(database, collectionName, property);
    }
    dropLegacyUniqueIndexes(database, database.getSchema().getType(collectionName), paths);
  }

  /**
   * Drops the unique indexes {@code createIndexes} built on the plain fields before #12073: on exactly these fields, none of them
   * declared, so that the keys were read as strings (a unique index on a declared field is the application's own, and compares what
   * its type says).
   */
  private static void dropLegacyUniqueIndexes(final Database database, final DocumentType type, final String[] paths) {
    final List<String> legacy = new ArrayList<>();
    for (final TypeIndex index : type.getAllIndexes(false))
      if (index.isUnique() && index.getPropertyNames().equals(Arrays.asList(paths)) && !hasDeclaredProperty(type, paths))
        legacy.add(index.getName());
    for (final String name : legacy)
      database.getSchema().dropIndex(name);
  }

  private static boolean hasDeclaredProperty(final DocumentType type, final String[] paths) {
    for (final String path : paths)
      if (type.getPolymorphicPropertyIfExists(path) != null)
        return true;
    return false;
  }

  /**
   * Builds the unique index on a derived key property; a failed build (a duplicate) leaves no partial index behind. Only an index
   * this call created is dropped on failure: one that already existed on the property is left as it was.
   */
  static void createKeyIndex(final Database database, final String collectionName, final String property) {
    final List<String> before = keyIndexNames(database, collectionName, property);
    try {
      final TypeIndexBuilder builder = database.getSchema().buildTypeIndex(collectionName, new String[] { property });
      builder.withType(Schema.INDEX_TYPE.LSM_TREE);
      builder.withUnique(true);
      builder.withIgnoreIfExists(true);
      builder.withDefaultKeyTypesForUndeclaredProperties(new Type[] { Type.STRING });
      builder.create();
    } catch (final RuntimeException e) {
      for (final String name : keyIndexNames(database, collectionName, property))
        if (!before.contains(name))
          database.getSchema().dropIndex(name);
      throw e;
    }
  }

  private static List<String> keyIndexNames(final Database database, final String collectionName, final String property) {
    final List<String> names = new ArrayList<>();
    for (final TypeIndex index : database.getSchema().getType(collectionName).getAllIndexes(false))
      if (index.getPropertyNames().size() == 1 && property.equals(index.getPropertyNames().getFirst()))
        names.add(index.getName());
    return names;
  }

  /**
   * Writes a derived key on every record of a collection, in transactions of {@link #BACKFILL_BATCH} records: the key of the
   * {@code _id} ({@code paths == null}, not for a record without one) or the key of {@code paths}. Each batch is its OWN
   * transaction, committed before the index is built on the keys whatever transaction the caller holds (issue #9977). The caller
   * holds the exclusive lock, so no write of the plugin runs meanwhile. One scan of the buckets feeds the batches, so the memory
   * held is one batch of RIDs whatever the size of the collection.
   */
  static void backfillKeys(final Database database, final String collectionName, final String property, final String[] paths) {
    final List<RID> batch = new ArrayList<>(BACKFILL_BATCH);
    final Iterator<Record> scan = database.iterateType(collectionName, true);
    while (scan.hasNext()) {
      batch.add(scan.next().getIdentity());
      if (batch.size() == BACKFILL_BATCH || !scan.hasNext()) {
        writeKeys(database, batch, property, paths);
        batch.clear();
      }
    }
  }

  private static void writeKeys(final Database database, final List<RID> rids, final String property, final String[] paths) {
    final OwnTransaction tx = OwnTransaction.begin((DatabaseInternal) database);
    try {
      for (final RID rid : rids) {
        final MutableDocument record = rid.asDocument().modify();
        final String key;
        if (paths == null) {
          if (!record.has("_id"))
            continue;
          key = MongoKeys.key(MongoDBToSqlTranslator.convertIdToMongoDB(record.get("_id")));
        } else
          key = MongoKeys.key(MongoDBToSqlTranslator.convertDocumentToMongoDB(record), paths);
        // a record that has its key is not written again: what a retry after a failed build (or an interrupted one) costs is the scan
        if (!key.equals(record.getString(property)))
          record.set(property, key).save();
      }
      tx.commit();
    } finally {
      tx.rollbackIfMine();
    }
  }

  /**
   * The unique index on the plain {@code _id} this plugin built before #12073.
   */
  private static TypeIndex findUniqueIdIndex(final DocumentType type) {
    for (final TypeIndex index : type.getAllIndexes(false))
      if (index.isUnique() && index.getPropertyNames().size() == 1 && "_id".equals(index.getPropertyNames().getFirst()))
        return index;
    return null;
  }

  /**
   * Whether the collection has the unique index of its {@code _id}, the one on its derived key or the one built before #12073.
   */
  static boolean hasUniqueIdIndex(final DocumentType type) {
    return MongoKeys.hasIdKeyIndex(type) || findUniqueIdIndex(type) != null;
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

  /**
   * The library's interface answers an int: the count is clamped to it, never wrapped (issue #11449). The {@code count} command reads
   * {@link #countDocuments} instead, which answers the whole count.
   */
  @Override
  public int count(final Document document, final int skip, final int limit) {
    return clampToInt(countDocuments(document, skip, limit));
  }

  /**
   * The documents {@code document} matches, past {@code skip} and up to {@code limit}, as a long: a type holds more documents than an
   * int counts (issue #11449).
   */
  long countDocuments(final Document document, final long skip, final long limit) {
    // The count command's own default for an unspecified limit is -1 (MongoDBDatabaseWrapper#countCollection), so
    // limit <= 0 here means "no limit" - deliberately, not incidentally lining up with an explicit limit: 0.
    final MongoFilter filter = new MongoFilter(database, document);
    long counted = 0;

    if (filter.isEmpty()) {
      // Without a filter every document of the type matches: skip and limit are arithmetic on the type's O(1) counter, the same
      // documents find() iterates (not those of its subtypes), with no record read and no bound narrowed to an int as SQL SKIP/LIMIT do
      counted = Math.max(0L, database.countType(collectionName, false) - Math.max(0L, skip));
      if (limit > 0)
        counted = Math.min(counted, limit);
    } else {
      // the documents are tested one by one: the skipped matches are not counted, the scan stops at the limit
      final long[] skipped = { 0L };
      final long[] matched = { 0L };
      // every matching row, not only those with an identity: a sample of a TIMESERIES type has none (issue #10442)
      filter.scanMatchingRows(database, collectionName, row -> {
        if (skipped[0] < skip) {
          skipped[0]++;
          return true;
        }
        matched[0]++;
        return limit <= 0 || matched[0] < limit;
      });
      counted = matched[0];
    }

    return counted;
  }

  @Override
  public boolean isEmpty() {
    return MongoCollection.super.isEmpty();
  }

  @Override
  public int count() {
    return clampToInt(database.countType(getCollectionName(), false));
  }

  private static int clampToInt(final long count) {
    return (int) Math.min(Integer.MAX_VALUE, count);
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
    forgetIdIndex(database, collectionName);
  }

  /**
   * The documents of a query, read and converted as the cursor is consumed.
   *
   * @param sortOrder the order of the answer, or {@code null} for the order the documents are read in
   */
  private MongoFindCursor queryDocuments(final MongoFilter filter, final MongoSortOrder sortOrder, final int numberToSkip,
      final int numberToReturn, final MongoProjection projection) {
    // a TIMESERIES type keeps its samples in its own engine, not in its buckets: the scan would answer no document while the
    // count answers the samples, so it is read with SQL, which serves them (issue #10442)
    if (filter.isEmpty() && !(database.getSchema().getTypeOrNull(collectionName) instanceof TimeSeriesType)) {
      // SCAN
      final Iterator<?> records = database.iterateType(collectionName, false);
      if (sortOrder == null)
        return new MongoFindCursor(records, null, numberToSkip, numberToReturn, projection);
      return sorted(sortOrder, numberToSkip, numberToReturn, records, projection);
    }

    // EXECUTE A SQL QUERY. A filter the SQL cannot answer exactly (see MongoFilter) is applied to the rows the query returns, in the
    // order it returns them, so skip and limit count the matches
    final Map<String, Object> params = new HashMap<>();
    final StringBuilder sql = new StringBuilder("select from ").append(Identifier.quote(collectionName));

    filter.appendCandidateWhere(sql, params, database.getSchema().getTypeOrNull(collectionName));

    final ResultSet rs = database.query("SQL", sql.toString(), params);
    // the cursor owns the result set once it exists: a sort reads every row at once and is done with it
    boolean handedOver = false;
    try {
      final Iterator<?> rows = filter.isEmpty() ? rs : new FilteredIterator(rs, filter);
      if (sortOrder == null) {
        final MongoFindCursor cursor = new MongoFindCursor(rows, rs, numberToSkip, numberToReturn, projection);
        handedOver = true;
        return cursor;
      }
      return sorted(sortOrder, numberToSkip, numberToReturn, rows, projection);
    } finally {
      if (!handedOver)
        rs.close();
    }
  }

  /** The cursor over the rows in the order of a sort, which is known only once every row is read. */
  private static MongoFindCursor sorted(final MongoSortOrder sortOrder, final int numberToSkip, final int numberToReturn,
      final Iterator<?> rows, final MongoProjection projection) {
    final List<Object> result = new ArrayList<>();
    fillSorted(sortOrder, numberToSkip, numberToReturn, result, rows);
    return new MongoFindCursor(result.iterator(), null, 0L, 0L, projection);
  }

  /**
   * Answers the matching rows in the BSON order of the sort, then skips and limits (issue #12069). SQL {@code ORDER BY} cannot be used:
   * it coerces across types and sees an array as one value, so it answers another order and paginates over other documents. With a
   * limit only the first {@code skip + limit} documents of the order are held, and only those are converted to a reply document: the
   * others are placed by the few properties the sort reads, and the ones answered are converted as the cursor serves them.
   *
   * @param result receives the rows, or the converted documents when every match was converted to be placed
   */
  private static void fillSorted(final MongoSortOrder sortOrder, final int numberToSkip, final int numberToReturn, final List<Object> result,
      final Iterator<?> rows) {
    final int skip = Math.max(numberToSkip, 0);
    if (numberToReturn > 0) {
      final MongoSortOrder.TopEntries<Object> top = new MongoSortOrder.TopEntries<>(sortOrder, (int) Math.min(Integer.MAX_VALUE,
          (long) skip + numberToReturn));
      long position = 0;
      while (rows.hasNext()) {
        final Object row = rows.next();
        final Object[] keys = sortOrder.keyOf(MongoDBToSqlTranslator.toMatchDocument(propertiesOf(row), sortOrder.properties()));
        if (top.accepts(keys))
          top.offer(new MongoSortOrder.Entry<>(keys, position, row));
        ++position;
      }
      final List<MongoSortOrder.Entry<Object>> sorted = top.sorted();
      for (int i = skip; i < sorted.size(); i++)
        result.add(sorted.get(i).value);
    } else {
      // every match is answered: each is converted once, and its keys read off the reply document. Known cost: without a limit
      // every match is held before the sort (mongod would spill to disk past 100MB), as the SQL ORDER BY this replaces held them
      final List<MongoSortOrder.Entry<Document>> entries = new ArrayList<>();
      while (rows.hasNext()) {
        final Document document = MongoFindCursor.toDocument(rows.next());
        entries.add(new MongoSortOrder.Entry<>(sortOrder.keyOf(document), 0, document));
      }
      sortOrder.sort(entries);
      for (int i = skip; i < entries.size(); i++)
        result.add(entries.get(i).value);
    }
  }

  private static Map<String, Object> propertiesOf(final Object row) {
    if (row instanceof Result result)
      return result.toPropertyMap(false);
    if (row instanceof com.arcadedb.database.Document document)
      return document.toMap(false);
    throw new IllegalArgumentException("Object not supported: " + (row != null ? row.getClass().getName() : null));
  }

  // com.arcadedb.database.Document is spelled out in full below: its simple name is the one of the MongoDB Document imported here
  /**
   * The rows (or records) of an iterator that match a filter.
   */
  private static final class FilteredIterator implements Iterator<Object> {
    private final Iterator<?> rows;
    private final MongoFilter filter;
    private       Object      next;

    FilteredIterator(final Iterator<?> rows, final MongoFilter filter) {
      this.rows = rows;
      this.filter = filter;
    }

    @Override
    public boolean hasNext() {
      while (next == null && rows.hasNext()) {
        final Object candidate = rows.next();
        final boolean matches;
        if (candidate instanceof Result row)
          matches = filter.matchesRow(row);
        else if (candidate instanceof com.arcadedb.database.Document document)
          matches = filter.matches(document);
        else
          throw new IllegalArgumentException("Object not supported: " + (candidate != null ? candidate.getClass().getName() : null));
        if (matches)
          next = candidate;
      }
      return next != null;
    }

    @Override
    public Object next() {
      if (!hasNext())
        throw new NoSuchElementException();
      final Object result = next;
      next = null;
      return result;
    }
  }
}
