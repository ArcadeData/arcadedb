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
package com.arcadedb.remote;

import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.Component;
import com.arcadedb.engine.Dictionary;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.function.FunctionDefinition;
import com.arcadedb.function.FunctionLibraryDefinition;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.schema.*;
import com.arcadedb.serializer.json.JSONObject;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.stream.Collectors;

/**
 * Remote Schema implementation used by Remote Database. The types are loaded from the server the first time
 * are needed and cached in RAM until the schema is changed, then it is automatically reloaded from the server.
 * You can manually reload the schema by calling the {@link #reload()} method.
 * <p>
 * Concurrent callers on a shared {@link RemoteDatabase} serialize on {@link #reload()} via a
 * synchronized method and a volatile-published snapshot of the types/buckets maps: readers either
 * see the previous complete snapshot or the new one, never a partially-built map. Prior to this
 * change, two threads could race on the {@code null}-gated init and hit
 * {@link java.util.ConcurrentModificationException} inside {@link java.util.HashMap#computeIfAbsent}.
 * <p>
 * A type/bucket lookup by name that misses the cache (e.g. one created afterward by raw DDL through
 * {@code command()}, which - unlike the {@code createXxxType()} helpers - does not invalidate this
 * cache, or by another connection/session entirely) retries with a single {@link #reload()} before
 * giving up, so a genuinely nonexistent name still fails fast, and a name that exists but was created
 * after this cache warmed up is found on the very next lookup regardless of timing. A time-based
 * debounce on that retry was considered and rejected: it can silently suppress the retry for a lookup
 * that happens to land within the debounce window of an earlier, unrelated reload - exactly the
 * cross-connection scenario this class exists to fix. A caller that walks many known-nonexistent
 * names in a hot loop does pay one reload per miss; that cost is bounded per call and was judged
 * preferable to reintroducing stale reads. The same trade-off applies under concurrent misses: several
 * threads missing at once each still call {@link #reload()} in turn after serializing on its
 * {@code synchronized} lock - there is no "someone already refreshed since my check" short-circuit -
 * so N concurrent misses cost N round trips, not one. Issue #6446.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RemoteSchema implements Schema {
  private final    RemoteDatabase                  remoteDatabase;
  private volatile Map<String, RemoteDocumentType> types   = null;
  private volatile Map<String, RemoteBucket>       buckets = null;

  public RemoteSchema(final RemoteDatabase remoteDatabase) {
    this.remoteDatabase = remoteDatabase;
  }

  @Override
  public boolean existsType(final String typeName) {
    checkSchemaIsLoaded();
    boolean found = types.containsKey(typeName);
    if (!found) {
      reload();
      found = types.containsKey(typeName);
    }
    return found;
  }

  /**
   * Answers from {@code schema:buckets}, the database's bucket list, and not from the buckets attached to the types
   * in {@code schema:types}: a bucket exists as soon as it is created, whether or not any type uses it, so asking the
   * type list answered {@code false} for a standalone bucket - including one the caller had just created through this
   * same API (issue #7031).
   */
  @Override
  public boolean existsBucket(final String bucketName) {
    return remoteDatabase.command("sql", "select from schema:buckets where name = :name", Map.of("name", bucketName)).hasNext();
  }

  @Override
  public boolean existsIndex(final String indexName) {
    // The name must be compared against a string parameter: quoting it as an identifier resolved it as a (missing) property of the
    // record, so this always answered false. Index names routinely carry characters - the comma of the auto-derived
    // `Type[propA,propB]` form - that cannot be inlined unescaped either.
    return remoteDatabase.command("sql", "select from schema:indexes where name = :name", Map.of("name", indexName)).hasNext();
  }

  @Override
  public void dropBucket(final String bucketName) {
    remoteDatabase.command("sql", "drop bucket " + Identifier.quote(bucketName));
  }

  @Override
  public void dropType(final String typeName) {
    remoteDatabase.command("sql", "drop type " + Identifier.quote(typeName));
    invalidateSchema();
  }

  @Override
  public void dropIndex(final String indexName) {
    remoteDatabase.command("sql", "drop index " + Identifier.quote(indexName));
  }

  // TRIGGER MANAGEMENT

  @Override
  public boolean existsTrigger(final String triggerName) {
    final ResultSet result = remoteDatabase.command("sql", "select from schema:triggers where name = :name",
        Map.of("name", triggerName));
    return result.hasNext();
  }

  @Override
  public Trigger getTrigger(final String triggerName) {
    throw new UnsupportedOperationException("getTrigger() is not supported in remote database. Use SQL SELECT FROM schema:triggers instead.");
  }

  @Override
  public Trigger[] getTriggers() {
    throw new UnsupportedOperationException("getTriggers() is not supported in remote database. Use SQL SELECT FROM schema:triggers instead.");
  }

  @Override
  public Trigger[] getTriggersForType(final String typeName) {
    throw new UnsupportedOperationException("getTriggersForType() is not supported in remote database. Use SQL SELECT FROM schema:triggers instead.");
  }

  @Override
  public void createTrigger(final Trigger trigger) {
    throw new UnsupportedOperationException("createTrigger() is not supported in remote database. Use SQL CREATE TRIGGER instead.");
  }

  @Override
  public void dropTrigger(final String triggerName) {
    remoteDatabase.command("sql", "drop trigger " + Identifier.quote(triggerName));
  }

  @Override
  public boolean existsMaterializedView(final String viewName) {
    final ResultSet result = remoteDatabase.command("sql",
        "SELECT FROM schema:materializedViews WHERE name = :name", Map.of("name", viewName));
    return result.hasNext();
  }

  @Override
  public MaterializedView getMaterializedView(final String viewName) {
    final ResultSet result = remoteDatabase.command("sql",
        "SELECT FROM schema:materializedViews WHERE name = :name", Map.of("name", viewName));
    if (result.hasNext())
      return new RemoteMaterializedView(result.next());
    throw new SchemaException("Materialized view '" + viewName + "' not found");
  }

  @Override
  public MaterializedView[] getMaterializedViews() {
    final ResultSet result = remoteDatabase.command("sql", "SELECT FROM schema:materializedViews");
    final List<MaterializedView> views = new ArrayList<>();
    while (result.hasNext())
      views.add(new RemoteMaterializedView(result.next()));
    return views.toArray(new MaterializedView[0]);
  }

  @Override
  public void dropMaterializedView(final String viewName) {
    remoteDatabase.command("sql", "DROP MATERIALIZED VIEW " + Identifier.quote(viewName));
  }

  @Override
  public void alterMaterializedView(final String viewName, final MaterializedViewRefreshMode newMode,
      final long newIntervalMs) {
    throw new UnsupportedOperationException(
        "alterMaterializedView() is not supported in remote database. Use SQL ALTER MATERIALIZED VIEW instead.");
  }

  @Override
  public MaterializedViewBuilder buildMaterializedView() {
    throw new UnsupportedOperationException(
        "buildMaterializedView() is not supported in remote database. Use SQL CREATE MATERIALIZED VIEW instead.");
  }

  @Override
  public boolean existsContinuousAggregate(final String name) {
    final ResultSet result = remoteDatabase.command("sql",
        "SELECT FROM schema:continuousaggregates WHERE name = :name", Map.of("name", name));
    return result.hasNext();
  }

  @Override
  public ContinuousAggregate getContinuousAggregate(final String name) {
    throw new UnsupportedOperationException(
        "getContinuousAggregate() is not supported in remote database. Use SQL SELECT FROM schema:continuousaggregates instead.");
  }

  @Override
  public ContinuousAggregate[] getContinuousAggregates() {
    throw new UnsupportedOperationException(
        "getContinuousAggregates() is not supported in remote database. Use SQL SELECT FROM schema:continuousaggregates instead.");
  }

  @Override
  public void dropContinuousAggregate(final String name) {
    remoteDatabase.command("sql", "DROP CONTINUOUS AGGREGATE " + Identifier.quote(name));
  }

  @Override
  public ContinuousAggregateBuilder buildContinuousAggregate() {
    throw new UnsupportedOperationException(
        "buildContinuousAggregate() is not supported in remote database. Use SQL CREATE CONTINUOUS AGGREGATE instead.");
  }

  @Override
  public Bucket createBucket(final String bucketName) {
    final ResultSet result = remoteDatabase.command("sql", "create bucket " + Identifier.quote(bucketName));
    return new RemoteBucket(result.next().getProperty("bucketName"));
  }

  @Override
  public TypeIndex createTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName,
      final String... propertyNames) {
    final String propList = Arrays.stream(propertyNames).map(Identifier::quote).collect(Collectors.joining(","));
    remoteDatabase.command("sql", "create index on " + Identifier.quote(typeName) +//
        "(" + propList + ") " +//
        (unique ? "UNIQUE" : "NOTUNIQUE") +//
        " ENGINE " + indexType.name());
    return null;
  }

  @Override
  public TypeIndex getOrCreateTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName,
      final String... propertyNames) {
    final String propList = Arrays.stream(propertyNames).map(Identifier::quote).collect(Collectors.joining(","));
    remoteDatabase.command("sql", "create index if not exists on " + Identifier.quote(typeName) +//
        "(" + propList + ") " +//
        (unique ? "UNIQUE" : "NOTUNIQUE") +//
        " ENGINE " + indexType.name());
    return null;
  }

  @Override
  public DocumentType createDocumentType(final String typeName) {
    final ResultSet result = remoteDatabase.command("sql", "create document type " + Identifier.quote(typeName));
    if (result.hasNext())
      return reload().getType(typeName);
    throw new SchemaException("Error on creating document type '" + typeName + "'");
  }

  @Override
  public DocumentType createDocumentType(final String typeName, final int buckets) {
    final ResultSet result = remoteDatabase.command("sql", "create document type " + Identifier.quote(typeName) + " buckets " + buckets);
    if (result.hasNext())
      return reload().getType(typeName);
    throw new SchemaException("Error on creating document type '" + typeName + "'");
  }

  // The getOrCreate*() methods below do not read the row count of the IF NOT EXISTS command: servers up to 26.9.1
  // answer zero rows when the type already exists, so treating an empty result as a failure threw on the exact case
  // the guard is there for (issue #7172). The schema is the authority: command() throws on a server error, and
  // getType() throws SchemaException if the type is still missing afterwards. The kind is checked here rather than
  // left to a bare cast, because IF NOT EXISTS is satisfied by a type of ANY kind: asking for a vertex type whose
  // name is already taken by a document type is a no-op on the server, and the embedded API answers that with
  // SchemaException("Type 'x' is not a vertex type") (TypeBuilder.checkExistingIsCompatible) rather than with a
  // ClassCastException from inside the client.
  @Override
  public DocumentType getOrCreateDocumentType(final String typeName) {
    remoteDatabase.command("sql", "create document type " + Identifier.quote(typeName) + " if not exists");
    return reload().getType(typeName);
  }

  @Override
  public DocumentType getOrCreateDocumentType(final String typeName, final int buckets) {
    remoteDatabase.command("sql", "create document type " + Identifier.quote(typeName) + " if not exists buckets " + buckets);
    return reload().getType(typeName);
  }

  @Override
  public VertexType createVertexType(final String typeName) {
    final ResultSet result = remoteDatabase.command("sql", "create vertex type " + Identifier.quote(typeName));
    if (result.hasNext())
      return asKind(reload().getType(typeName), VertexType.class, "vertex");
    throw new SchemaException("Error on creating vertex type '" + typeName + "'");
  }

  @Override
  public VertexType getOrCreateVertexType(String typeName, int buckets) {
    remoteDatabase.command("sql", "create vertex type " + Identifier.quote(typeName) + " if not exists buckets " + buckets);
    return asKind(reload().getType(typeName), VertexType.class, "vertex");
  }

  @Override
  public VertexType getOrCreateVertexType(final String typeName) {
    remoteDatabase.command("sql", "create vertex type " + Identifier.quote(typeName) + " if not exists");
    return asKind(reload().getType(typeName), VertexType.class, "vertex");
  }

  @Override
  public VertexType createVertexType(String typeName, int buckets) {
    final ResultSet result = remoteDatabase.command("sql", "create vertex type " + Identifier.quote(typeName) + " buckets " + buckets);
    if (result.hasNext())
      return asKind(reload().getType(typeName), VertexType.class, "vertex");
    throw new SchemaException("Error on creating vertex type '" + typeName + "'");
  }

  @Override
  public EdgeType createEdgeType(final String typeName) {
    final ResultSet result = remoteDatabase.command("sql", "create edge type " + Identifier.quote(typeName));
    if (result.hasNext())
      return asKind(reload().getType(typeName), EdgeType.class, "edge");
    throw new SchemaException("Error on creating edge type '" + typeName + "'");
  }

  @Override
  public EdgeType getOrCreateEdgeType(String typeName) {
    remoteDatabase.command("sql", "create edge type " + Identifier.quote(typeName) + " if not exists");
    return asKind(reload().getType(typeName), EdgeType.class, "edge");
  }

  @Override
  public EdgeType createEdgeType(String typeName, int buckets) {
    final ResultSet result = remoteDatabase.command("sql", "create edge type " + Identifier.quote(typeName) + " buckets " + buckets);
    if (result.hasNext())
      return asKind(reload().getType(typeName), EdgeType.class, "edge");
    throw new SchemaException("Error on creating edge type '" + typeName + "'");
  }

  @Override
  public EdgeType getOrCreateEdgeType(final String typeName, final int buckets) {
    remoteDatabase.command("sql", "create edge type " + Identifier.quote(typeName) + " if not exists buckets " + buckets);
    return asKind(reload().getType(typeName), EdgeType.class, "edge");
  }

  /**
   * Answers {@code type} narrowed to {@code expected}, or throws the same {@link SchemaException} the embedded API
   * throws when a {@code getOrCreate*Type()} call finds the name already taken by a type of another kind.
   */
  private static <T extends DocumentType> T asKind(final DocumentType type, final Class<T> expected,
      final String expectedLabel) {
    if (!expected.isInstance(type))
      throw new SchemaException("Type '" + type.getName() + "' is not a " + expectedLabel + " type");
    return expected.cast(type);
  }

  @Override
  public Collection<? extends DocumentType> getTypes() {
    checkSchemaIsLoaded();
    return types.values();
  }

  @Override
  public DocumentType getType(final String typeName) {
    checkSchemaIsLoaded();

    RemoteDocumentType t = types.get(typeName);
    if (t == null) {
      reload();
      t = types.get(typeName);
    }
    if (t == null)
      throw new SchemaException("Type with name '" + typeName + "' was not found");
    return t;
  }

  @Override
  public DocumentType getTypeOrNull(final String typeName) {
    checkSchemaIsLoaded();
    RemoteDocumentType t = types.get(typeName);
    if (t == null) {
      reload();
      t = types.get(typeName);
    }
    return t;
  }

  @Override
  public LocalSchema getEmbedded() {
    return null;
  }

  // UNSUPPORTED METHODS. OPEN A NEW ISSUE TO REQUEST THE SUPPORT OF ADDITIONAL METHODS IN REMOTE
  @Deprecated
  @Override
  public TypeIndexBuilder buildTypeIndex(final String typeName, final String[] propertyNames) {
    throw new UnsupportedOperationException("buildTypeIndex() is not supported in remote database. Use SQL CREATE INDEX instead.");
  }

  @Override
  @Deprecated
  public BucketIndexBuilder buildBucketIndex(final String typeName, final String bucketName, final String[] propertyNames) {
    throw new UnsupportedOperationException("buildBucketIndex() is not supported in remote database. Use SQL CREATE INDEX instead.");
  }

  @Deprecated
  @Override
  public ManualIndexBuilder buildManualIndex(final String indexName, final Type[] keyTypes) {
    throw new UnsupportedOperationException("buildManualIndex() is not supported in remote database. A manual index is an engine-level structure with no remote equivalent.");
  }

  @Deprecated
  @Override
  public TypeBuilder<DocumentType> buildDocumentType() {
    throw new UnsupportedOperationException("buildDocumentType() is not supported in remote database. Use SQL CREATE DOCUMENT TYPE instead.");
  }

  @Deprecated
  @Override
  public TypeBuilder<VertexType> buildVertexType() {
    throw new UnsupportedOperationException("buildVertexType() is not supported in remote database. Use SQL CREATE VERTEX TYPE instead.");
  }

  @Deprecated
  @Override
  public TypeBuilder<EdgeType> buildEdgeType() {
    throw new UnsupportedOperationException("buildEdgeType() is not supported in remote database. Use SQL CREATE EDGE TYPE instead.");
  }

  /**
   * A builder that accumulates the same state as the embedded one and renders it as {@code CREATE TIMESERIES TYPE}
   * DDL at {@code create()} (issue #7399). One body of builder code therefore runs against an embedded
   * {@code Database} and against a {@link RemoteDatabase} unchanged.
   */
  @Override
  public TimeSeriesTypeBuilder buildTimeSeriesType() {
    return new RemoteTimeSeriesTypeBuilder(remoteDatabase, this);
  }

  @Deprecated
  @Override
  public DocumentType createDocumentType(String typeName, List<Bucket> buckets) {
    throw new UnsupportedOperationException("createDocumentType() is not supported in remote database. Use SQL CREATE DOCUMENT TYPE instead.");
  }

  @Deprecated
  @Override
  public DocumentType createDocumentType(String typeName, int buckets, int pageSize) {
    throw new UnsupportedOperationException("createDocumentType() is not supported in remote database. Use SQL CREATE DOCUMENT TYPE instead.");
  }

  @Deprecated
  @Override
  public DocumentType createDocumentType(String typeName, List<Bucket> buckets, int pageSize) {
    throw new UnsupportedOperationException("createDocumentType() is not supported in remote database. Use SQL CREATE DOCUMENT TYPE instead.");
  }

  @Deprecated
  @Override
  public DocumentType getOrCreateDocumentType(String typeName, int buckets, int pageSize) {
    throw new UnsupportedOperationException("getOrCreateDocumentType() is not supported in remote database. Use SQL CREATE DOCUMENT TYPE IF NOT EXISTS instead.");
  }

  @Deprecated
  @Override
  public EdgeType createEdgeType(String typeName, List<Bucket> buckets) {
    throw new UnsupportedOperationException("createEdgeType() is not supported in remote database. Use SQL CREATE EDGE TYPE instead.");
  }

  @Deprecated
  @Override
  public VertexType createVertexType(String typeName, List<Bucket> buckets) {
    throw new UnsupportedOperationException("createVertexType() is not supported in remote database. Use SQL CREATE VERTEX TYPE instead.");
  }

  @Deprecated
  @Override
  public VertexType createVertexType(String typeName, int buckets, int pageSize) {
    throw new UnsupportedOperationException("createVertexType() is not supported in remote database. Use SQL CREATE VERTEX TYPE instead.");
  }

  @Deprecated
  @Override
  public VertexType createVertexType(String typeName, List<Bucket> buckets, int pageSize) {
    throw new UnsupportedOperationException("createVertexType() is not supported in remote database. Use SQL CREATE VERTEX TYPE instead.");
  }

  @Deprecated
  @Override
  public VertexType getOrCreateVertexType(String typeName, int buckets, int pageSize) {
    throw new UnsupportedOperationException("getOrCreateVertexType() is not supported in remote database. Use SQL CREATE VERTEX TYPE IF NOT EXISTS instead.");
  }

  @Deprecated
  @Override
  public EdgeType createEdgeType(String typeName, int buckets, int pageSize) {
    throw new UnsupportedOperationException("createEdgeType() is not supported in remote database. Use SQL CREATE EDGE TYPE instead.");
  }

  @Deprecated
  @Override
  public EdgeType createEdgeType(String typeName, List<Bucket> buckets, int pageSize) {
    throw new UnsupportedOperationException("createEdgeType() is not supported in remote database. Use SQL CREATE EDGE TYPE instead.");
  }

  @Deprecated
  @Override
  public EdgeType getOrCreateEdgeType(String typeName, int buckets, int pageSize) {
    throw new UnsupportedOperationException("getOrCreateEdgeType() is not supported in remote database. Use SQL CREATE EDGE TYPE IF NOT EXISTS instead.");
  }

  @Deprecated
  @Override
  public TimeZone getTimeZone() {
    throw new UnsupportedOperationException("getTimeZone() is not supported in remote database. The time zone is a server-side setting with no remote accessor.");
  }

  @Deprecated
  @Override
  public void setTimeZone(TimeZone timeZone) {
    throw new UnsupportedOperationException("setTimeZone() is not supported in remote database. The time zone is a server-side setting with no remote accessor.");
  }

  @Deprecated
  @Override
  public ZoneId getZoneId() {
    throw new UnsupportedOperationException("getZoneId() is not supported in remote database. The zone id is a server-side setting with no remote accessor.");
  }

  @Deprecated
  @Override
  public void setZoneId(ZoneId zoneId) {
    throw new UnsupportedOperationException("setZoneId() is not supported in remote database. The zone id is a server-side setting with no remote accessor.");
  }

  @Deprecated
  @Override
  public String getDateFormat() {
    throw new UnsupportedOperationException("getDateFormat() is not supported in remote database. Use SQL SELECT FROM schema:database instead.");
  }

  @Deprecated
  @Override
  public void setDateFormat(final String dateFormat) {
    throw new UnsupportedOperationException("setDateFormat() is not supported in remote database. Use SQL ALTER DATABASE `arcadedb.dateFormat` instead.");
  }

  @Deprecated
  @Override
  public String getDateTimeFormat() {
    throw new UnsupportedOperationException("getDateTimeFormat() is not supported in remote database. Use SQL SELECT FROM schema:database instead.");
  }

  @Deprecated
  @Override
  public void setDateTimeFormat(final String dateTimeFormat) {
    throw new UnsupportedOperationException("setDateTimeFormat() is not supported in remote database. Use SQL ALTER DATABASE `arcadedb.dateTimeFormat` instead.");
  }

  @Deprecated
  @Override
  public String getEncoding() {
    throw new UnsupportedOperationException("getEncoding() is not supported in remote database. The encoding is a server-side setting with no remote accessor.");
  }

  @Deprecated
  @Override
  public void setEncoding(final String encoding) {
    throw new UnsupportedOperationException("setEncoding() is not supported in remote database. The encoding is a server-side setting with no remote accessor.");
  }

  @Deprecated
  @Override
  public Schema registerFunctionLibrary(final FunctionLibraryDefinition library) {
    throw new UnsupportedOperationException("registerFunctionLibrary() is not supported in remote database. A function library is registered in the server JVM, not over the wire.");
  }

  @Deprecated
  @Override
  public Schema unregisterFunctionLibrary(final String name) {
    throw new UnsupportedOperationException("unregisterFunctionLibrary() is not supported in remote database. A function library is registered in the server JVM, not over the wire.");
  }

  @Deprecated
  @Override
  public Iterable<FunctionLibraryDefinition> getFunctionLibraries() {
    throw new UnsupportedOperationException("getFunctionLibraries() is not supported in remote database. A function library is registered in the server JVM, not over the wire.");
  }

  @Deprecated
  @Override
  public boolean hasFunctionLibrary(final String name) {
    throw new UnsupportedOperationException("hasFunctionLibrary() is not supported in remote database. A function library is registered in the server JVM, not over the wire.");
  }

  @Deprecated
  @Override
  public FunctionLibraryDefinition getFunctionLibrary(final String name) throws IllegalArgumentException {
    throw new UnsupportedOperationException("getFunctionLibrary() is not supported in remote database. A function library is registered in the server JVM, not over the wire.");
  }

  @Deprecated
  @Override
  public FunctionDefinition getFunction(final String libraryName, final String functionName) throws IllegalArgumentException {
    throw new UnsupportedOperationException("getFunction() is not supported in remote database. A function library is registered in the server JVM, not over the wire.");
  }

  @Override
  public JSONObject getExtension(final String name) {
    return null;
  }

  @Override
  public void setExtension(final String name, final JSONObject value) {

  }

  @Deprecated
  @Override
  public Component getFileById(final int id) {
    throw new UnsupportedOperationException("getFileById() is not supported in remote database. Files are an engine-level structure with no remote equivalent.");
  }

  @Deprecated
  @Override
  public RemoteBucket getBucketByName(final String name) {
    checkSchemaIsLoaded();
    RemoteBucket b = buckets.get(name);
    if (b == null) {
      reload();
      b = buckets.get(name);
    }
    if (b == null)
      throw new SchemaException("Bucket '" + name + "' not found");
    return b;
  }

  @Deprecated
  @Override
  public RemoteBucket getBucketByNameIfExists(final String name) {
    checkSchemaIsLoaded();
    RemoteBucket b = buckets.get(name);
    if (b == null) {
      reload();
      b = buckets.get(name);
    }
    return b;
  }

  @Deprecated
  @Override
  public Component getFileByIdIfExists(final int id) {
    throw new UnsupportedOperationException("getFileByIdIfExists() is not supported in remote database. Files are an engine-level structure with no remote equivalent.");
  }

  @Deprecated
  @Override
  public Collection<? extends Bucket> getBuckets() {
    checkSchemaIsLoaded();
    return buckets.values();
  }

  @Deprecated
  @Override
  public LocalBucket getBucketById(final int id) {
    throw new UnsupportedOperationException("getBucketById() is not supported in remote database. Bucket ids are an engine-level structure with no remote equivalent. Use SQL SELECT FROM schema:types to list the buckets of a type.");
  }

  /**
   * Throws rather than returning {@code null}, matching {@link #getBucketById(int)} and
   * {@link #getFileByIdIfExists(int)} on this class: a remote schema carries buckets by name only, so it cannot
   * resolve an id at all. Returning {@code null} would claim "no such bucket", which is a different and wrong
   * answer to "I cannot tell".
   */
  @Deprecated
  @Override
  public LocalBucket getBucketByIdIfExists(final int id) {
    throw new UnsupportedOperationException("getBucketByIdIfExists() is not supported in remote database. Bucket ids are an engine-level structure with no remote equivalent. Use SQL SELECT FROM schema:types to list the buckets of a type.");
  }

  @Deprecated
  @Override
  public DocumentType copyType(final String typeName, final String newTypeName, final Class<? extends DocumentType> newType,
      final int buckets, final int pageSize, final int transactionBatchSize) {
    throw new UnsupportedOperationException("copyType() is not supported in remote database. There is no SQL equivalent: create the target type and copy the records with SQL INSERT INTO ... FROM SELECT.");
  }

  @Deprecated
  @Override
  public Index[] getIndexes() {
    throw new UnsupportedOperationException("getIndexes() is not supported in remote database. Use SQL SELECT FROM schema:indexes instead.");
  }

  @Deprecated
  @Override
  public Index getIndexByName(final String indexName) {
    throw new UnsupportedOperationException("getIndexByName() is not supported in remote database. Use SQL SELECT FROM schema:indexes instead.");
  }

  @Deprecated
  @Override
  public TypeIndex createTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName,
      final String[] propertyNames, final int pageSize) {
    throw new UnsupportedOperationException("createTypeIndex() is not supported in remote database. Use SQL CREATE INDEX instead.");
  }

  @Deprecated
  @Override
  public TypeIndex createTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName,
      final String[] propertyNames, final int pageSize, Index.BuildIndexCallback callback) {
    throw new UnsupportedOperationException("createTypeIndex() is not supported in remote database. Use SQL CREATE INDEX instead.");
  }

  @Deprecated
  @Override
  public TypeIndex createTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName,
      final String[] propertyNames, final int pageSize, final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy,
      final Index.BuildIndexCallback callback) {
    throw new UnsupportedOperationException("createTypeIndex() is not supported in remote database. Use SQL CREATE INDEX instead.");
  }

  @Deprecated
  @Override
  public TypeIndex getOrCreateTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName,
      final String[] propertyNames, final int pageSize, final Index.BuildIndexCallback callback) {
    throw new UnsupportedOperationException("getOrCreateTypeIndex() is not supported in remote database. Use SQL CREATE INDEX IF NOT EXISTS instead.");
  }

  @Deprecated
  @Override
  public TypeIndex getOrCreateTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName,
      final String[] propertyNames, final int pageSize, LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy,
      Index.BuildIndexCallback callback) {
    throw new UnsupportedOperationException("getOrCreateTypeIndex() is not supported in remote database. Use SQL CREATE INDEX IF NOT EXISTS instead.");
  }

  @Deprecated
  @Override
  public Index createBucketIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName, final String bucketName,
      final String[] propertyNames, final int pageSize, final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy,
      final Index.BuildIndexCallback callback) {
    throw new UnsupportedOperationException("createBucketIndex() is not supported in remote database. Use SQL CREATE INDEX instead.");
  }

  @Deprecated
  @Override
  public Index createManualIndex(final INDEX_TYPE indexType, final boolean unique, final String indexName, final Type[] keyTypes,
      final int pageSize, final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy) {
    throw new UnsupportedOperationException("createManualIndex() is not supported in remote database. A manual index is an engine-level structure with no remote equivalent.");
  }

  @Deprecated
  @Override
  public String getTypeNameByBucketId(final int bucketId) {
    throw new UnsupportedOperationException("getTypeNameByBucketId() is not supported in remote database. Bucket ids are an engine-level structure with no remote equivalent. Use SQL SELECT FROM schema:types to map types to their buckets.");
  }

  @Deprecated
  @Override
  public DocumentType getTypeByBucketId(final int bucketId) {
    throw new UnsupportedOperationException("getTypeByBucketId() is not supported in remote database. Bucket ids are an engine-level structure with no remote equivalent. Use SQL SELECT FROM schema:types to map types to their buckets.");
  }

  @Deprecated
  @Override
  public DocumentType getInvolvedTypeByBucketId(final int bucketId) {
    throw new UnsupportedOperationException("getInvolvedTypeByBucketId() is not supported in remote database. Bucket ids are an engine-level structure with no remote equivalent. Use SQL SELECT FROM schema:types to map types to their buckets.");
  }

  @Deprecated
  @Override
  public DocumentType getTypeByBucketName(final String bucketName) {
    ResultSet resultSet = remoteDatabase.command("sql", "select from schema:types where buckets contains '" + bucketName + "'");

    final Result result = resultSet.nextIfAvailable();
    return result != null ? remoteDatabase.getSchema().getType(result.getProperty("name")) : null;
  }

  @Deprecated
  @Override
  public Dictionary getDictionary() {
    throw new UnsupportedOperationException("getDictionary() is not supported in remote database. The schema dictionary is an engine-level structure with no remote equivalent.");
  }

  @Deprecated
  @Override
  public TypeIndex getOrCreateTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName,
      final String[] propertyNames, int pageSize) {
    throw new UnsupportedOperationException("getOrCreateTypeIndex() is not supported in remote database. Use SQL CREATE INDEX IF NOT EXISTS instead.");
  }

  void invalidateSchema() {
    types = null;
  }

  /**
   * Force a reload of the schema from the server. Synchronized so concurrent callers on a shared
   * {@link RemoteDatabase} don't race on the {@code types}/{@code buckets} init; the new maps are
   * built locally and only published to the volatile fields once complete, so readers always see
   * either the previous snapshot or the new one, never a partially populated map.
   */
  public synchronized RemoteSchema reload() {
    final ResultSet result = remoteDatabase.command("sql", "select from schema:types");

    final List<Result> cached = new ArrayList<>();
    while (result.hasNext())
      cached.add(result.next());

    final Map<String, RemoteBucket>       newBuckets = new HashMap<>();
    final Map<String, RemoteDocumentType> newTypes   = new HashMap<>();
    final Map<String, RemoteDocumentType> previous   = this.types;

    for (Result record : cached) {
      final List<String> typeBucketNames = record.getProperty("buckets");
      for (String typeBucketName : typeBucketNames)
        newBuckets.computeIfAbsent(typeBucketName, name -> new RemoteBucket(name));
    }

    for (Result record : cached) {
      final String typeName = record.getProperty("name");
      // Type codes here come from FetchFromSchemaTypesStep: full names for document/vertex/edge,
      // KIND_CODE ("t") for timeseries. The single-char codes from LocalDocumentType.toJSON
      // ("d"/"v"/"e"/"t") are NOT used on this path.
      final String typeCode = record.getProperty("type");
      RemoteDocumentType type = previous != null ? previous.get(typeName) : null;
      // A cached instance is reused only while it is still the right KIND of type (issue #7740). A
      // drop-and-recreate can change that under a live connection - the same name coming back as a TIMESERIES
      // type, say - and reusing the old instance left getType(name) answering with the old Java class forever,
      // an explicit reload() included, because the switch below ran only for a name the snapshot did not carry.
      if (type != null && !isOfKind(type, typeCode))
        type = null;
      if (type == null)
        type = newTypeFor(typeCode, typeName, record);
      else
        type.reload(record);
      newTypes.put(typeName, type);
    }

    this.buckets = newBuckets;
    this.types   = newTypes;
    return this;
  }

  /**
   * The remote type a schema record describes, by its type code. Unknown codes are a document type, with a
   * warning: a newer server's kind must not make the whole schema unusable on an older client.
   */
  private RemoteDocumentType newTypeFor(final String typeCode, final String typeName, final Result record) {
    switch (typeCode) {
    case "document":
      return new RemoteDocumentType(remoteDatabase, record);
    case "vertex":
      return new RemoteVertexType(remoteDatabase, record);
    case "edge":
      return new RemoteEdgeType(remoteDatabase, record,
          record.hasProperty("bidirectional") ? (Boolean) record.getProperty("bidirectional") : true,
          record.hasProperty("lightweight") && (Boolean) record.getProperty("lightweight"),
          record.hasProperty("unique") && (Boolean) record.getProperty("unique"));
    case LocalTimeSeriesType.KIND_CODE:
      return new RemoteTimeSeriesType(remoteDatabase, record);
    default:
      LogManager.instance().log(this, Level.WARNING,
          "Unknown schema type code '%s' for type '%s' - treating as document type", typeCode, typeName);
      return new RemoteDocumentType(remoteDatabase, record);
    }
  }

  /**
   * Whether a cached instance is what {@link #newTypeFor} would build for {@code typeCode} now. Exact classes,
   * not {@code instanceof}: {@link RemoteVertexType} IS a {@link RemoteDocumentType}, so a vertex type recreated
   * as a document type has to come back as a new instance rather than pass as one (issue #7740).
   */
  private static boolean isOfKind(final RemoteDocumentType type, final String typeCode) {
    return switch (typeCode) {
      case "vertex" -> type.getClass() == RemoteVertexType.class;
      case "edge" -> type.getClass() == RemoteEdgeType.class;
      case LocalTimeSeriesType.KIND_CODE -> type.getClass() == RemoteTimeSeriesType.class;
      // "document" and any code this client does not know, both of which build a plain document type.
      default -> type.getClass() == RemoteDocumentType.class;
    };
  }

  private void checkSchemaIsLoaded() {
    if (types == null) {
      synchronized (this) {
        if (types == null)
          reload();
      }
    }
  }
}
