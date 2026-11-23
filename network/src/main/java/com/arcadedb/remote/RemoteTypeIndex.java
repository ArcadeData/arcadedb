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

import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.IndexMetadata;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The description of a type index as the server lists it in {@code schema:indexes}, returned by the index lookups and the index
 * creation methods of {@link RemoteSchema} and {@link RemoteDocumentType} (issue #12193). It answers the index's metadata and
 * {@link #drop()}; the client holds none of the index's entries, so reading or writing them throws
 * {@link UnsupportedOperationException} (run a query instead, the server picks the index), and {@link #isReadyForQueries()} is
 * false so that no client side planner (the Gremlin traversal over a {@link RemoteDatabase}) tries to read it.
 * <p>
 * It is a snapshot of one row, taken when the index was looked up or created. The row is the one {@code FetchFromSchemaIndexesStep}
 * of the engine writes ({@code Issue12193RemoteTypeIndexTest} pins the shape read here): the listing has one row for the type index and one per bucket
 * index under it, and tells them apart by the {@code fileId} only the bucket indexes have.
 * <p>
 * The {@link TypeIndex} members that are not on the {@link com.arcadedb.index.Index} interface read the bucket indexes under the
 * type index, which a client does not hold: those that would answer from none of them as if the index were empty (the metadata, the
 * page size, the compaction) throw {@link UnsupportedOperationException} here too, and the ones the row describes (unique, valid,
 * online) answer from it.
 */
public class RemoteTypeIndex extends TypeIndex {
  private final RemoteDatabase                     remoteDatabase;
  private final String                             name;
  private final String                             typeName;
  private final List<String>                       propertyNames;
  private final Schema.INDEX_TYPE                  indexType;
  private final boolean                            unique;
  private final boolean                            automatic;
  private final boolean                            valid;
  private final boolean                            supportsOrderedIterations;
  private final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy;
  private final Type[]                             keyTypes;
  private volatile boolean                         dropped;

  /**
   * @param type the type that declares the index, the one the row names (a sub type asking for an index its super type declares gets
   *             the super type's)
   */
  RemoteTypeIndex(final RemoteDatabase remoteDatabase, final DocumentType type, final Result row) {
    super(row.getProperty("name"), type);
    this.remoteDatabase = remoteDatabase;
    this.name = row.getProperty("name");
    this.typeName = row.getProperty("typeName");
    this.propertyNames = propertyNamesOf(row.getProperty("properties"));
    this.indexType = enumOrNull(Schema.INDEX_TYPE.class, row.getProperty("indexType"));
    this.unique = Boolean.TRUE.equals(row.getProperty("unique"));
    this.automatic = !Boolean.FALSE.equals(row.getProperty("automatic"));
    // ABSENT ON A SERVER OLDER THAN THE COLUMN (#5469): NOT KNOWN TO BE INVALID IS VALID
    this.valid = !Boolean.FALSE.equals(row.getProperty("valid"));
    this.supportsOrderedIterations = Boolean.TRUE.equals(row.getProperty("supportsOrderedIterations"));
    this.nullStrategy = enumOrNull(LSMTreeIndexAbstract.NULL_STRATEGY.class, row.getProperty("nullStrategy"));

    final List<?> keyTypeNames = row.getProperty("keyTypes");
    this.keyTypes = keyTypeNames == null ?
        null :
        keyTypeNames.stream().map(t -> enumOrNull(Type.class, t)).toArray(Type[]::new);
  }

  /**
   * Whether a {@code schema:indexes} row describes a type index, not one of the bucket indexes the server lists next to it: a
   * bucket index has a file, a type index has none ({@code TypeIndex#getFileId()} is -1 and {@code FetchFromSchemaIndexesStep}
   * writes the {@code fileId} only above that).
   */
  static boolean isTypeIndexRow(final Result row) {
    return !row.hasProperty("fileId") && row.getProperty("name") != null;
  }

  /** The server lists the properties of an index as a list holding the list of the names. */
  private static List<String> propertyNamesOf(final Object value) {
    if (!(value instanceof List<?> list) || list.isEmpty())
      return Collections.emptyList();
    final List<?> names = list.getFirst() instanceof List<?> inner ? inner : list;
    return names.stream().map(Object::toString).toList();
  }

  private static <E extends Enum<E>> E enumOrNull(final Class<E> enumClass, final Object value) {
    if (value == null)
      return null;
    try {
      return Enum.valueOf(enumClass, value.toString());
    } catch (final IllegalArgumentException e) {
      return null;
    }
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getTypeName() {
    return typeName;
  }

  @Override
  public List<String> getPropertyNames() {
    return propertyNames;
  }

  @Override
  public Schema.INDEX_TYPE getType() {
    return indexType;
  }

  @Override
  public boolean isUnique() {
    return unique;
  }

  @Override
  public boolean isAutomatic() {
    return automatic;
  }

  @Override
  public boolean supportsOrderedIterations() {
    return supportsOrderedIterations;
  }

  @Override
  public LSMTreeIndexAbstract.NULL_STRATEGY getNullStrategy() {
    return nullStrategy;
  }

  @Override
  public Type[] getKeyTypes() {
    return keyTypes;
  }

  /** Whether the server listed the index as valid, and it was not dropped through this object since. */
  @Override
  public boolean isValid() {
    return valid && !dropped;
  }

  @Override
  public boolean isDropped() {
    return dropped;
  }

  /** Whether the server listed the index as valid (the row says it is complete too), and it was not dropped since. */
  @Override
  public boolean isOnline() {
    return isValid();
  }

  /** The bucket indexes are the server's: the row's {@code valid} says whether the index is complete, so it is never asked here. */
  @Override
  public boolean isComplete() {
    return true;
  }

  @Override
  public List<Bucket> getBucketsWithoutIndex() {
    return Collections.emptyList();
  }

  /** Whether the index is unique and was not dropped: the base class answers false for an index with no bucket index under it. */
  @Override
  public boolean isUniqueIfPresent() {
    return unique && !dropped;
  }

  /** Always false: the client cannot read the index, a query that needs it runs on the server. */
  @Override
  public boolean isReadyForQueries() {
    return false;
  }

  @Override
  public void drop() {
    if (dropped)
      // AS THE EMBEDDED INDEX: NOTHING LEFT TO DROP
      return;
    remoteDatabase.getSchema().dropIndex(name);
    dropped = true;
  }

  @Override
  public boolean equals(final Object obj) {
    return obj instanceof RemoteTypeIndex other && name.equals(other.name) && Objects.equals(typeName, other.typeName)
        && propertyNames.equals(other.propertyNames);
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @Override
  public String toString() {
    return name;
  }

  // THE ENTRIES LIVE ON THE SERVER: A QUERY READS THEM THROUGH THE INDEX THERE

  @Override
  public long countEntries() {
    throw unsupported("countEntries()");
  }

  @Override
  public IndexCursor iterator(final boolean ascendingOrder) {
    throw unsupported("iterator()");
  }

  @Override
  public IndexCursor iterator(final boolean ascendingOrder, final Object[] fromKeys, final boolean inclusive) {
    throw unsupported("iterator()");
  }

  @Override
  public IndexCursor range(final boolean ascending, final Object[] beginKeys, final boolean beginKeysInclusive,
      final Object[] endKeys, final boolean endKeysInclusive) {
    throw unsupported("range()");
  }

  @Override
  public IndexCursor getForQuery(final Object[] keys) {
    throw unsupported("getForQuery()");
  }

  @Override
  public IndexCursor prefixForQuery(final Object[] prefix) {
    throw unsupported("prefixForQuery()");
  }

  @Override
  public IndexCursor get(final Object[] keys) {
    throw unsupported("get()");
  }

  @Override
  public IndexCursor get(final Object[] keys, final int limit) {
    throw unsupported("get()");
  }

  @Override
  public void put(final Object[] keys, final RID[] rid) {
    throw unsupported("put()");
  }

  @Override
  public void remove(final Object[] keys) {
    throw unsupported("remove()");
  }

  @Override
  public void remove(final Object[] keys, final Identifiable rid) {
    throw unsupported("remove()");
  }

  @Override
  public boolean compact() {
    throw unsupported("compact()");
  }

  @Override
  public long build(final int buildIndexBatchSize, final boolean sharesCallerTransaction, final BuildIndexCallback callback) {
    throw unsupported("build()");
  }

  @Override
  public Map<String, Long> getStats() {
    throw unsupported("getStats()");
  }

  @Override
  public void setNullStrategy(final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy) {
    throw unsupported("setNullStrategy()");
  }

  // THE ROW DOES NOT CARRY THE SETTINGS OF THE FILES UNDER THE INDEX, AND THE BASE CLASS READS THEM FROM THE FIRST BUCKET INDEX, WHICH A
  // CLIENT DOES NOT HOLD: IT ANSWERED null, AN IndexException "PROBABLY HAS BEEN DROPPED" OR (scheduleCompaction()) true

  @Override
  public IndexMetadata getMetadata() {
    throw unsupported("getMetadata()");
  }

  @Override
  public IndexMetadata getMetadataForNewFile() {
    throw unsupported("getMetadataForNewFile()");
  }

  @Override
  public int getPageSize() {
    throw unsupported("getPageSize()");
  }

  @Override
  public int getPageSizeForNewFile() {
    throw unsupported("getPageSizeForNewFile()");
  }

  @Override
  public JSONObject toJSON() {
    throw unsupported("toJSON()");
  }

  @Override
  public String getMostRecentFileName() {
    throw unsupported("getMostRecentFileName()");
  }

  @Override
  public byte[] getBinaryKeyTypes() {
    throw unsupported("getBinaryKeyTypes()");
  }

  @Override
  public Object[] convertLookupKeys(final Object[] keys) {
    throw unsupported("convertLookupKeys()");
  }

  @Override
  public List<? extends Index> getIndexesByKeys(final Object[] keys) {
    throw unsupported("getIndexesByKeys()");
  }

  @Override
  public boolean scheduleCompaction() {
    throw unsupported("scheduleCompaction()");
  }

  @Override
  public boolean recomputeStatistics() {
    throw unsupported("recomputeStatistics()");
  }

  private UnsupportedOperationException unsupported(final String method) {
    return new UnsupportedOperationException(
        method + " is not supported on the index '" + name + "' of a remote database. Run a query, the server uses the index");
  }
}
