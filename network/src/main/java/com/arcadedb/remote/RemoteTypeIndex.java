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
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

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
 */
public class RemoteTypeIndex extends TypeIndex {
  private final RemoteDatabase                     remoteDatabase;
  private final String                             name;
  private final String                             typeName;
  private final List<String>                       propertyNames;
  private final Schema.INDEX_TYPE                  indexType;
  private final boolean                            unique;
  private final boolean                            automatic;
  private final boolean                            supportsOrderedIterations;
  private final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy;
  private final Type[]                             keyTypes;
  private       boolean                            dropped;

  RemoteTypeIndex(final RemoteDatabase remoteDatabase, final RemoteDocumentType type, final Result row) {
    super(row.getProperty("name"), type);
    this.remoteDatabase = remoteDatabase;
    this.name = row.getProperty("name");
    this.typeName = row.getProperty("typeName");
    this.propertyNames = propertyNamesOf(row.getProperty("properties"));
    this.indexType = enumOrNull(Schema.INDEX_TYPE.class, row.getProperty("indexType"));
    this.unique = Boolean.TRUE.equals(row.getProperty("unique"));
    this.automatic = !Boolean.FALSE.equals(row.getProperty("automatic"));
    this.supportsOrderedIterations = Boolean.TRUE.equals(row.getProperty("supportsOrderedIterations"));
    this.nullStrategy = enumOrNull(LSMTreeIndexAbstract.NULL_STRATEGY.class, row.getProperty("nullStrategy"));

    final List<?> keyTypeNames = row.getProperty("keyTypes");
    this.keyTypes = keyTypeNames == null ?
        null :
        keyTypeNames.stream().map(t -> enumOrNull(Type.class, t)).toArray(Type[]::new);
  }

  /**
   * Whether a {@code schema:indexes} row describes a type index, not one of the bucket indexes the server lists next to it: a
   * bucket index has a file, a type index has none.
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

  @Override
  public boolean isValid() {
    return !dropped;
  }

  @Override
  public boolean isDropped() {
    return dropped;
  }

  /** Always false: the client cannot read the index, a query that needs it runs on the server. */
  @Override
  public boolean isReadyForQueries() {
    return false;
  }

  @Override
  public void drop() {
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

  private UnsupportedOperationException unsupported(final String method) {
    return new UnsupportedOperationException(
        method + " is not supported on the index '" + name + "' of a remote database. Run a query, the server uses the index");
  }
}
