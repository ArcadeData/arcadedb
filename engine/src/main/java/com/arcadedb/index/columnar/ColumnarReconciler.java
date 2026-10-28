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
package com.arcadedb.index.columnar;

import com.arcadedb.schema.ColumnarIndexMetadata;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.IntPredicate;

/**
 * The schema events of spec 6.4 as differences between two schema states, computed by {@link ColumnarIndexManager}'s reconcile:
 * {@link #diff} over the COLUMNAR columns of each projected bucket, {@link #hierarchyDiff} over a fingerprint of every type a COLUMNAR
 * hierarchy reads (R20: the column maps cannot see a change to those types). Pure functions; the events come out sorted by bucket file
 * id, then by index file id, then by type name, so two reconciles of the same change report the same list.
 */
final class ColumnarReconciler {
  /** The fingerprint of a type that does not exist. */
  static final String ABSENT = "absent";

  private ColumnarReconciler() {
  }

  /**
   * The events that turn {@code current} into {@code fromSchema}, both keyed by bucket file id, columns keyed by index file id. A bucket
   * with no column (absent or an empty array) has no projection.
   * <ul>
   * <li>a bucket only in {@code current}: {@code DROP_PROJECTION};</li>
   * <li>a bucket only in {@code fromSchema}: {@code CREATE_EMPTY} when {@code bucketIsEmpty} says so, else one {@code ADD_COLUMN}
   * (reason {@code create}) per column;</li>
   * <li>a bucket in both: {@code DROP_COLUMN} per column gone, {@code ADD_COLUMN} (reason {@code create}) per column added,
   * {@code RETYPE_COLUMN} (reason {@code schema}) per column whose type, kind, BY ITEM or precision changed (a rename too), and
   * {@code RENAME} per column whose property name alone changed.</li>
   * </ul>
   *
   * @param current       the definitions of the previous reconcile (V-B4), not the projections' columns
   * @param fromSchema    the definitions the schema has now
   * @param bucketIsEmpty whether a bucket new to the projections has no record
   */
  static List<ColumnarSchemaEvent> diff(final Map<Integer, ColumnDefinition[]> current, final Map<Integer, ColumnDefinition[]> fromSchema,
      final IntPredicate bucketIsEmpty) {
    final TreeSet<Integer> buckets = new TreeSet<>();
    for (final Map.Entry<Integer, ColumnDefinition[]> entry : current.entrySet())
      if (hasColumns(entry.getValue()))
        buckets.add(entry.getKey());
    for (final Map.Entry<Integer, ColumnDefinition[]> entry : fromSchema.entrySet())
      if (hasColumns(entry.getValue()))
        buckets.add(entry.getKey());

    final List<ColumnarSchemaEvent> events = new ArrayList<>();
    for (final int bucket : buckets) {
      final ColumnDefinition[] before = current.get(bucket);
      final ColumnDefinition[] after = fromSchema.get(bucket);
      if (!hasColumns(after))
        events.add(event(ColumnarSchemaEvent.Kind.DROP_PROJECTION, bucket, -1, null));
      else if (!hasColumns(before)) {
        if (bucketIsEmpty.test(bucket))
          events.add(event(ColumnarSchemaEvent.Kind.CREATE_EMPTY, bucket, -1, null));
        else
          for (final ColumnDefinition column : byIndexFileId(after).values())
            events.add(event(ColumnarSchemaEvent.Kind.ADD_COLUMN, bucket, column.indexFileId(), ColumnarRebuilder.CREATE));
      } else
        diffColumns(bucket, byIndexFileId(before), byIndexFileId(after), events);
    }
    return events;
  }

  private static void diffColumns(final int bucket, final TreeMap<Integer, ColumnDefinition> before,
      final TreeMap<Integer, ColumnDefinition> after, final List<ColumnarSchemaEvent> events) {
    final TreeSet<Integer> ids = new TreeSet<>(before.keySet());
    ids.addAll(after.keySet());
    for (final int id : ids) {
      final ColumnDefinition was = before.get(id);
      final ColumnDefinition is = after.get(id);
      if (is == null)
        events.add(event(ColumnarSchemaEvent.Kind.DROP_COLUMN, bucket, id, null));
      else if (was == null)
        events.add(event(ColumnarSchemaEvent.Kind.ADD_COLUMN, bucket, id, ColumnarRebuilder.CREATE));
      else if (!sameData(was, is))
        events.add(event(ColumnarSchemaEvent.Kind.RETYPE_COLUMN, bucket, id, ColumnarRebuilder.SCHEMA));
      else if (!was.property().equals(is.property()))
        events.add(event(ColumnarSchemaEvent.Kind.RENAME, bucket, id, null));
    }
  }

  /** Whether the two definitions store the same values: everything but the property name is equal. */
  static boolean sameData(final ColumnDefinition a, final ColumnDefinition b) {
    return a.indexFileId() == b.indexFileId() && a.propertyType() == b.propertyType() && a.kind() == b.kind() && a.byItem() == b.byItem()
        && a.precision() == b.precision();
  }

  /**
   * R20: the fingerprint of every type a hierarchy of {@code withHierarchy} names ({@code vertexType} and {@code parentEdge}):
   * {@code "exists|<polymorphic bucket ids, sorted>|<key>:<key type>[,...]"}, the key part listing the {@code key} property of each
   * hierarchy that names the type as its {@code vertexType} ({@code absent} for a property the type does not declare), empty for an
   * edge type; {@link #ABSENT} for a type the schema does not have.
   */
  static Map<String, String> hierarchyFingerprints(final Schema schema, final Collection<ColumnarIndexMetadata> withHierarchy) {
    final TreeMap<String, TreeSet<String>> keysByType = new TreeMap<>();
    for (final ColumnarIndexMetadata metadata : withHierarchy) {
      final JSONObject hierarchy = metadata.getHierarchy();
      if (hierarchy == null)
        continue;
      final String vertexType = hierarchy.getString("vertexType", null);
      if (vertexType != null) {
        final String key = hierarchy.getString("key", null);
        final TreeSet<String> keys = keysByType.computeIfAbsent(vertexType, k -> new TreeSet<>());
        if (key != null)
          keys.add(key);
      }
      final String parentEdge = hierarchy.getString("parentEdge", null);
      if (parentEdge != null)
        keysByType.computeIfAbsent(parentEdge, k -> new TreeSet<>());
    }

    final Map<String, String> fingerprints = new TreeMap<>();
    for (final Map.Entry<String, TreeSet<String>> entry : keysByType.entrySet()) {
      final DocumentType type = schema.getTypeOrNull(entry.getKey());
      if (type == null) {
        fingerprints.put(entry.getKey(), ABSENT);
        continue;
      }
      final StringBuilder fingerprint = new StringBuilder("exists|");
      final List<Integer> bucketIds = new ArrayList<>(type.getBucketIds(true));
      bucketIds.sort(null);
      for (int i = 0; i < bucketIds.size(); i++)
        fingerprint.append(i == 0 ? "" : ",").append(bucketIds.get(i));
      fingerprint.append('|');
      boolean first = true;
      for (final String key : entry.getValue()) {
        final Property property = type.getPolymorphicPropertyIfExists(key);
        fingerprint.append(first ? "" : ",").append(key).append(':').append(property == null ? ABSENT : property.getType().name());
        first = false;
      }
      fingerprints.put(entry.getKey(), fingerprint.toString());
    }
    return fingerprints;
  }

  /** One {@code HIERARCHY_SOURCE_CHANGED} per type whose fingerprint differs, appeared or disappeared, sorted by type name. */
  static List<ColumnarSchemaEvent> hierarchyDiff(final Map<String, String> before, final Map<String, String> after) {
    final TreeSet<String> types = new TreeSet<>(before.keySet());
    types.addAll(after.keySet());
    final List<ColumnarSchemaEvent> events = new ArrayList<>();
    for (final String type : types)
      if (!Objects.equals(before.get(type), after.get(type)))
        events.add(new ColumnarSchemaEvent(ColumnarSchemaEvent.Kind.HIERARCHY_SOURCE_CHANGED, -1, -1, null, type));
    return events;
  }

  private static boolean hasColumns(final ColumnDefinition[] columns) {
    return columns != null && columns.length > 0;
  }

  private static TreeMap<Integer, ColumnDefinition> byIndexFileId(final ColumnDefinition[] columns) {
    final TreeMap<Integer, ColumnDefinition> map = new TreeMap<>();
    for (final ColumnDefinition column : columns)
      map.put(column.indexFileId(), column);
    return map;
  }

  private static ColumnarSchemaEvent event(final ColumnarSchemaEvent.Kind kind, final int bucket, final int indexFileId,
      final String reason) {
    return new ColumnarSchemaEvent(kind, bucket, indexFileId, reason, null);
  }
}
