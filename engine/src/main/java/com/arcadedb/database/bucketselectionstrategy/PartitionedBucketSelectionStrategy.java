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
package com.arcadedb.database.bucketselectionstrategy;

import com.arcadedb.database.Document;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Select the bucket using a partition algorithm computed as the hashed value of the properties values. This allows to predetermine in which bucket is contained
 * a key(s) and therefore a document. There are some limitations on using this implementation: (1) field identified as partition key cannot be modified. (This
 * could be solved in the future by removing and recreating the document in a different bucket. If the record is part of a graph, then the edges will be updated
 * accordingly.)
 *
 * @author Luca Garulli
 */
public class PartitionedBucketSelectionStrategy extends RoundRobinBucketSelectionStrategy {
  private       LocalDocumentType type;
  private final List<String>      propertyNames;

  public PartitionedBucketSelectionStrategy(final List<String> propertyNames) {
    this.propertyNames = Collections.unmodifiableList(propertyNames);
  }

  public PartitionedBucketSelectionStrategy(final JSONObject json) {
    final JSONArray array = json.getJSONArray("properties");
    final List<String> pn = new ArrayList<>(array.length());
    for (int i = 0; i < array.length(); i++)
      pn.add(array.getString(i));
    this.propertyNames = Collections.unmodifiableList(pn);
  }

  @Override
  public BucketSelectionStrategy copy() {
    final PartitionedBucketSelectionStrategy copy = new PartitionedBucketSelectionStrategy(propertyNames);
    copy.total = total;
    copy.type = type;
    return copy;
  }

  @Override
  public void setType(final LocalDocumentType type) {
    super.setType(type);
    this.type = type;

    final TypeIndex index = type.getPolymorphicIndexByProperties(propertyNames);
    if (index == null || !index.isAutomatic() || !index.isUnique())
      throw new IllegalArgumentException("Cannot find a unique index on properties " + propertyNames);
  }

  @Override
  public int getBucketIdByRecord(final Document record, final boolean async) {
    if (propertyNames != null) {
      final DocumentType documentType = record.getType();
      if (!this.type.equals(documentType))
        throw new IllegalArgumentException(
            "Record of type '" + documentType.getName() + "' is not supported by partitioned bucket selection strategy built on type '" + type.getName() + "'");

      int hash = 0;
      for (int i = 0; i < propertyNames.size(); i++) {
        final Object value = record.get(propertyNames.get(i));
        if (value != null)
          hash += value.hashCode();
      }
      return (hash & 0x7fffffff) % total;
    }

    return super.getBucketIdByRecord(record, async);
  }

  @Override
  public int getBucketIdByKeys(final List<String> lookupProperties, final Object[] keyValues, final boolean async) {
    // A record is placed by hashing THIS strategy's properties (see getBucketIdByRecord), so hashing the lookup
    // key only reaches the same bucket when the lookup covers exactly those properties. Anything else - another
    // index of the same type, or a partial key on a composite partition - hashes a different value set and would
    // point at an unrelated bucket, silently missing the record (issue #5589). Decline and let the caller fan out.
    if (!coversPartitionProperties(lookupProperties, keyValues))
      return -1;

    int hash = 0;
    for (int i = 0; i < keyValues.length; i++) {
      final Object value = keyValues[i];
      if (value != null)
        hash += value.hashCode();
    }
    return (hash & 0x7fffffff) % total;
  }

  /**
   * Whether {@code lookupProperties} is exactly this strategy's partition property set, with one key value each.
   * <p>
   * Order is deliberately NOT required: the hash both sides compute is a SUM over the per-value hash codes, which
   * is commutative, so a permutation of the same properties reaches the same bucket.
   * <p>
   * The comparison is multiset equality, not "every lookup property is also a partition property". The weaker test
   * would accept a lookup on {@code [a, a]} against a partition of {@code [a, b]}, which sums a different pair of
   * values than placement did and would prune to the wrong bucket. No index declares a repeated property today, so
   * this is unreachable - but this method is the guard the whole fix rests on, so it enforces the invariant instead
   * of assuming callers uphold it. Counting with nested scans rather than a Set keeps it allocation-free: partition
   * keys hold one to three properties, and this runs on a per-query path.
   */
  private boolean coversPartitionProperties(final List<String> lookupProperties, final Object[] keyValues) {
    if (lookupProperties == null)
      // THE CALLER COULD NOT SAY WHICH PROPERTIES THE KEYS BELONG TO: UNVERIFIABLE, SO NOT A MATCH
      return false;

    final int size = propertyNames.size();
    if (lookupProperties.size() != size || keyValues.length != size)
      return false;

    for (int i = 0; i < size; i++) {
      final String partitionProperty = propertyNames.get(i);
      if (occurrencesOf(partitionProperty, lookupProperties) != occurrencesOf(partitionProperty, propertyNames))
        return false;
    }

    return true;
  }

  private static int occurrencesOf(final String property, final List<String> properties) {
    int occurrences = 0;
    for (int i = 0; i < properties.size(); i++)
      if (properties.get(i).equals(property))
        ++occurrences;
    return occurrences;
  }

  @Override
  public String getName() {
    return "partitioned";
  }

  public List<String> getProperties() {
    return propertyNames;
  }

  @Override
  public JSONObject toJSON() {
    return new JSONObject().put("name", getName()).put("properties", new JSONArray(propertyNames));
  }
}
