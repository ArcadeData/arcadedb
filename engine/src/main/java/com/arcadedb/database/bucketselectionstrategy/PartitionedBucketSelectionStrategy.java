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

import java.util.*;

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
  public int getBucketIdByKeys(final Object[] keyValues, final boolean async) {
    if (propertyNames != null) {
      int hash = 0;
      for (int i = 0; i < keyValues.length; i++) {
        final Object value = keyValues[i];
        if (value != null)
          hash += value.hashCode();
      }
      return (hash & 0x7fffffff) % total;
    }

    return super.getBucketIdByKeys(keyValues, async);
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
