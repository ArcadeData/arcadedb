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

import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;

import java.time.temporal.ChronoUnit;

/**
 * One indexed property of one bucket's projection.
 *
 * @param indexFileId  file id of the ColumnarIndex that owns the column (columns are keyed by it, not by property name)
 * @param property     the indexed property name
 * @param propertyType the schema type of the property
 * @param kind         the storage kind, {@link ColumnKind#of(Type, boolean)} of the type
 * @param byItem       whether a list property is indexed per item
 * @param precision    the epoch unit of a date column ({@code DAYS} for DATE), null for any other
 */
public record ColumnDefinition(int indexFileId, String property, Type propertyType, ColumnKind kind, boolean byItem, ChronoUnit precision) {
  /**
   * Whether a value is stored the same way in both columns (same property type, kind, BY ITEM and precision), whatever index file and
   * property name each belongs to: the bucket columns of one polymorphic index differ by file id only, and a reader decodes one with the
   * other's definition exactly when this holds.
   */
  public boolean sameEncoding(final ColumnDefinition other) {
    return other != null && propertyType == other.propertyType && kind == other.kind && byItem == other.byItem
        && precision == other.precision;
  }

  public JSONObject toJSON() {
    final JSONObject json = new JSONObject();
    json.put("indexFileId", indexFileId);
    json.put("property", property);
    json.put("propertyType", propertyType.name());
    json.put("kind", kind.name());
    json.put("byItem", byItem);
    if (precision != null)
      json.put("precision", precision.name());
    return json;
  }

  public static ColumnDefinition fromJSON(final JSONObject json) {
    final String precisionName = json.getString("precision", null);
    return new ColumnDefinition(json.getInt("indexFileId"), json.getString("property"), Type.valueOf(json.getString("propertyType")),
        ColumnKind.valueOf(json.getString("kind")), json.getBoolean("byItem", false),
        precisionName == null ? null : ChronoUnit.valueOf(precisionName));
  }
}
