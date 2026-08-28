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
package com.arcadedb.database;

import com.arcadedb.schema.DocumentType;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Document interface. Vertex and Edge both extend the Document interface.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@ExcludeFromJacocoGeneratedReport
public interface Document extends Record {
  byte RECORD_TYPE = 0;

  MutableDocument modify();

  DetachedDocument detach();

  boolean has(String propertyName);

  Object get(String propertyName);

  String getString(String propertyName);

  Boolean getBoolean(String propertyName);

  Byte getByte(String propertyName);

  Short getShort(String propertyName);

  Integer getInteger(String propertyName);

  Long getLong(String propertyName);

  Float getFloat(String propertyName);

  Double getDouble(String propertyName);

  BigDecimal getDecimal(String propertyName);

  byte[] getBinary(String propertyName);

  Date getDate(String propertyName);

  /**
   * Returns a java.util.Calendar object from a datetime property.
   *
   * @since 23.1.1
   */
  Calendar getCalendar(String propertyName);

  /**
   * Returns a java.time.LocalDate object from a date property.
   *
   * @since 23.1.1
   */
  LocalDate getLocalDate(String propertyName);

  /**
   * Returns a java.time.LocalDateTime object from a datetime property.
   *
   * @since 23.1.1
   */
  LocalDateTime getLocalDateTime(String propertyName);

  /**
   * Returns a java.time.ZonedDateTime object from a datetime property.
   *
   * @since 23.1.1
   */
  ZonedDateTime getZonedDateTime(String propertyName);

  /**
   * Returns a java.time.Instant object from a datetime property.
   *
   * @since 23.1.1
   */
  Instant getInstant(String propertyName);

  Map<String, Object> getMap(String propertyName);

  <T> List<T> getList(String propertyName);

  EmbeddedDocument getEmbedded(String propertyName);

  /**
   * Returns the names of the properties set on this document, in the order they were set.
   * <p>
   * The returned set is an unmodifiable <b>snapshot</b>: it neither reflects later changes to the document nor
   * allows changing the document through it, and it stays valid while the caller mutates the record - so the
   * natural {@code for (name : getPropertyNames()) remove(name)} prune loop works. That contract is the only one
   * every implementation can honour: {@link ImmutableDocument} reads its names out of the serialized buffer and has
   * no live view to hand back, so a caller holding a {@code Document} could not tell whether the set it got was a
   * snapshot or a window onto the record's own key set. {@link MutableDocument} used to return the latter, which let
   * a {@code remove()} on the returned set strip properties while bypassing {@link MutableDocument#remove(String)}
   * entirely - no dirty flag, no validation (issue #6818).
   *
   * @return the property names, as an unmodifiable snapshot
   */
  Set<String> getPropertyNames();

  DocumentType getType();

  String getTypeName();

  /**
   * Returns a map containing the document properties, including metadata such as `@rid`, `@type` and `@cat`.
   */
  default Map<String, Object> toMap() {
    return toMap(true);
  }

  /**
   * Returns a map containing the document properties.
   *
   * @param includeMetadata true to include metadata such as `@rid`, `@type` and `@cat`, otherwise only the document properties
   */
  Map<String, Object> toMap(boolean includeMetadata);

  Map<String, Object> propertiesAsMap();

  @Override
  default Document asDocument() {
    return this;
  }

  @Override
  default Document asDocument(final boolean loadContent) {
    return this;
  }

  JSONObject toJSON(String... includeProperties);
}
