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

import java.math.*;
import java.time.*;
import java.util.*;

/**
 * Document interface. Vertex and Edge both extend the Document interface.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
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

  EmbeddedDocument getEmbedded(String propertyName);

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
}
