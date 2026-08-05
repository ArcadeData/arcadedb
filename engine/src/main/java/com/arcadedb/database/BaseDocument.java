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
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.JavaBinarySerializer;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.serializer.json.JSONObject;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

public abstract class BaseDocument extends BaseRecord implements Document, DocumentInternal, Serializable, Externalizable {
  protected final DocumentType type;
  protected       int          propertiesStartingPosition = 1;

  protected BaseDocument(final Database database, final DocumentType type, final RID rid, final Binary buffer) {
    super(database, rid, buffer);
    this.type = type;
  }

  @Override
  public Document asDocument() {
    return this;
  }

  @Override
  public Document asDocument(final boolean loadContent) {
    return this;
  }

  @Override
  public DetachedDocument detach() {
    return new DetachedDocument(this);
  }

  @Override
  public String getString(final String propertyName) {
    return (String) Type.convert(database, get(propertyName), String.class);
  }

  @Override
  public Boolean getBoolean(final String propertyName) {
    return (Boolean) Type.convert(database, get(propertyName), Boolean.class);
  }

  @Override
  public Byte getByte(final String propertyName) {
    return (Byte) Type.convert(database, get(propertyName), Byte.class);
  }

  @Override
  public Short getShort(final String propertyName) {
    return (Short) Type.convert(database, get(propertyName), Short.class);
  }

  @Override
  public Integer getInteger(final String propertyName) {
    return (Integer) Type.convert(database, get(propertyName), Integer.class);
  }

  @Override
  public Long getLong(final String propertyName) {
    return (Long) Type.convert(database, get(propertyName), Long.class);
  }

  @Override
  public Float getFloat(final String propertyName) {
    return (Float) Type.convert(database, get(propertyName), Float.class);
  }

  @Override
  public Double getDouble(final String propertyName) {
    return (Double) Type.convert(database, get(propertyName), Double.class);
  }

  @Override
  public BigDecimal getDecimal(final String propertyName) {
    return (BigDecimal) Type.convert(database, get(propertyName), BigDecimal.class);
  }

  @Override
  public Date getDate(final String propertyName) {
    return (Date) Type.convert(database, get(propertyName), Date.class);
  }

  public Calendar getCalendar(final String propertyName) {
    return (Calendar) Type.convert(database, get(propertyName), Calendar.class);
  }

  public LocalDate getLocalDate(final String propertyName) {
    return (LocalDate) Type.convert(database, get(propertyName), LocalDate.class, type.getPropertyIfExists(propertyName));
  }

  public LocalDateTime getLocalDateTime(final String propertyName) {
    return (LocalDateTime) Type.convert(database, get(propertyName), LocalDateTime.class, type.getPropertyIfExists(propertyName));
  }

  public ZonedDateTime getZonedDateTime(final String propertyName) {
    return (ZonedDateTime) Type.convert(database, get(propertyName), ZonedDateTime.class, type.getPropertyIfExists(propertyName));
  }

  public Instant getInstant(final String propertyName) {
    return (Instant) Type.convert(database, get(propertyName), Instant.class, type.getPropertyIfExists(propertyName));
  }

  @Override
  public byte[] getBinary(final String propertyName) {
    return (byte[]) Type.convert(database, get(propertyName), byte[].class);
  }

  @Override
  public Map<String, Object> getMap(final String propertyName) {
    return (Map<String, Object>) Type.convert(database, get(propertyName), Map.class);
  }

  @Override
  public <T> List<T> getList(final String propertyName) {
    return (List<T>) Type.convert(database, get(propertyName), List.class);
  }

  @Override
  public EmbeddedDocument getEmbedded(final String propertyName) {
    return (EmbeddedDocument) Type.convert(database, get(propertyName), EmbeddedDocument.class);
  }

  public DocumentType getType() {
    return type;
  }

  @Override
  public int getPropertiesStartingPosition() {
    return propertiesStartingPosition;
  }

  public String getTypeName() {
    return type.getName();
  }

  @Override
  public byte getRecordType() {
    return RECORD_TYPE;
  }

  /**
   * Re-derives the fixed prefix of a <b>freshly installed</b> buffer and leaves the buffer on the first byte of the
   * properties section, updating {@link #propertiesStartingPosition} to match. {@link #reload()} is its only caller,
   * deliberately: the already-materialised read path only has to move a cursor, and re-deriving the prefix there would
   * charge every property read of every vertex and edge for a prefix parse and two RID allocations.
   * <p>
   * A plain document has no prefix beyond the record-type byte, so the inherited {@code propertiesStartingPosition} of
   * 1 already describes any buffer of this shape and seeking to it is enough. The shapes that do carry a prefix - a
   * vertex with its two edge-list head pointers, an edge with its out/in RIDs - override this to read the prefix out of
   * the <i>current</i> buffer instead of trusting a field that described the previous one. Without that, a
   * {@link #reload()} left the parsed prefix pointing at the pre-reload content: the vertex kept answering with the
   * edges it had before the reload (issue #5771), and an edge whose replacement buffer had shorter compressed RIDs kept
   * a {@code propertiesStartingPosition} pointing past its own properties.
   */
  protected void parseRecordPrefix() {
    buffer.position(propertiesStartingPosition);
  }

  @Override
  public void reload() {
    super.reload();
    if (buffer != null)
      parseRecordPrefix();
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    JavaBinarySerializer.writeExternal(this, out);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException {
    JavaBinarySerializer.readExternal(this, in);
  }

  @Override
  public JSONObject toJSON(final String... includeProperties) {
    return new JsonSerializer(database).map2json(propertiesAsMap(), type, false, includeProperties);
  }
}
