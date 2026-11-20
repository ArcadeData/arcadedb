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
package com.arcadedb.graph;

import com.arcadedb.database.Database;
import com.arcadedb.database.DetachedDocument;
import com.arcadedb.database.Document;
import com.arcadedb.database.EmbeddedDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.serializer.json.JSONObject;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A graph element that exists only in a query result: the virtual nodes and relationships of {@code create.vNode},
 * {@code create.vRelationship} and {@code meta.graph}. It is never stored, so it has no type in the schema, no bucket and
 * no edges of its own; it carries its properties in memory and a synthetic negative id, as the APOC virtual entities do.
 * <p>
 * The identity is {@code #-1:<id>}: bucket -1 is never a real bucket, so the RID cannot name a stored record, and every
 * virtual element gets its own id, so two of them are never equal. Everything that would write or reach the storage
 * answers {@link UnsupportedOperationException}.
 */
public abstract class VirtualElement implements Document {
  private static final AtomicLong NEXT_ID = new AtomicLong();

  private final Database            database;
  private final long                id;
  private final RID                 rid;
  private final Map<String, Object> properties;

  protected VirtualElement(final Database database, final Map<String, Object> properties) {
    this.database = database;
    this.id = -NEXT_ID.incrementAndGet();
    this.rid = new RID(-1, id);
    this.properties = properties == null || properties.isEmpty() ? Map.of() :
        Collections.unmodifiableMap(new LinkedHashMap<>(properties));
  }

  /**
   * The negative number {@code id()} answers for this element, unique among the virtual elements of the server.
   */
  public long getVirtualId() {
    return id;
  }

  /**
   * The text {@code elementId()} answers: the virtual id, as APOC renders it.
   */
  public String getVirtualElementId() {
    return Long.toString(id);
  }

  @Override
  public RID getIdentity() {
    return rid;
  }

  @Override
  public Database getDatabase() {
    return database;
  }

  @Override
  public DocumentType getType() {
    return null;
  }

  @Override
  public boolean has(final String propertyName) {
    return properties.containsKey(propertyName);
  }

  @Override
  public Object get(final String propertyName) {
    return properties.get(propertyName);
  }

  @Override
  public Set<String> getPropertyNames() {
    return properties.keySet();
  }

  @Override
  public Map<String, Object> propertiesAsMap() {
    return properties;
  }

  @Override
  public Map<String, Object> toMap(final boolean includeMetadata) {
    final Map<String, Object> map = new LinkedHashMap<>(properties);
    if (includeMetadata) {
      map.put(Property.RID_PROPERTY, rid.toString());
      final String typeName = getTypeName();
      if (typeName != null)
        map.put(Property.TYPE_PROPERTY, typeName);
    }
    return map;
  }

  @Override
  public JSONObject toJSON(final boolean includeMetadata) {
    final JSONObject json = new JsonSerializer(database).map2json(properties, null, false);
    if (includeMetadata) {
      json.put(Property.RID_PROPERTY, rid.toString());
      final String typeName = getTypeName();
      if (typeName != null)
        json.put(Property.TYPE_PROPERTY, typeName);
    }
    return json;
  }

  @Override
  public JSONObject toJSON(final String... includeProperties) {
    return new JsonSerializer(database).map2json(properties, null, false, includeProperties);
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
  public byte[] getBinary(final String propertyName) {
    return (byte[]) Type.convert(database, get(propertyName), byte[].class);
  }

  @Override
  public Date getDate(final String propertyName) {
    return (Date) Type.convertOrNull(database, get(propertyName), Date.class);
  }

  @Override
  public Calendar getCalendar(final String propertyName) {
    return (Calendar) Type.convertOrNull(database, get(propertyName), Calendar.class);
  }

  @Override
  public LocalDate getLocalDate(final String propertyName) {
    return (LocalDate) Type.convertOrNull(database, get(propertyName), LocalDate.class);
  }

  @Override
  public LocalDateTime getLocalDateTime(final String propertyName) {
    return (LocalDateTime) Type.convertOrNull(database, get(propertyName), LocalDateTime.class);
  }

  @Override
  public ZonedDateTime getZonedDateTime(final String propertyName) {
    return (ZonedDateTime) Type.convertOrNull(database, get(propertyName), ZonedDateTime.class);
  }

  @Override
  public Instant getInstant(final String propertyName) {
    return (Instant) Type.convertOrNull(database, get(propertyName), Instant.class);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Map<String, Object> getMap(final String propertyName) {
    return (Map<String, Object>) Type.convert(database, get(propertyName), Map.class);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> List<T> getList(final String propertyName) {
    return (List<T>) Type.convert(database, get(propertyName), List.class);
  }

  @Override
  public EmbeddedDocument getEmbedded(final String propertyName) {
    return (EmbeddedDocument) Type.convert(database, get(propertyName), EmbeddedDocument.class);
  }

  @Override
  public int size() {
    return properties.size();
  }

  @Override
  public Record getRecord() {
    return this;
  }

  @Override
  public Record getRecord(final boolean loadContent) {
    return this;
  }

  @Override
  public void reload() {
    // Nothing to reload: the element lives only in memory
  }

  @Override
  public void delete() {
    throw unsupported("deleted");
  }

  @Override
  public DetachedDocument detach() {
    throw unsupported("detached");
  }

  /**
   * The refusal of a write this element cannot take, for a caller that meets a virtual element before it reaches one of the
   * mutators above (a relabelling, for one, rewrites the node under another type before it modifies anything).
   *
   * @param what the write, as a past participle: {@code "relabelled"}
   */
  public UnsupportedOperationException unsupported(final String what) {
    return new UnsupportedOperationException("A virtual " + kind() + " is not stored and cannot be " + what);
  }

  protected abstract String kind();

  @Override
  public int hashCode() {
    return Long.hashCode(id);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj)
      return true;
    return obj instanceof VirtualElement other && other.id == id;
  }
}
