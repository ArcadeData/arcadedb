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
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;
import org.json.JSONObject;

import java.util.*;

/**
 * Mutable document implementation. Nested objects are not tracked, so if you update any embedded objects, you need to call {@link #save()} to mark the record
 * as dirty in the current transaction.
 *
 * @author Luca Garulli
 */
public class MutableDocument extends BaseDocument implements RecordInternal {
  private   Map<String, Object> map;
  protected boolean             dirty = false;

  protected MutableDocument(final Database database, final DocumentType type, final RID rid) {
    super(database, type, rid, null);
    this.map = new LinkedHashMap<>(type.getPropertyNames().size());
  }

  protected MutableDocument(final Database database, final DocumentType type, final RID rid, final Binary buffer) {
    super(database, type, rid, buffer);
    buffer.position(buffer.position() + 1); // SKIP RECORD TYPE
  }

  public synchronized void merge(final Map<String, Object> other) {
    for (Map.Entry<String, Object> entry : other.entrySet())
      set(entry.getKey(), entry.getValue());
  }

  public synchronized boolean isDirty() {
    return dirty;
  }

  @Override
  public synchronized void setBuffer(final Binary buffer) {
    super.setBuffer(buffer);
    dirty = false;
    //map = null; // AVOID RESETTING HERE FOR INDEXES THAT CAN LOOKUP UP FOR FIELDS CAUSING AN UNMARSHALLING
  }

  @Override
  public synchronized void unsetDirty() {
    map = null;
    dirty = false;
  }

  public synchronized MutableDocument fromMap(final Map<String, Object> map) {
    this.map = new LinkedHashMap<>(map.size());
    for (Map.Entry<String, Object> entry : map.entrySet())
      this.map.put(entry.getKey(), convertValueToSchemaType(entry.getKey(), entry.getValue(), type));

    dirty = true;
    return this;
  }

  @Override
  public synchronized Map<String, Object> toMap() {
    checkForLazyLoadingProperties();
    return Collections.unmodifiableMap(map);
  }

  public synchronized MutableDocument fromJSON(final JSONObject json) {
    return fromMap(new JSONSerializer(database).json2map(json));
  }

  @Override
  public synchronized JSONObject toJSON() {
    checkForLazyLoadingProperties();
    final JSONObject result = new JSONSerializer(database).map2json(map);
    result.put("@type", type.getName());
    if (getIdentity() != null)
      result.put("@rid", getIdentity().toString());
    return result;
  }

  @Override
  public synchronized boolean has(String propertyName) {
    checkForLazyLoadingProperties();
    return map.containsKey(propertyName);
  }

  public synchronized Object get(final String propertyName) {
    checkForLazyLoadingProperties();
    return map.get(propertyName);
  }

  /**
   * Sets the property value in the document. If the property has been defined in the schema, the value is converted according to the property type.
   */
  public synchronized MutableDocument set(final String name, Object value) {
    checkForLazyLoadingProperties();
    dirty = true;
    value = setTransformValue(value, name);
    map.put(name, convertValueToSchemaType(name, value, type));
    return this;
  }

  /**
   * Sets the property values in the document. If any properties has been defined in the schema, the value is converted according to the property type.
   *
   * @param properties Array containing pairs of name (String) and value (Object)
   */
  public synchronized MutableDocument set(final Object... properties) {
    if (properties == null || properties.length == 0)
      throw new IllegalArgumentException("Empty list of properties");

    if (properties.length % 2 != 0)
      throw new IllegalArgumentException("Properties must be an even pair of key/values");

    checkForLazyLoadingProperties();
    dirty = true;

    if (properties.length > 0) {
      for (int p = 0; p < properties.length; p += 2) {
        final String propertyName = (String) properties[p];
        final Object value = setTransformValue(properties[p + 1], propertyName);
        map.put(propertyName, convertValueToSchemaType(propertyName, value, type));
      }
    }

    return this;
  }

  /**
   * Creates a new embedded document attached to the current document. If the property name already exists, and it is a collection, then the embedded document
   * is added to the collection.
   *
   * @param embeddedTypeName Embedded type name
   * @param propertyName     Current document's property name where the embedded document is stored
   *
   * @return MutableEmbeddedDocument instance
   */
  public synchronized MutableEmbeddedDocument newEmbeddedDocument(final String embeddedTypeName, final String propertyName) {
    final Object old = get(propertyName);

    final MutableEmbeddedDocument emb = database.newEmbeddedDocument(new EmbeddedModifierProperty(this, propertyName), embeddedTypeName);
    if (old instanceof Collection)
      ((Collection<EmbeddedDocument>) old).add(emb);
    else
      set(propertyName, emb);

    return emb;
  }

  /**
   * Creates a new embedded document attached to the current document. If the property name already exists, and it is a map, then the embedded document
   * is added to the collection.
   *
   * @param embeddedTypeName Embedded type name
   * @param propertyName     Current document's property name where the embedded document is stored
   * @param mapKey           key for the map to assign the embedded document
   *
   * @return MutableEmbeddedDocument instance
   */
  public synchronized MutableEmbeddedDocument newEmbeddedDocument(final String embeddedTypeName, final String propertyName, final String mapKey) {
    final Object old = get(propertyName);

    final MutableEmbeddedDocument emb = database.newEmbeddedDocument(new EmbeddedModifierProperty(this, propertyName), embeddedTypeName);

    if (old == null) {
      final Map<String, EmbeddedDocument> embMap = new HashMap<>();
      embMap.put(mapKey, emb);
      set(propertyName, embMap);
    } else if (old instanceof Map)
      ((Map<String, EmbeddedDocument>) old).put(mapKey, emb);
    else
      throw new IllegalArgumentException("Property '" + propertyName + "' is '" + old.getClass() + "', but null or Map was expected");

    return emb;
  }

  /**
   * Creates a new embedded document attached to the current document inside a map that must be previously created and set.
   *
   * @param embeddedTypeName Embedded type name
   * @param propertyName     Current document's property name where the embedded document is stored
   * @param propertyMapKey   Key to use when storing the embedded document in the map
   *
   * @return MutableEmbeddedDocument instance
   */
  public synchronized MutableEmbeddedDocument newEmbeddedDocument(final String embeddedTypeName, final String propertyName, final Object propertyMapKey) {
    final Object old = get(propertyName);

    if (old == null)
      throw new IllegalArgumentException("Cannot store an embedded document in a null map. Create and set the map first");

    if (old instanceof Collection)
      throw new IllegalArgumentException("Cannot store an embedded document in a map because a collection was found");

    if (!(old instanceof Map))
      throw new IllegalArgumentException("Cannot store an embedded document in a map because another value was found instead of a Map");

    final MutableEmbeddedDocument emb = database.newEmbeddedDocument(new EmbeddedModifierProperty(this, propertyName), embeddedTypeName);
    ((Map<Object, EmbeddedDocument>) old).put(propertyMapKey, emb);

    return emb;
  }

  /**
   * Sets the property values in the document from a map. If any properties has been defined in the schema, the value is converted according to the property type.
   *
   * @param properties {@literal Map<String,Object>} containing pairs of name (String) and value (Object)
   */
  public synchronized MutableDocument set(final Map<String, Object> properties) {
    checkForLazyLoadingProperties();
    dirty = true;

    for (Map.Entry<String, Object> entry : properties.entrySet()) {
      final String propertyName = entry.getKey();
      final Object value = setTransformValue(entry.getValue(), propertyName);
      map.put(propertyName, convertValueToSchemaType(propertyName, value, type));
    }

    return this;
  }

  public synchronized Object remove(final String name) {
    checkForLazyLoadingProperties();
    dirty = true;
    return map.remove(name);
  }

  public synchronized MutableDocument save() {
    dirty = true;
    if (rid != null)
      database.updateRecord(this);
    else
      database.createRecord(this);
    return this;
  }

  public synchronized MutableDocument save(final String bucketName) {
    dirty = true;
    if (rid != null)
      throw new IllegalStateException("Cannot update a record in a custom bucket");

    database.createRecord(this, bucketName);
    return this;
  }

  @Override
  public synchronized void setIdentity(final RID rid) {
    this.rid = rid;
  }

  @Override
  public synchronized String toString() {
    final StringBuilder result = new StringBuilder(256);
    if (rid != null)
      result.append(rid);
    if (type != null) {
      result.append('@');
      result.append(type.getName());
    }
    result.append('[');
    if (map == null) {
      result.append('?');
    } else {
      int i = 0;
      for (Map.Entry<String, Object> entry : map.entrySet()) {
        if (i > 0)
          result.append(',');

        result.append(entry.getKey());
        result.append('=');
        result.append(entry.getValue());
        i++;
      }
    }
    result.append(']');
    return result.toString();
  }

  @Override
  public synchronized Set<String> getPropertyNames() {
    checkForLazyLoadingProperties();
    return map.keySet();
  }

  public synchronized MutableDocument modify() {
    return this;
  }

  @Override
  public synchronized void reload() {
    dirty = false;
    map = null;
    buffer = null;
    super.reload();
  }

  protected void checkForLazyLoadingProperties() {
    if (this.map == null) {
      if (buffer == null) {
        if (this instanceof EmbeddedDocument)
          return;

        reload();
      }

      if (buffer == null)
        return;

      buffer.position(propertiesStartingPosition);
      this.map = this.database.getSerializer().deserializeProperties(this.database, buffer, new EmbeddedModifierObject(this));
    }
  }

  private Object convertValueToSchemaType(final String name, final Object value, final DocumentType type) {
    if (value == null || value == JSONObject.NULL)
      return null;

    final Property prop = type.getPolymorphicPropertyIfExists(name);
    if (prop != null)
      try {
        return Type.convert(database, value, prop.getType().getDefaultJavaType());
      } catch (Exception e) {
        throw new IllegalArgumentException("Cannot convert type '" + value.getClass() + "' to '" + prop.getType().name() + "' found in property '" + name + "'",
            e);
      }

    return value;
  }

  private Object setTransformValue(final Object value, final String propertyName) {
    // SET DIRTY TO FORCE RE-MARSHALL. IF THE RECORD COMES FROM ANOTHER DATABASE WITHOUT A FULL RE-MARSHALL, IT WILL HAVE THE DICTIONARY IDS OF THE OTHER DATABASE
    if (value instanceof EmbeddedDocument) {
      if (!((EmbeddedDocument) value).getDatabase().getName().equals(database.getName())) {
        ((BaseDocument) value).buffer.rewind();
        final MutableDocument newRecord = (MutableDocument) database.getRecordFactory()
            .newModifiableRecord(database, ((EmbeddedDocument) value).getType(), null, ((BaseDocument) value).buffer,
                new EmbeddedModifierProperty(this, propertyName));
        newRecord.buffer = null;
        newRecord.map = new LinkedHashMap<>();
        newRecord.dirty = true;
        newRecord.set(((BaseDocument) value).toMap());
        return newRecord;
      }
    } else if (value instanceof List) {
      List<Object> list = (List<Object>) value;
      for (int i = 0; i < list.size(); i++) {
        Object v = list.get(i);
        if (v instanceof Document && !((Document) v).getDatabase().getName().equals(database.getName())) {
          ((BaseDocument) v).buffer.rewind();
          final MutableDocument newRecord = (MutableDocument) database.getRecordFactory()
              .newModifiableRecord(database, ((EmbeddedDocument) v).getType(), null, ((BaseDocument) v).buffer,
                  new EmbeddedModifierProperty(this, propertyName));
          newRecord.buffer = null;
          newRecord.map = new LinkedHashMap<>();
          newRecord.dirty = true;
          newRecord.set(((BaseDocument) v).toMap());
          list.set(i, newRecord);
        }
      }
    } else if (value instanceof Map) {
      final Map<Object, Object> map = (Map<Object, Object>) value;
      for (Map.Entry<Object, Object> entry : map.entrySet()) {
        final Object v = entry.getValue();
        if (v instanceof Document && !((Document) v).getDatabase().getName().equals(database.getName())) {
          ((BaseDocument) v).buffer.rewind();
          final MutableDocument newRecord = (MutableDocument) database.getRecordFactory()
              .newModifiableRecord(database, ((EmbeddedDocument) v).getType(), null, ((BaseDocument) v).buffer,
                  new EmbeddedModifierProperty(this, propertyName));
          newRecord.buffer = null;
          newRecord.map = new LinkedHashMap<>();
          newRecord.dirty = true;
          newRecord.set(((BaseDocument) v).toMap());
          map.put(entry.getKey(), newRecord);
        }
      }
    }
    return value;
  }
}
