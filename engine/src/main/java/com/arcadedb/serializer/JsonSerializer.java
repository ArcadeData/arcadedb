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
package com.arcadedb.serializer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.DetachedDocument;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableEmbeddedDocument;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.IterableGraph;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.LocalVertexType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.DateUtils;

import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

import static com.arcadedb.schema.Property.CAT_PROPERTY;
import static com.arcadedb.schema.Property.IN_PROPERTY;
import static com.arcadedb.schema.Property.OUT_PROPERTY;
import static com.arcadedb.schema.Property.RID_PROPERTY;
import static com.arcadedb.schema.Property.TYPE_PROPERTY;
import static com.arcadedb.utility.CollectionUtils.arrayToList;

public class JsonSerializer {
  private boolean  useCollectionSize         = false;
  private boolean  includeVertexEdges        = true;
  private boolean  useVertexEdgeSize         = true;
  private boolean  useCollectionSizeForEdges = true;
  private Database database;

  JsonSerializer() {
  }

  public JsonSerializer(final Database database) {
    this.database = database;
  }

  public static JsonSerializer createJsonSerializer() {
    return new JsonSerializer();
  }

  public JSONObject serializeDocument(final Document document) {
    final Database database = document.getDatabase();
    final JSONObject object = new JSONObject().setDateFormat(database.getSchema().getDateTimeFormat())
        .setDateTimeFormat(database.getSchema().getDateTimeFormat());

    if (document.getIdentity() != null)
      object.put(RID_PROPERTY, document.getIdentity().toString());
    object.put(TYPE_PROPERTY, document.getTypeName());

    final Map<String, Object> documentAsMap = document.toMap();
    for (final Map.Entry<String, Object> documentEntry : documentAsMap.entrySet()) {
      final String p = documentEntry.getKey();
      Object value = documentEntry.getValue();

      switch (value) {
        case null -> value = JSONObject.NULL;
        case Document document1 -> value = serializeDocument(document1);
        case Collection<?> collection -> serializeCollection(database, collection, null);
        case Map map -> value = serializeMap(database, (Map<Object, Object>) map);
        default -> {
        }
      }

      value = convertNonNumbers(value);

      object.put(p, value);
    }

    setMetadata(document, object);

    return object;
  }

  public JSONObject serializeResult(final Database database, final Result result) {
    final JSONObject object = new JSONObject().setDateFormat(database.getSchema().getDateFormat())
        .setDateTimeFormat(database.getSchema().getDateTimeFormat());

    DocumentType type = null;
    if (result.isElement()) {
      final Document document = result.toElement();
      if (document.getIdentity() != null)
        object.put(RID_PROPERTY, document.getIdentity().toString());
      object.put(TYPE_PROPERTY, document.getTypeName());

      setMetadata(document, object);

      type = document.getType();

      if (result.getMetadata("_projectionName") != null) {
        for (final Map.Entry<String, Object> entry : document.toMap().entrySet()) {
          object.put(entry.getKey(), serializeObject(database, entry.getValue()));
        }
        return object;
      }
    }

    final StringBuilder propertyTypes = new StringBuilder();

    for (final String propertyName : result.getPropertyNames()) {
      Object value = result.getProperty(propertyName);

      final Type propertyType;
      if (type != null && type.existsProperty(propertyName))
        propertyType = type.getProperty(propertyName).getType();
      else if (value != null)
        propertyType = Type.getTypeByClass(value.getClass());
      else
        propertyType = null;

      if (propertyType != null) {
        if (!propertyTypes.isEmpty())
          propertyTypes.append(",");
        propertyTypes.append(propertyName).append(":").append(propertyType.getId());
      }

      value = serializeObject(database, value);

      object.put(propertyName, value);
    }

    return object;
  }

  /**
   * Converts a map to a JSON object with optional type information and property filtering.
   * This method was moved from the deprecated JSONSerializer class.
   */
  public JSONObject map2json(final Map<String, Object> map, final DocumentType type, final boolean includeMetadata,
                             final String... includeProperties) {
    final JSONObject json = new JSONObject();

    final Set<String> includePropertiesSet;
    if (includeProperties.length > 0)
      includePropertiesSet = new HashSet<>(Arrays.asList(includeProperties));
    else
      includePropertiesSet = null;

    final StringBuilder propertyTypes = includeMetadata ? new StringBuilder() : null;

    for (final Map.Entry<String, Object> entry : map.entrySet()) {
      final String propertyName = entry.getKey();

      if (includePropertiesSet != null && !includePropertiesSet.contains(propertyName))
        continue;

      Object value = entry.getValue();

      final Type propertyType;
      if (type != null && type.existsProperty(propertyName))
        propertyType = type.getProperty(propertyName).getType();
      else if (value != null)
        propertyType = Type.getTypeByClass(value.getClass());
      else
        propertyType = null;

      if (includeMetadata && propertyType != null) {
        if (propertyTypes.length() > 0)
          propertyTypes.append(",");
        propertyTypes.append(propertyName).append(":").append(propertyType.getId());
      }

      value = convertToJSONType(value, propertyType);

      if (value instanceof Number number && !Float.isFinite(number.floatValue())) {
        LogManager.instance()
            .log(this, Level.SEVERE, "Found non finite number in map with key '%s', ignore this entry in the " +
                    "conversion",
                propertyName);
        continue;
      }

      json.put(propertyName, value);
    }

    if (propertyTypes != null && !propertyTypes.isEmpty())
      json.put(Property.PROPERTY_TYPES_PROPERTY, propertyTypes);

    return json;
  }

  /**
   * Converts a JSON object to a map.
   * This method was moved from the deprecated JSONSerializer class.
   */
  public Map<String, Object> json2map(final JSONObject json) {
    // Optimized: initial capacity from JSON size
    final Map<String, Object> map = new HashMap<>(json.length());
    for (final String k : json.keySet()) {
      final Object value = convertFromJSONType(json.get(k));
      map.put(k, value);
    }

    return map;
  }

  private Object serializeCollection(final Database database, final Collection<?> value,
                                     Class<? extends Document> entryType) {
    Object result = value;
    if (useCollectionSize) {
      result = value.size();
    } else {
      if (useCollectionSizeForEdges && //
          ((entryType != null && entryType.isAssignableFrom(Edge.class)) || //
              (!value.isEmpty() && value.iterator().next() instanceof Edge)))
        result = value.size();
      else {
        final JSONArray list = new JSONArray();
        for (Object o : value)
          list.put(serializeObject(database, o));

        result = list;
      }
    }
    return result;
  }

  private Object serializeIterator(final Database database, final Iterator<?> value,
                                   final Class<? extends Document> entryType) {
    final List<Object> list = new ArrayList<>();
    while (value.hasNext())
      list.add(value.next());

    return serializeCollection(database, list, entryType);
  }

  private Object serializeResultSet(final Database database, final ResultSet resultSet) {
    final JSONArray array = new JSONArray();
    while (resultSet.hasNext()) {
      final Result row = resultSet.next();
      array.put(serializeResult(database, row));
    }
    return array;
  }

  private Object serializeMap(final Database database, final Map<Object, Object> value) {
    final Object result;
    if (useCollectionSize) {
      result = value.size();
    } else {
      final JSONObject map = new JSONObject().setDateFormat(database.getSchema().getDateFormat())
          .setDateTimeFormat(database.getSchema().getDateTimeFormat());
      for (final Map.Entry<Object, Object> entry : value.entrySet()) {
        final Object o = serializeObject(database, entry.getValue());
        map.put(entry.getKey().toString(), o);
      }
      result = map;
    }
    return result;
  }

  public boolean isUseVertexEdgeSize() {
    return useVertexEdgeSize;
  }

  public JsonSerializer setUseVertexEdgeSize(final boolean useVertexEdgeSize) {
    this.useVertexEdgeSize = useVertexEdgeSize;
    return this;
  }

  public boolean isUseCollectionSize() {
    return useCollectionSize;
  }

  public JsonSerializer setUseCollectionSize(final boolean useCollectionSize) {
    this.useCollectionSize = useCollectionSize;
    return this;
  }

  public boolean isIncludeVertexEdges() {
    return includeVertexEdges;
  }

  public JsonSerializer setIncludeVertexEdges(final boolean includeVertexEdges) {
    this.includeVertexEdges = includeVertexEdges;
    return this;
  }

  public boolean isUseCollectionSizeForEdges() {
    return useCollectionSizeForEdges;
  }

  public JsonSerializer setUseCollectionSizeForEdges(final boolean useCollectionSizeForEdges) {
    this.useCollectionSizeForEdges = useCollectionSizeForEdges;
    return this;
  }

  private void setMetadata(final Document document, final JSONObject object) {
    switch (document) {
      case DetachedDocument doc -> {
        final DocumentType docType = doc.getType();
        if (docType instanceof VertexType)
          object.put(CAT_PROPERTY, "v");
        else if (docType instanceof EdgeType)
          object.put(CAT_PROPERTY, "e");
        else
          object.put(CAT_PROPERTY, "d");
      }
      case Vertex vertex -> {
        object.put(CAT_PROPERTY, "v");
        if (includeVertexEdges) {
          if (useVertexEdgeSize) {
            object.put(OUT_PROPERTY, vertex.countEdges(Vertex.DIRECTION.OUT));
            object.put(IN_PROPERTY, vertex.countEdges(Vertex.DIRECTION.IN));

          } else {
            final JSONArray outEdges = new JSONArray();
            for (final Edge e : vertex.getEdges(Vertex.DIRECTION.OUT))
              outEdges.put(e.getIdentity().toString());
            object.put(OUT_PROPERTY, outEdges);

            final JSONArray inEdges = new JSONArray();
            for (final Edge e : vertex.getEdges(Vertex.DIRECTION.IN))
              inEdges.put(e.getIdentity().toString());
            object.put(IN_PROPERTY, inEdges);
          }
        }
      }
      case Edge edge -> {
        object.put(CAT_PROPERTY, "e");
        object.put(IN_PROPERTY, edge.getIn());
        object.put(OUT_PROPERTY, edge.getOut());
      }
      case null, default -> object.put(CAT_PROPERTY, "d");
    }

  }

  /**
   * Converts a value to JSON-compatible type, handling Documents, Collections, Dates, etc.
   * This method was moved from the deprecated JSONSerializer class.
   */
  private Object convertToJSONType(Object value, final Type type) {
    if (value instanceof Document document) {
      value = document.toJSON(true);
    } else if (value instanceof Collection c) {
      final JSONArray array = new JSONArray();
      for (final Iterator it = c.iterator(); it.hasNext(); )
        array.put(convertToJSONType(it.next(), null));
      value = array;
    } else if (value instanceof Date date)
      value = date.getTime();
    else if (value instanceof Temporal)
      value = DateUtils.dateTimeToTimestamp(value, type != null ? DateUtils.getPrecisionFromType(type) :
          ChronoUnit.MILLIS);
    else if (value instanceof Map) {
      final Map<String, Object> m = (Map<String, Object>) value;
      final JSONObject map = new JSONObject();
      for (final Map.Entry<String, Object> entry : m.entrySet())
        map.put(entry.getKey(), convertToJSONType(entry.getValue(), null));
      value = map;
    }

    return value;
  }

  /**
   * Converts from JSON type back to Java objects, handling embedded documents and collections.
   * This method was moved from the deprecated JSONSerializer class.
   */
  private Object convertFromJSONType(Object value) {
    if (database == null) {
      // If no database is available, return the value as-is
      return value;
    }

    if (value instanceof JSONObject json) {
      final String embeddedTypeName = json.has(Property.TYPE_PROPERTY) ? json.getString(Property.TYPE_PROPERTY) : null;

      if (embeddedTypeName != null) {
        final DocumentType type = database.getSchema().getType(embeddedTypeName);

        if (type instanceof LocalVertexType) {
          final MutableVertex v = database.newVertex(embeddedTypeName);
          v.fromJSON((JSONObject) value);
          value = v;
        } else if (type != null) {
          final MutableEmbeddedDocument embeddedDocument = ((DatabaseInternal) database).newEmbeddedDocument(null,
              embeddedTypeName);
          embeddedDocument.fromJSON((JSONObject) value);
          value = embeddedDocument;
        }
      }
    } else if (value instanceof JSONArray array) {
      final List<Object> list = new ArrayList<>();
      for (int i = 0; i < array.length(); ++i)
        list.add(convertFromJSONType(array.get(i)));
      value = list;
    }

    return value;
  }

  private Object convertNonNumbers(Object value) {
    if (value != null)
      if (value.equals(Double.NaN) || value.equals(Float.NaN))
        // JSON DOES NOT SUPPORT NaN
        value = "NaN";
      else if (value.equals(Double.POSITIVE_INFINITY) || value.equals(Float.POSITIVE_INFINITY))
        // JSON DOES NOT SUPPORT INFINITY
        value = "PosInfinity";
      else if (value.equals(Double.NEGATIVE_INFINITY) || value.equals(Float.NEGATIVE_INFINITY))
        // JSON DOES NOT SUPPORT INFINITY
        value = "NegInfinity";
    return value;
  }

  private Object serializeObject(final Database database, Object value) {
    if (value == null)
      value = JSONObject.NULL;
    else if (value instanceof Document document)
      value = serializeDocument(document);
    else if (value instanceof Result res)
      value = serializeResult(database, res);
    else if (value instanceof ResultSet res)
      value = serializeResultSet(database, res);
    else if (value instanceof Collection<?> coll)
      value = serializeCollection(database, coll, null);
    else if (value instanceof IterableGraph<?> iter)
      value = serializeIterator(database, iter.iterator(), iter.getEntryType());
    else if (value instanceof Iterable<?> iter)
      value = serializeIterator(database, iter.iterator(), null);
    else if (value instanceof Iterator<?> iter)
      value = serializeIterator(database, iter, null);
    else if (value instanceof Map)
      value = serializeMap(database, (Map<Object, Object>) value);
    else if (value.getClass().isArray())
      value = serializeCollection(database, arrayToList(value), null);

    value = convertNonNumbers(value);

    return value;
  }

}
