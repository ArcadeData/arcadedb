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
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.EmbeddedDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.DateUtils;
import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

import java.lang.reflect.Array;
import java.time.LocalDateTime;
import java.util.*;

/**
 * Created by luigidellaquila on 21/07/16.
 */
@ExcludeFromJacocoGeneratedReport
public interface Result {

  /**
   * Returns the value for the property.
   *
   * @param name the property name
   *
   * @return the property value. If the property value is a persistent record, it only returns the RID. See also  {@link
   * #getElementProperty(String)}
   */
  <T> T getProperty(String name);

  /**
   * Returns the value for the property. If the property does not exist, then the `defaultValue` is returned.
   *
   * @param name         the property name
   * @param defaultValue default value to return in case the property is missing
   *
   * @return the property value. If the property value is a persistent record, it only returns the RID. See also  {@link
   * #getElementProperty(String)}
   */
  <T> T getProperty(String name, Object defaultValue);

  /**
   * Returns what {@link #getProperty(String)} answers when {@link #hasProperty(String)} is true, and {@code absentValue}
   * otherwise, in one lookup where the implementation can do it (issue #8266). Unlike
   * {@link #getProperty(String, Object)}, which falls back to the element when the projected content lacks the name,
   * this keeps exactly the precedence of the {@code hasProperty()}-then-{@code getProperty()} pair. A present property
   * with a {@code null} value answers {@code null}: pass a marker object as {@code absentValue} to tell the two apart.
   *
   * @param name        the property name
   * @param absentValue the value returned when the property is not present
   */
  default Object getPropertyIfPresent(final String name, final Object absentValue) {
    return hasProperty(name) ? getProperty(name) : absentValue;
  }

  /**
   * returns an OElement property from the result
   *
   * @param name the property name
   *
   * @return the property value. Null if the property is not defined or if it's not an OElement
   */
  Record getElementProperty(String name);

  /**
   * The schema {@link Type} declared for the column this property was read from, or {@code null} when there is no
   * such column: a computed expression, an aggregate, a value from an untyped source, or a row that simply never
   * recorded one.
   * <p>
   * It exists because a value's Java class does not always say what the column declared. A {@code java.util.Date}
   * is the materialised form of a DATE column under {@code arcadedb.dateImplementation=java.util.Date} and of a
   * DATETIME column under the matching {@code dateTimeImplementation}, and a column-list projection
   * ({@code SELECT d FROM T}) produces a non-element row whose consumers therefore had nothing to ask - so a
   * genuine DATE was serialized with a spurious time of day (issue #7638). Answering {@code null} is always safe:
   * the caller falls back to whatever it did before.
   *
   * THIS IS THE WHOLE ANSWER, not one source a caller then falls back from. A row that knows about a projection
   * answers from it FIRST and falls back to its backing record itself (see {@code ResultInternal}), so a caller
   * that added its own "and if that was null, ask the record's schema" would undo the distinction: a computed
   * alias colliding with a real column name answers null ON PURPOSE, and a second fallback turns that back into
   * the column's type - which is the collision this method exists to prevent. Ask once, take the answer.
   *
   * @param name the property name as this row publishes it, i.e. the projection alias
   *
   * @return the declared type of the source column, or null
   */
  default Type getPropertyType(final String name) {
    if (!isElement())
      return null;
    final Document element = toElement();
    final DocumentType elementType = element != null ? element.getType() : null;
    final Property declared = elementType != null ? elementType.getPolymorphicPropertyIfExists(name) : null;
    return declared != null ? declared.getType() : null;
  }

  Set<String> getPropertyNames();

  Optional<RID> getIdentity();

  boolean isElement();

  Optional<Document> getElement();

  Document toElement();

  Optional<Record> getRecord();

  default boolean isRecord() {
    return !isProjection();
  }

  boolean isProjection();

  /**
   * return metadata related to current result given a key
   *
   * @param key the metadata key
   *
   * @return metadata related to current result given a key
   */
  Object getMetadata(String key);

  /**
   * return all the metadata keys available
   *
   * @return all the metadata keys available
   */
  Set<String> getMetadataKeys();

  default JSONObject toJSON() {
    if (isElement())
      return getElement().get().toJSON();

    final JSONObject result = new JSONObject();
    for (final String prop : getPropertyNames())
      result.put(prop, valueToJSON(getProperty(prop)));

    return result;
  }

  default Object valueToJSON(final Object val) {
    if (val != null) {
      if (val instanceof Result result) {
        return result.toJSON();
      } else if (val instanceof EmbeddedDocument embedded) {
        return embedded.toJSON();
      } else if (val instanceof Record record) {
        return record.getIdentity() != null ? record.getIdentity().toString() : null;
      } else if (val instanceof Iterable<?> iterable) {
        final JSONArray array = new JSONArray();
        for (final Object o : iterable)
          array.put(valueToJSON(o));
        return array;
      } else if (val instanceof Iterator<?> iterator) {
        final JSONArray array = new JSONArray();
        while (iterator.hasNext())
          array.put(valueToJSON(iterator.next()));
        return array;
      } else if (val instanceof Map<?, ?> map) {
        // Recursing, as the Iterable arm does: JSONObject(Map) writes a nested ZonedDateTime as epoch millis (the zone is
        // gone) and a nested OffsetTime as null (issue #9810). A null key is written as the text "null", where the former
        // JSONObject(Map) conversion refused the whole map: a projection should not fail because of one odd key
        final JSONObject object = new JSONObject();
        for (final Map.Entry<?, ?> entry : map.entrySet())
          object.put(String.valueOf(entry.getKey()), valueToJSON(entry.getValue()));
        return object;
      } else if (val instanceof TraversalPath path) {
        // a path as its {_type, nodes, relationships, length} map rather than its toString() (issue #9913)
        return valueToJSON(path.toMap());
      } else if (JsonSerializer.isNativeCypherTemporal(val, null)) {
        // A time of day, a zoned datetime and a duration have no epoch form that round trips: epoch millis drops the zone and
        // the generic conversion drops an OffsetTime altogether. The ISO text is what the server's own serializer sends
        // (issue #9810)
        return val.toString();
      } else if (val instanceof byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
      } else if (val.getClass().isArray()) {
        final JSONArray array = new JSONArray();
        final int length = Array.getLength(val);
        for (int i = 0; i < length; i++)
          array.put(valueToJSON(Array.get(val, i)));
        return array;
      } else if (val instanceof Date) {
        final Database database = getDatabase();
        if (database != null)
          return DateUtils.format(val, database.getSchema().getDateTimeFormat());
        else
          // No database to take the schema's format from, so the product-wide default pattern stands in - rendered
          // through the same locale-pinned chain (issue #7112) as the branch above. A bare `new SimpleDateFormat()`
          // used to render this: no pattern and no locale, so the JSON a client read back carried the server's
          // default SHORT date/time - `9/18/26, 3:04 PM` on one host, `18.09.26, 15:04` on another, and neither
          // shape parseable by the format every other Date in the same answer uses (issue #7921).
          return DateUtils.format(val, GlobalConfiguration.DATE_TIME_FORMAT.getValueAsString());

      } else if (val instanceof LocalDateTime) {
        final Database database = getDatabase();
        if (database != null)
          return DateUtils.format(val, database.getSchema().getDateTimeFormat());
      } else if (val instanceof Type type)
        return type.name();
    }

    return val;
  }

  Database getDatabase();

  default String encode(final String s) {
    String result = s.replace("\"", "\\\\\"");
    result = result.replace("\n", "\\\\n");
    result = result.replace("\t", "\\\\t");
    return result;
  }

  default boolean isEdge() {
    return getElement().map(x -> x instanceof Edge).orElse(false);
  }

  default boolean isVertex() {
    return getElement().map(x -> x instanceof Vertex).orElse(false);
  }

  default Optional<Vertex> getVertex() {
    if (isVertex()) {
      return Optional.ofNullable((Vertex) getElement().get());
    }
    return Optional.empty();
  }

  default Optional<Edge> getEdge() {
    if (isEdge()) {
      return Optional.ofNullable((Edge) getElement().get());
    }
    return Optional.empty();
  }

  boolean hasProperty(String varName);

  /**
   * The entity behind this row: deliberately element-first, so a row that carries a record answers the record's map, not
   * what a projection made of it. To read the row itself - what {@link #getPropertyNames()} lists and
   * {@link #getProperty(String)} answers - use {@link #toPropertyMap(boolean)}.
   */
  Map<String, Object> toMap();

  /**
   * The properties of this ROW, as a new map: every name {@link #getPropertyNames()} lists, with the value
   * {@link #getProperty(String)} answers for it. A projection that renamed, overrode or excluded a column has already decided
   * what the row contains, so a consumer that wants "the row's properties" - {@code include()}, {@code exclude()},
   * {@code INSERT ... FROM SELECT}, {@code UPDATE ... MERGE} - reads them here and not off {@link #toMap()}, which answers
   * the record behind the row and so resurrected an excluded column and the stored value of an overridden one (issues #9964
   * and #9965).
   * <p>
   * With {@code includeMetadata}, a row backed by a record is first seeded with the record's attributes ({@code @cat},
   * {@code @type}, {@code @rid} and an edge's {@code @in}/{@code @out}), the shape {@link #toJSON()} gives the same row, and
   * the row's properties are written over the seed. An attribute the row dropped stays dropped.
   * <p>
   * This default seeds from the record's {@code toMap(true)}, since a generic row has no way to drop an attribute.
   * {@code ResultInternal} overrides it to seed from the same method its {@code toJSON()} uses, which also honours the
   * attributes its projection excluded ({@code !@rid}) or its {@code removeProperty()} took back.
   *
   * @param includeMetadata whether a record-backed row also answers its record's attributes
   *
   * @return a new, mutable map the caller owns
   */
  default Map<String, Object> toPropertyMap(final boolean includeMetadata) {
    final Set<String> names = getPropertyNames();
    final Map<String, Object> map = new LinkedHashMap<>(names.size() + (includeMetadata ? 5 : 0));
    if (includeMetadata && isElement())
      for (final Map.Entry<String, Object> entry : toElement().toMap(true).entrySet())
        if (entry.getKey().startsWith("@"))
          map.put(entry.getKey(), entry.getValue());

    for (final String name : names)
      map.put(name, getProperty(name));
    return map;
  }
}
