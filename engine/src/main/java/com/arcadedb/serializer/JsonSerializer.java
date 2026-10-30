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

import com.arcadedb.GlobalConfiguration;
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
import com.arcadedb.query.facet.FacetResult;
import com.arcadedb.query.facet.FacetResults;
import com.arcadedb.query.facet.FacetStats;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
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
import com.arcadedb.serializer.json.NonFiniteNumbers;
import com.arcadedb.utility.DateUtils;

import java.lang.reflect.Array;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Calendar;
import java.util.Date;
import java.util.EnumSet;
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

public class JsonSerializer {
  // Types JSON round-trips to the same Java type (decimals -> Double, 32-bit-signed-range integers -> Integer), so
  // the per-column @props hint is omitted for them; lossy types (LONG, FLOAT, SHORT, BYTE, DECIMAL, temporals, ...) keep it.
  private static final Set<Type> JSON_ROUND_TRIP_FAITHFUL_TYPES = EnumSet.of(Type.BOOLEAN, Type.INTEGER, Type.DOUBLE,
      Type.STRING);

  private boolean  useCollectionSize         = false;
  private boolean  includeVertexEdges        = true;
  private boolean  useVertexEdgeSize         = true;
  private boolean  useCollectionSizeForEdges = true;
  // Issue #5812: off by default. The @props type hint only matters to a consumer that must rebuild the exact
  // Java type of a non-element (projection/aggregate) column - today that is the RemoteDatabase Java driver,
  // which turns this on explicitly for its own requests. A generic HTTP/JSON client (curl, Postman, another
  // language) never asked for it and should get plain JSON back.
  private boolean  includeTypeHints          = false;
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
    final String schemaDateTimeFormat = database.getSchema().getDateTimeFormat();
    final String schemaDateFormat = database.getSchema().getDateFormat();
    // THE DATE SLOT CARRIES THE DATE PATTERN, as it does in serializeResult below. It used to carry the DATE-TIME
    // one, so the same column rendered differently through GET /document/{db}/{rid} than through a query - a
    // genuine DATE reaching this dispatch picked up a spurious 00:00:00 (issue #7638). What makes the change safe
    // is the loop below now routing EVERY value through formatTemporalForPrecision, declared property or not, so a
    // java.util.Date that is not on a DATE column is formatted there with the date-time pattern and never reaches
    // this slot at all - the distinction #7610 established, which the unconditional date-time slot was standing in
    // for here.
    final JSONObject object = new JSONObject().setDateFormat(schemaDateFormat)
        .setDateTimeFormat(schemaDateTimeFormat);

    if (document.getIdentity() != null)
      object.put(RID_PROPERTY, document.getIdentity().toString());
    object.put(TYPE_PROPERTY, document.getTypeName());

    final DocumentType type = document.getType();
    final Map<String, Object> documentAsMap = document.toMap();
    for (final Map.Entry<String, Object> documentEntry : documentAsMap.entrySet()) {
      final String p = documentEntry.getKey();
      // Issue #7778: same dispatch as serializeResult(), so both paths render a property identically
      Object value = serializeObject(database, documentEntry.getValue());

      // Issue #4149: format temporals with the column's declared precision so DATETIME_MICROS /
      // DATETIME_NANOS / DATETIME_SECOND don't all collapse onto the schema-wide format string. Called for an
      // undeclared property too (with a null type, exactly as serializeResult does), so a java.util.Date always
      // gets the explicit DATE-vs-DATETIME decision rather than JSONObject's class-only dispatch (issue #7638).
      //
      // THE UNDECLARED CASE IS A DELIBERATE WIDENING, not a side effect. A java.util.Date on a schemaless property
      // used to skip this call and reach JSONObject's Date branch, which renders in the JVM's DEFAULT zone; it is
      // now formatted UTC-anchored like every other Date in the engine. That is what serializeResult() has done
      // for the same value since #7610, and what the write side does - Type#convertToDate anchors its
      // LocalDateTime-to-Date conversion to UTC - so the old answer was the outlier: the same document read back
      // through GET /document/{db}/{rid} and through a query disagreed, and the document one moved with the
      // machine's time zone. Pinned by Issue7638DateColumnPrecisionTest#anUndeclaredDatePropertyIsAlsoUtcAnchored.
      value = formatTemporalForPrecision(value, declaredType(type, p), schemaDateTimeFormat, schemaDateFormat);

      object.put(p, value);
    }

    setMetadata(document, object);

    return object;
  }

  public JSONObject serializeResult(final Database database, final Result result) {
    final String schemaDateTimeFormat = database.getSchema().getDateTimeFormat();
    final String schemaDateFormat = database.getSchema().getDateFormat();
    final JSONObject object = new JSONObject().setDateFormat(schemaDateFormat)
        .setDateTimeFormat(schemaDateTimeFormat);

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
          // Issue #4149: keep precision-aware formatting on the projection branch too.
          Object projValue = serializeObject(database, entry.getValue());
          // Undeclared properties go through it too (null type = the value's own precision), as in serializeDocument (#9005).
          final Type propertyType = declaredType(type, entry.getKey());
          projValue = formatTemporalForPrecision(projValue, propertyType, schemaDateTimeFormat, schemaDateFormat);
          object.put(entry.getKey(), projValue);
        }
        return object;
      }
    }

    final StringBuilder propertyTypes = includeTypeHints ? new StringBuilder() : null;

    for (final String propertyName : result.getPropertyNames()) {
      Object value = result.getProperty(propertyName);

      // Property type from the document's schema only - issue #4149: getTypeByClass(LocalDateTime)
      // collapses every datetime variant onto DATETIME (millis), so the schema lookup is the
      // only reliable source for column-precision-aware formatting.
      //
      // Issue #7638: a column-list projection (SELECT d FROM T) produces a NON-element result, so `type` is null
      // here and every property used to be formatted with no schema type at all - which is only harmless while
      // the value's Java class says what the column was. A java.util.Date does not: it backs a DATE column under
      // arcadedb.dateImplementation=java.util.Date and a DATETIME one just as well, so a genuine DATE came out
      // with a spurious 00:00:00. Result.getPropertyType() answers for exactly that case, from the type the
      // projection recorded for the column it read.
      // THE ROW IS THE ONLY SOURCE ASKED. getPropertyType() already falls back to the backing record's schema
      // when the projection has nothing to say about the name, so adding a second fallback here would undo the
      // one case the first exists for: SELECT *, <expression> AS d keeps the element AND publishes a computed
      // value under a name the element also has, and the row answers null ON PURPOSE for it. Falling back to
      // type.getProperty(d) on that null handed the computed value the COLUMN's type back - a DATE verdict that
      // strips the time component off a computed timestamp, and a wrong @props hint (issue #7638).
      final Type schemaPropertyType = result.getPropertyType(propertyName);

      if (includeTypeHints) {
        final Type propertyType;
        if (schemaPropertyType != null)
          propertyType = schemaPropertyType;
        else if (value != null)
          propertyType = Type.getTypeByClass(value.getClass());
        else
          propertyType = null;

        // Issue #10899: a non-finite DOUBLE is NOT faithful - it goes out as a NonFiniteNumbers marker string, which a
        // client with no hint for the column would hand back as that String instead of the number.
        // Issue #10905: an ELEMENT row takes its types from the schema on the client, so it carries a hint only for a
        // property the schema does not declare AND whose value (or collection elements) is non-finite: nothing else
        // tells the client the marker string stands for a number. Every other element row keeps its shape.
        final boolean hintElementRow = type != null && schemaPropertyType == null && carriesNonFinite(value);
        final boolean hint = propertyType != null && (type == null ?
            (!JSON_ROUND_TRIP_FAITHFUL_TYPES.contains(propertyType) || isNonFiniteDouble(value)) :
            hintElementRow);
        if (hint) {
          if (!propertyTypes.isEmpty())
            propertyTypes.append(",");
          propertyTypes.append(propertyName).append(":").append(propertyType.getId());

          // Issue #4849: a LIST/MAP column only carries a container hint, so temporal items would reach
          // the remote client as raw epoch-millis Numbers with no metadata to coerce them back to the
          // configured Java temporal type (the client returned List<Long>/Map<String,Long> instead of
          // List<LocalDateTime>/Map<String,LocalDateTime>). When the items share a temporal type, append
          // the element-type id in parentheses (e.g. "dates:9(6)") so the client can rebuild each item
          // as the configured temporal implementation.
          final Collection<?> elements;
          if (propertyType == Type.LIST && value instanceof Collection<?> collection)
            elements = collection;
          else if (propertyType == Type.MAP && value instanceof Map<?, ?> map)
            elements = map.values();
          else
            elements = null;
          if (elements != null) {
            Type elementType = detectHomogeneousTemporalElementType(elements);
            if (elementType == null)
              elementType = detectNonFiniteFloatingElementType(elements);
            if (elementType != null)
              propertyTypes.append('(').append(elementType.getId()).append(')');
          }
        }
      }

      value = serializeObject(database, value);

      // Issue #4149: format temporals with the column's declared precision so DATETIME_MICROS /
      // DATETIME_NANOS / DATETIME_SECOND keep their fractional digits even when the schema-wide
      // dateTimeFormat would truncate them. Falls back to data-driven precision (the value's
      // nanos field) when no schema type is available, e.g. for SELECT projection results.
      value = formatTemporalForPrecision(value, schemaPropertyType, schemaDateTimeFormat, schemaDateFormat);

      object.put(propertyName, value);
    }

    // Issue #4267: emit per-column type hints for synthetic (non-element) result rows so HTTP
    // clients can restore the original Java type (e.g. count(*) is a long; JSON would otherwise
    // collapse it to Integer when it fits). Element rows already carry their type via @type and
    // the schema, so they get @props only for a property the schema does not declare whose value is non-finite
    // (issue #10905); every other element row keeps its shape.
    // Issue #5812: gated behind includeTypeHints (off by default) - see the field javadoc.
    if (propertyTypes != null && !propertyTypes.isEmpty())
      object.put(Property.PROPERTY_TYPES_PROPERTY, propertyTypes.toString());

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

    final StringBuilder propertyTypes = includeTypeHints ? new StringBuilder() : null;

    for (final Map.Entry<String, Object> entry : map.entrySet()) {
      final String propertyName = entry.getKey();

      if (includePropertiesSet != null && !includePropertiesSet.contains(propertyName))
        continue;

      Object value = entry.getValue();

      // The declaration may sit on a supertype (issue #10865): a java.util.Date for an inherited DATE fell through to
      // getTypeByClass, which answers DATETIME, and was sent as epoch milliseconds the server reads as epoch days
      Type propertyType = declaredType(type, propertyName);
      if (propertyType == null && value != null)
        propertyType = Type.getTypeByClass(value.getClass());

      // Issue #5863: @props emission is gated by includeTypeHints (off by default), decoupled from
      // includeMetadata - which keeps meaning "include @rid/@type/@cat/@in/@out" only. Unlike serializeResult
      // (which omits JSON-faithful types), this opt-in path is exhaustive by design when enabled.
      if (includeTypeHints && propertyType != null) {
        if (!propertyTypes.isEmpty())
          propertyTypes.append(",");
        propertyTypes.append(propertyName).append(":").append(propertyType.getId());
      }

      value = convertToJSONType(value, propertyType);

      if (value instanceof Number number && !Double.isFinite(number.doubleValue())) {
        LogManager.instance()
            .log(this, Level.SEVERE, """
                    Found non finite number in map with key '%s', ignore this entry in the \
                    conversion""",
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
        final TemporalFormats formats = new TemporalFormats(database);
        for (final Object o : value)
          list.put(formatNestedTemporal(serializeObject(database, o), formats));

        result = list;
      }
    }
    return result;
  }

  /** The schema patterns, read once per container and only when it holds a temporal. */
  private static final class TemporalFormats {
    private final Database database;
    private       String   dateTimeFormat;
    private       String   dateFormat;

    private TemporalFormats(final Database database) {
      this.database = database;
    }

    private String dateTimeFormat() {
      if (dateTimeFormat == null)
        dateTimeFormat = database.getSchema().getDateTimeFormat();
      return dateTimeFormat;
    }

    private String dateFormat() {
      if (dateFormat == null)
        dateFormat = database.getSchema().getDateFormat();
      return dateFormat;
    }
  }

  /**
   * Issue #9011: a temporal inside a LIST or a MAP is rendered as a string with the precision it carries, exactly as a scalar
   * column is. It used to reach {@link JSONArray} as an epoch-millis number (two instants a microsecond apart became the same
   * value) and {@link JSONObject} with the schema pattern only (the fractional seconds were cut). The client rebuilds the
   * configured temporal type from the string through the element-type hint, the same conversion it applies to a scalar.
   */
  private static Object formatNestedTemporal(final Object value, final TemporalFormats formats) {
    if (value instanceof Calendar calendar)
      return formatNestedTemporal(calendar.getTime(), formats);
    if (!(value instanceof Temporal || value instanceof Date))
      return value;
    if (value instanceof LocalDate)
      return DateUtils.format(value, formats.dateFormat());
    final Object formatted = formatTemporalForPrecision(value, null, formats.dateTimeFormat(), formats.dateFormat());
    // A whole-second value is handed back untouched by formatTemporalForPrecision, for the JSONObject that owns the schema pattern
    // to format. A container has no such owner, so it is formatted here.
    if (formatted instanceof Temporal || formatted instanceof Date) {
      final String text = DateUtils.format(formatted, formats.dateTimeFormat());
      return text != null ? text : formatted;
    }
    return formatted;
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

  /**
   * A path as its {@code {_type, nodes, relationships, length}} object rather than its toString() (issue #9913). The nodes
   * and relationships are written whole whatever {@link #setUseCollectionSizeForEdges} says: that setting keeps a
   * vertex's edge lists from being dumped, while the relationships of a path are part of the value itself.
   */
  private JSONObject serializePath(final TraversalPath path) {
    final JSONArray nodes = new JSONArray();
    for (final Vertex vertex : path.getVertices())
      nodes.put(serializeDocument(vertex));
    final JSONArray relationships = new JSONArray();
    for (final Edge edge : path.getEdges())
      relationships.put(serializeDocument(edge));

    final JSONObject json = new JSONObject();
    json.put(TraversalPath.TYPE_KEY, TraversalPath.PATH_TYPE);
    json.put(TraversalPath.NODES_KEY, nodes);
    json.put(TraversalPath.RELATIONSHIPS_KEY, relationships);
    json.put(TraversalPath.LENGTH_KEY, path.length());
    return json;
  }

  private Object serializeMap(final Database database, final Map<Object, Object> value) {
    final Object result;
    if (useCollectionSize) {
      result = value.size();
    } else {
      final JSONObject map = new JSONObject().setDateFormat(database.getSchema().getDateFormat())
          .setDateTimeFormat(database.getSchema().getDateTimeFormat());
      final TemporalFormats formats = new TemporalFormats(database);
      for (final Map.Entry<Object, Object> entry : value.entrySet())
        map.put(String.valueOf(entry.getKey()), formatNestedTemporal(serializeObject(database, entry.getValue()), formats));
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

  public boolean isIncludeTypeHints() {
    return includeTypeHints;
  }

  /**
   * Enables the per-column {@code @props} type hint on non-element (projection/aggregate) result rows -
   * see the {@link #includeTypeHints} field javadoc for who needs this and why it is off by default.
   */
  public JsonSerializer setIncludeTypeHints(final boolean includeTypeHints) {
    this.includeTypeHints = includeTypeHints;
    return this;
  }

  /**
   * The {@code facets} member of a query response: {@link FacetResults#toJSON} with every value of the data (a term, a bound, a
   * cursor) written exactly as {@link #serializeResult} writes a row's value of the same property (final review C-I8): a date in the
   * schema's date format, a date-time in its date-time format at the column's precision, a non-finite number by its name (the engine
   * already gives every value the Java type a row of its property holds); a STATS facet's sum and average likewise, as values of their
   * own type. With type hints a facet carries the {@code @props} hint a row would, one entry per member whose type JSON does not
   * round-trip: {@code value:<type id>} for the values of the property, {@code sum:<type id>} and {@code avg:<type id>} for a STATS
   * facet's, so the client decodes them as it decodes rows.
   */
  public JSONObject serializeFacets(final Database database, final FacetResults facets) {
    // the engine hands the values over as a row of the property holds them: encoded here exactly as such a row's
    final JSONObject json = facets.toJSON((value, type) -> serializeValue(database, value, type));
    if (includeTypeHints)
      for (final String alias : facets.aliases()) {
        final FacetResult result = facets.get(alias);
        final StringBuilder hint = new StringBuilder();
        appendFacetHint(hint, "value", result.valueType());
        final FacetStats stats = result.stats();
        if (stats != null && !FacetResult.UNAVAILABLE.equals(result.source())) {
          // the sum and the average have the type SELECT sum() and avg() give them (a FLOAT's sum is a Float, an INTEGER's may widen)
          appendFacetHint(hint, "sum", stats.sum() != null ? Type.getTypeByClass(stats.sum().getClass()) : null);
          appendFacetHint(hint, "avg", stats.avg() != null ? Type.getTypeByClass(stats.avg().getClass()) : null);
        }
        if (!hint.isEmpty())
          json.getJSONObject(alias).put(FacetResult.VALUE_TYPE_HINT, hint.toString());
      }
    return json;
  }

  /** Adds {@code member:<type id>} to a facet's hint, as a row's hint names a column, for a type JSON does not round-trip. */
  private static void appendFacetHint(final StringBuilder hint, final String member, final Type type) {
    if (type == null || JSON_ROUND_TRIP_FAITHFUL_TYPES.contains(type))
      return;
    if (!hint.isEmpty())
      hint.append(',');
    hint.append(member).append(':').append(type.getId());
  }

  /**
   * One value as {@link #serializeResult} writes a column of {@code propertyType} (null when unknown): the JSON-ready form, a temporal
   * already formatted.
   */
  private Object serializeValue(final Database database, final Object value, final Type propertyType) {
    final Object serialized = formatTemporalForPrecision(serializeObject(database, value), propertyType,
        database.getSchema().getDateTimeFormat(), database.getSchema().getDateFormat());
    if (!(serialized instanceof Date || serialized instanceof Calendar || serialized instanceof Temporal))
      return serialized;
    // left for the date formats of the row's JSONObject: formatted by the same one here
    final JSONObject row = new JSONObject().setDateFormat(database.getSchema().getDateFormat())
        .setDateTimeFormat(database.getSchema().getDateTimeFormat()).put("v", serialized);
    return row.get("v");
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
   * The type a property is declared with on {@code type} or on any of its supertypes, {@code null} when it is not declared (or
   * there is no type). The declaration of an inherited property lives on the supertype only, so asking the record's own type
   * missed it, and the value was rendered as if the property were undeclared (issue #10865).
   */
  static Type declaredType(final DocumentType type, final String propertyName) {
    if (type == null)
      return null;
    if (type.existsProperty(propertyName))
      return type.getProperty(propertyName).getType();
    final Property property = type.getPolymorphicPropertyIfExists(propertyName);
    return property != null ? property.getType() : null;
  }

  /**
   * True for the values of the types that keep more than an instant: a time of day, a zoned datetime and a duration. A
   * {@code ZonedDateTime} under a DATETIME declaration is still an instant (issue #8572). Public so that
   * {@code Result#valueToJSON} asks the same question and the two renderings of one row cannot drift (issue #9810).
   *
   * @param value the value to classify
   * @param type  the declared property type, or {@code null} when the value comes from a projection with no declaration
   */
  public static boolean isNativeCypherTemporal(final Object value, final Type type) {
    if (value instanceof OffsetTime || value instanceof LocalTime || value instanceof CypherDuration)
      return true;
    return value instanceof ZonedDateTime && (type == null || type == Type.ZONED_DATETIME);
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
    } else if (isNativeCypherTemporal(value, type))
      // A time of day, a zoned datetime and a duration have no epoch form: the text keeps what they hold (issue #8572)
      value = value.toString();
    else if (type == Type.DATE && (value instanceof Date || value instanceof Calendar || value instanceof Temporal))
      // Issue #4601: a DATE value is encoded as epoch DAYS on the wire (the server decodes a DATE number
      // as days). A plain Date.getTime() would emit milliseconds, which the server reads as days and the
      // resulting out-of-range value is silently dropped to null.
      value = DateUtils.dateToEpochDays(value);
    else if (value instanceof Date date)
      value = date.getTime();
    else if (value instanceof Temporal)
      value = temporalToJSON(value, type);
    else if (value instanceof Map<?, ?> m) {
      // a member name is derived from the key, whatever its class (#9805)
      final JSONObject map = new JSONObject();
      for (final Map.Entry<?, ?> entry : m.entrySet())
        map.put(String.valueOf(entry.getKey()), convertToJSONType(entry.getValue(), null));
      value = map;
    }

    return value;
  }

  /**
   * A temporal of a record as JSON, in a form {@code fromJSON()} reads back as the same value (issue #10486). A number is epoch
   * MILLISECONDS for every DATETIME precision, on the way in as on the way out - that is how SQL, the HTTP API and the remote client
   * read it - so a count in the column's own unit (seconds, microseconds, nanoseconds) came back in 1970, in the year 51972 or in
   * the year 50004715. A column that milliseconds hold without losing a digit, DATETIME and DATETIME_SECOND, is therefore written
   * as milliseconds. A DATETIME_MICROS or DATETIME_NANOS column, which milliseconds would truncate, is written as the text the HTTP
   * API answers for it, with all its digits, which reading it back truncates to the column's precision. That text has no offset and is
   * read in the schema zone, so a {@code ZonedDateTime} is rendered in that zone, at the same instant.
   */
  private Object temporalToJSON(final Object value, final Type type) {
    if (type == Type.DATETIME_MICROS || type == Type.DATETIME_NANOS) {
      final String dateTimeFormat = database != null ? database.getSchema().getDateTimeFormat() :
          GlobalConfiguration.DATE_TIME_FORMAT.getValueAsString();
      final Object inSchemaZone = database != null && value instanceof ZonedDateTime zoned ?
          zoned.withZoneSameInstant(database.getSchema().getZoneId()) :
          value;
      return formatTemporalForPrecision(inSchemaZone, type, dateTimeFormat, null);
    }
    return DateUtils.dateTimeToTimestamp(value, ChronoUnit.MILLIS);
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

  /**
   * JSON has no NaN and no infinity: a non-finite {@link Double} or {@link Float} is rendered as its name. Tested by type
   * rather than through {@code value.equals(Double.NaN)} and its five siblings, which boxed a fresh {@code Double} or
   * {@code Float} for each of the six comparisons on every value serialized (issue #8624).
   */
  private static Object convertNonNumbers(final Object value) {
    if (value instanceof Double d) {
      final double v = d;
      if (!Double.isFinite(v))
        return nonFiniteName(v);
    } else if (value instanceof Float f) {
      final float v = f;
      if (!Float.isFinite(v))
        return nonFiniteName(v);
    }
    return value;
  }

  private static String nonFiniteName(final double value) {
    return Double.isNaN(value) ? NonFiniteNumbers.NAN : value > 0 ? NonFiniteNumbers.POSITIVE_INFINITY : NonFiniteNumbers.NEGATIVE_INFINITY;
  }

  private static boolean isNonFiniteDouble(final Object value) {
    return value instanceof Double d && !Double.isFinite(d);
  }

  /**
   * Issue #10905: true when {@code value} is a non-finite {@code Float}/{@code Double}, or a LIST/MAP one of whose
   * elements is - the values that go out as a {@code NonFiniteNumbers} marker string.
   */
  private static boolean carriesNonFinite(final Object value) {
    if (value instanceof Double d)
      return !Double.isFinite(d);
    if (value instanceof Float f)
      return !Float.isFinite(f);
    if (value instanceof Collection<?> collection)
      return detectNonFiniteFloatingElementType(collection) != null;
    if (value instanceof Map<?, ?> map)
      return detectNonFiniteFloatingElementType(map.values()) != null;
    return false;
  }

  /**
   * Issue #8624: a primitive array is written straight into the JSON array. A vector embedding is a {@code float[]} of a
   * thousand elements or more, serialized once per search hit, and it used to be boxed into an intermediate list first,
   * each element then going back through the whole {@link #serializeObject} dispatch and {@link #convertNonNumbers}.
   * <p>
   * The output is byte-identical to that path, which is what a {@link Collection} of the same values still takes: every
   * element becomes the same boxed type ({@code Float} stays {@code Float}, so {@code 0.1F} does not widen into
   * {@code 0.10000000149011612}), a non-finite float or double the same name, and {@code useCollectionSize} the length.
   * An array of objects has no primitive to take a shortcut on and keeps the collection path.
   */
  private Object serializeArray(final Database database, final Object array) {
    if (!array.getClass().getComponentType().isPrimitive())
      return serializeCollection(database, Arrays.asList((Object[]) array), null);

    if (useCollectionSize)
      return Array.getLength(array);

    final JSONArray json = new JSONArray(Array.getLength(array));
    switch (array) {
    case float[] floats -> {
      for (final float v : floats)
        if (Float.isFinite(v))
          json.put(v);
        else
          json.put(nonFiniteName(v));
    }
    case double[] doubles -> {
      for (final double v : doubles)
        if (Double.isFinite(v))
          json.put(v);
        else
          json.put(nonFiniteName(v));
    }
    case int[] ints -> {
      for (final int v : ints)
        json.put(v);
    }
    case long[] longs -> {
      for (final long v : longs)
        json.put(v);
    }
    case short[] shorts -> {
      for (final short v : shorts)
        json.put(v);
    }
    case byte[] bytes -> {
      for (final byte v : bytes)
        json.put(v);
    }
    case boolean[] booleans -> {
      for (final boolean v : booleans)
        json.put(v);
    }
    case char[] chars -> {
      for (final char v : chars)
        json.put(v);
    }
    default -> throw new IllegalArgumentException("Unsupported primitive array " + array.getClass().getName());
    }
    return json;
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
    else if (value instanceof TraversalPath path)
      value = serializePath(path);
    else if (value.getClass().isArray())
      value = serializeArray(database, value);

    value = convertNonNumbers(value);

    return value;
  }

  /**
   * Issue #10899: returns FLOAT (DOUBLE) when every non-null element of {@code collection} is a {@link Float}
   * ({@link Double}) and at least one of them is non-finite, or {@code null} otherwise. Such an element goes out as a
   * {@code NonFiniteNumbers} marker string, and without the element-type hint the remote client would hand that string
   * back instead of the number. A collection of finite numbers is left alone: JSON already carries it faithfully enough
   * for the client, as it did before.
   * <p>
   * Issue #10905: a collection that mixes {@code Float} and {@code Double} (and integers a double holds exactly)
   * reports DOUBLE: the hint is one type for the whole collection, and widening each of those to a double loses
   * nothing. A collection holding any other kind of element (a {@code Long} a double cannot hold exactly, a
   * {@code BigDecimal}, a string, ...) has no single type to name and still reports {@code null}.
   */
  private static Type detectNonFiniteFloatingElementType(final Collection<?> collection) {
    boolean floats = false;
    boolean doubles = false;
    boolean others = false;
    boolean nonFinite = false;
    for (final Object element : collection) {
      if (element == null)
        continue;
      if (element instanceof Float f) {
        floats = true;
        nonFinite |= !Float.isFinite(f);
      } else if (element instanceof Double d) {
        doubles = true;
        nonFinite |= !Double.isFinite(d);
      } else if (element instanceof Integer || element instanceof Short || element instanceof Byte)
        others = true;
      // (double) Long.MAX_VALUE is 2^63, which the cast back saturates to Long.MAX_VALUE: the round trip only looks exact
      else if (element instanceof Long l && l != Long.MAX_VALUE && (long) (double) l == l)
        others = true;
      else
        return null;
    }
    if (!nonFinite)
      return null;
    if (floats && !doubles && !others)
      return Type.FLOAT;
    return Type.DOUBLE;
  }

  /**
   * Issue #4849: returns the temporal {@link Type} shared by every non-null element of {@code
   * collection} (the items of a LIST column or the values of a MAP column), or {@code null} when the
   * collection is empty, contains a non-temporal element, or mixes temporal types. Only the {@code
   * DATETIME} family is reported, plus {@code DATE} for {@link LocalDate} items (issue #10909).
   * {@link Type#getTypeByClass(Class)} collapses every datetime variant onto {@code DATETIME}, which matches
   * the millisecond encoding that {@link com.arcadedb.serializer.json.JSONObject#objectToElement} uses
   * for array items, so the remote client can safely rebuild each item from the epoch-millis Number.
   */
  private static Type detectHomogeneousTemporalElementType(final Collection<?> collection) {
    Type elementType = null;
    for (final Object element : collection) {
      if (element == null)
        continue;
      if (!(element instanceof Temporal || element instanceof Date || element instanceof Calendar))
        return null;
      final Type t = Type.getTypeByClass(element.getClass());
      // Issue #10909: a LocalDate item is a DATE, and goes out as its text, so it needs its hint as much as a DATETIME one
      if (t != Type.DATETIME && !(t == Type.DATE && element instanceof LocalDate))
        return null;
      if (elementType == null)
        elementType = t;
      else if (elementType != t)
        return null;
    }
    return elementType;
  }

  /**
   * Issue #4149: pre-format a temporal value with a format string wide enough for the column's
   * declared precision. Without this, every datetime variant collapses onto whatever fractional
   * digits the schema-wide {@code arcadedb.dateTimeFormat} provides, dropping micro/nano data
   * that storage already preserved correctly.
   * <p>
   * Resolution order for the precision:
   * <ol>
   *   <li>if {@code propertyType} is one of the {@code DATETIME_*} variants, use its declared
   *   precision (so {@code SELECT *} on a typed document gets exact column precision, with
   *   trailing zeros when the data is less precise than the column);</li>
   *   <li>otherwise (projection, computed expression, missing schema type), derive the precision
   *   from the value's nanoseconds field. Storage already truncated to the column's precision
   *   when reading from disk, so the nanos field reliably reports the kept precision.</li>
   * </ol>
   * Returns {@code value} unchanged when it is not a temporal we can handle, so {@link
   * JSONObject#put(String, Object)}'s existing temporal branch covers any leftover cases.
   * <p>
   * Issue #7610: a {@code java.util.Date} backs both a genuine DATE column and any computed or
   * aggregate expression that yields an instant (e.g. {@code ts.timeBucket()}), or a DATETIME column
   * configured with {@code arcadedb.dateTimeImplementation} = java.util.Date. {@link
   * JSONObject#put(String, Object)} dispatches on Java class alone and can't tell those apart, so it
   * would format every one of them with the schema's date-only pattern and silently drop the time of
   * day. Only a value actually backed by a DATE-typed property is left for that dispatch (unchanged
   * below); everything else is formatted here with the full date-time pattern, anchored to UTC to
   * match {@code Type#convertToDate}'s own UTC-anchored LocalDateTime-to-Date conversion (the write
   * side of the same round trip), and precision-expanded the same way the {@code Temporal} branch
   * below is, so a {@code Date} with a non-zero millisecond component doesn't get silently truncated
   * by a seconds-only schema pattern. {@code Date} can never hold finer than millisecond precision, so
   * the data-driven fallback below caps at {@link ChronoUnit#MILLIS}, not the nanos field.
   * <p>
   * Issue #7638: a value on a DATE-typed column is now formatted HERE, with {@code baseDateFormat} anchored to
   * UTC, rather than left for {@link JSONObject#put(String, Object)} to dispatch on. Deferring was wrong twice
   * over, and both were invisible while {@code arcadedb.dateImplementation=java.util.Date} was broken on the
   * write side (see {@code BinaryTypes.getTypeFromValue}) and no DATE column could hold a {@code Date} at all:
   * <ul>
   *   <li>{@code JSONObject}'s {@code Date} branch renders in the JVM's DEFAULT zone, while a DATE is stored as
   *   a count of days and read back as UTC midnight - so west of Greenwich every date came out one day
   *   early;</li>
   *   <li>the slot that dispatch reads is the caller's, and {@code serializeDocument} sets it to the DATE-TIME
   *   pattern while {@code serializeResult} sets it to the date one, so the same column rendered differently
   *   through {@code GET /document/{db}/{rid}} than through a query. Formatting here makes the two agree by
   *   construction rather than by both remembering to set the same slot.</li>
   * </ul>
   */
  private static Object formatTemporalForPrecision(final Object value, final Type propertyType,
      final String baseDateTimeFormat, final String baseDateFormat) {
    if (isNativeCypherTemporal(value, propertyType))
      return value.toString();
    if (propertyType == Type.DATE && (value instanceof Date || value instanceof Calendar || value instanceof Temporal))
      // UTC, because that is the anchor the whole DATE round trip uses: DateUtils.date() materialises a stored day
      // count as UTC midnight and Type#convertToDate converts back the same way
      return DateUtils.format(value, baseDateFormat, "UTC");

    if (value instanceof Date date) {
      final ChronoUnit precision = propertyType != null
          ? DateUtils.getPrecisionFromType(propertyType)
          : DateUtils.getPrecision((int) (Math.floorMod(date.getTime(), 1000L) * 1_000_000L));
      return DateUtils.format(date, expandFormatToPrecision(baseDateTimeFormat, precision), "UTC");
    }
    if (!(value instanceof Temporal))
      return value;
    final ChronoUnit precision;
    if (propertyType != null && (propertyType == Type.DATETIME_SECOND || propertyType == Type.DATETIME
        || propertyType == Type.DATETIME_MICROS || propertyType == Type.DATETIME_NANOS)) {
      precision = DateUtils.getPrecisionFromType(propertyType);
    } else {
      try {
        precision = DateUtils.getPrecision(DateUtils.getNanos(value));
      } catch (final IllegalArgumentException e) {
        return value;
      }
    }
    final String format = expandFormatToPrecision(baseDateTimeFormat, precision);
    if (format.equals(baseDateTimeFormat) && precision == ChronoUnit.SECONDS)
      // No expansion needed and no fractional digits to render: let JSONObject's existing
      // temporal path apply the schema-wide format. Avoids unnecessary string allocation.
      return value;
    return DateUtils.format(value, format);
  }

  /**
   * Returns a format string with at least the number of fractional second digits required by
   * {@code precision}. Existing trailing {@code S} digits in {@code baseFormat} are honored;
   * missing digits are appended (with a {@code .} separator inserted when no fractional segment
   * existed yet). Issue #4149.
   */
  private static String expandFormatToPrecision(final String baseFormat, final ChronoUnit precision) {
    final int needed;
    if (precision == ChronoUnit.SECONDS)
      needed = 0;
    else if (precision == ChronoUnit.MILLIS)
      needed = 3;
    else if (precision == ChronoUnit.MICROS)
      needed = 6;
    else if (precision == ChronoUnit.NANOS)
      needed = 9;
    else
      needed = 0;
    if (needed == 0)
      return baseFormat;

    int existing = 0;
    int i = baseFormat.length() - 1;
    while (i >= 0 && baseFormat.charAt(i) == 'S') {
      existing++;
      i--;
    }
    if (existing >= needed)
      return baseFormat;

    final StringBuilder b = new StringBuilder(baseFormat.length() + (needed - existing) + 1);
    b.append(baseFormat);
    if (existing == 0)
      b.append('.');
    for (int j = existing; j < needed; j++)
      b.append('S');
    return b.toString();
  }

}
