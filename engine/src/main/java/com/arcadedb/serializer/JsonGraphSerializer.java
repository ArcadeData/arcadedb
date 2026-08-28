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

import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.DateUtils;

import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class JsonGraphSerializer extends JsonSerializer {

  private boolean    expandVertexEdges       = false;
  private JSONObject sharedJson              = null;
  private boolean    includeMetadata         = true;
  private boolean    precisionAwareTemporals = false;

  private JsonGraphSerializer() {
  }

  public static JsonGraphSerializer createJsonGraphSerializer() {
    return new JsonGraphSerializer();
  }

  public JSONObject serializeGraphElement(final Document document) {
    if (sharedJson != null)
      return serializeGraphElement(document, sharedJson);
    return serializeGraphElement(document, new JSONObject());
  }

  public JSONObject serializeGraphElement(final Document document, final JSONObject object) {
    final JSONObject properties;

    if (object.has("p")) {
      // REUSE PROPERTY OBJECT
      properties = object.getJSONObject("p");
      properties.clear();
    } else
      properties = new JSONObject();

    object.clear();
    object.put("p", properties);

    final RID rid = document.getIdentity();
    if (rid != null)
      object.put("r", rid.toString());
    object.put("t", document.getTypeName());

    final DocumentType type = precisionAwareTemporals ? document.getType() : null;

    for (final Map.Entry<String, Object> prop : document.toMap(includeMetadata).entrySet()) {
      Object value = prop.getValue();

      if (value != null) {
        if (value instanceof Document document1)
          value = serializeGraphElement(document1, new JSONObject());
        else if (value instanceof Collection collection) {
          final List<Object> list = new ArrayList<>();
          for (Object o : collection) {
            if (o instanceof Document document1)
              o = serializeGraphElement(document1, new JSONObject());
            list.add(o);
          }
          value = list;
        } else if (value.equals(Double.NaN) || value.equals(Float.NaN))
          // JSON DOES NOT SUPPORT NaN
          value = "NaN";
        else if (value.equals(Double.POSITIVE_INFINITY) || value.equals(Float.POSITIVE_INFINITY))
          // JSON DOES NOT SUPPORT INFINITY
          value = "PosInfinity";
        else if (value.equals(Double.NEGATIVE_INFINITY) || value.equals(Float.NEGATIVE_INFINITY))
          // JSON DOES NOT SUPPORT INFINITY
          value = "NegInfinity";
        else if (type != null && isEncodableTemporal(value) && type.existsProperty(prop.getKey()))
          value = encodeTemporalForWriteBack(value, type.getProperty(prop.getKey()).getType());
      }
      properties.put(prop.getKey(), value);
    }

    setMetadata(document, object);

    return object;
  }

  private void setMetadata(final Document document, final JSONObject object) {
    if (document instanceof Vertex vertex1) {
      final Vertex vertex = vertex1;

      if (expandVertexEdges) {
        final JSONArray outEdges = new JSONArray();
        for (final Edge e : vertex.getEdges(Vertex.DIRECTION.OUT))
          outEdges.put(e.getIdentity().toString());
        object.put("o", outEdges);

        final JSONArray inEdges = new JSONArray();
        for (final Edge e : vertex.getEdges(Vertex.DIRECTION.IN))
          inEdges.put(e.getIdentity().toString());
        object.put("i", inEdges);
      } else {
        object.put("i", vertex.countEdges(Vertex.DIRECTION.IN));
        object.put("o", vertex.countEdges(Vertex.DIRECTION.OUT));
      }

    } else if (document instanceof Edge edge1) {
      final Edge edge = edge1;
      object.put("i", edge.getIn().toString());
      object.put("o", edge.getOut().toString());
    }
  }

  public boolean isExpandVertexEdges() {
    return expandVertexEdges;
  }

  public JsonGraphSerializer setExpandVertexEdges(final boolean expandVertexEdges) {
    this.expandVertexEdges = expandVertexEdges;
    return this;
  }

  public JsonGraphSerializer setSharedJson(final JSONObject json) {
    sharedJson = json;
    return this;
  }

  public JsonGraphSerializer setIncludeMetadata(final boolean includeMetadata) {
    this.includeMetadata = includeMetadata;
    return this;
  }

  /**
   * Issue #6455: when enabled, a schema-typed DATE or DATETIME_MICROS/DATETIME_NANOS property is
   * pre-converted to the encoding a schema-typed write-back path (such as {@code MutableDocument.fromMap})
   * actually decodes, instead of the epoch-millisecond number {@link JSONObject#put(String, Object)}'s
   * default temporal branch writes for every temporal:
   * <ul>
   *   <li>DATE becomes epoch DAYS ({@link DateUtils#dateToEpochDays}) - the encoding {@code Type.convert}'s
   *   {@code Number} branch reads back for a DATE property, matching the remote-wire fix of issue #4601.
   *   A DATE written as epoch millis is read back as epoch days: any date from ~August 1981 onward
   *   overflows {@code LocalDate.ofEpochDay}'s range, and the resulting exception is swallowed by
   *   {@code Type.convert}'s broad catch, silently nulling the property.</li>
   *   <li>DATETIME_MICROS/DATETIME_NANOS become an ISO-8601 string, because a raw epoch number is always
   *   decoded as MILLIS on the way back in, regardless of the column's declared precision (the {@code
   *   Number} branch of {@code Type.convert} hardcodes it) - only a parsed string carries enough digits
   *   to restore the sub-millisecond component.</li>
   * </ul>
   * Off by default: existing callers - the HTTP graph-mode query response in particular, which does not
   * write these values back through the schema-typed path this class assumes - keep writing every
   * temporal as an epoch-millis number. {@code JsonlExporterFormat} opts in because its counterpart
   * importer feeds the JSON straight back through {@code MutableDocument.fromMap}.
   */
  public JsonGraphSerializer setPrecisionAwareTemporals(final boolean precisionAwareTemporals) {
    this.precisionAwareTemporals = precisionAwareTemporals;
    return this;
  }

  /**
   * Issue #6795 (follow-up on #6455): {@code arcadedb.dateImplementation=java.util.Date} (or {@code Calendar})
   * makes the binary serializer hand back a DATE property as a {@link Date}/{@link Calendar} rather than a
   * {@link Temporal}, so the write-back encoding must recognize those too - {@link DateUtils#dateToEpochDays}
   * already handles both.
   */
  private static boolean isEncodableTemporal(final Object value) {
    return value instanceof Temporal || value instanceof Date || value instanceof Calendar;
  }

  private static Object encodeTemporalForWriteBack(final Object value, final Type propertyType) {
    if (propertyType == Type.DATE)
      return DateUtils.dateToEpochDays(value);
    if (propertyType == Type.DATETIME_MICROS || propertyType == Type.DATETIME_NANOS)
      return value.toString();
    return value;
  }
}
