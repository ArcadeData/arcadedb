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
package com.arcadedb.function.convert;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.query.opencypher.temporal.TemporalUtil;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * convert.toJson(value) - Convert a value to its JSON text, in the layout of APOC's {@code apoc.convert.toJson} (issue #12108).
 * <p>
 * The answer is always a JSON document: a string is quoted and escaped, a temporal is the quoted ISO-8601 text, a non-finite
 * float is the quoted {@code "NaN"} / {@code "Infinity"}, a node is {@code {"id", "type": "node", "labels", "properties"}}, a
 * relationship is {@code {"id", "type": "relationship", "label", "start", "end", "properties"}} and a path is the list of its
 * nodes and relationships. {@code labels} and {@code properties} are left out when empty, as APOC does. The {@code id} of a
 * node or relationship is its RID as a string ({@code "#1:0"}), where Neo4j writes its internal numeric id.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class ConvertToJson extends AbstractConvertFunction {
  private static final char[] HEX_CHARS = "0123456789ABCDEF".toCharArray();

  @Override
  protected String getSimpleName() {
    return "toJson";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 1;
  }

  @Override
  public String getDescription() {
    return "Convert value to JSON string representation";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    final StringBuilder json = new StringBuilder();
    write(args[0], json);
    return json.toString();
  }

  private static void write(final Object value, final StringBuilder json) {
    switch (value) {
    case null -> json.append("null");
    case String string -> writeString(string, json);
    case Boolean bool -> json.append(bool);
    // JSON has no literal for NaN and the infinities: APOC writes them as strings
    case Double d when !Double.isFinite(d) -> writeString(d.toString(), json);
    case Float f when !Float.isFinite(f) -> writeString(f.toString(), json);
    case Number number -> json.append(number);
    case Vertex vertex -> writeNode(vertex, false, json);
    case Edge edge -> writeRelationship(edge, false, json);
    case TraversalPath path -> writePath(path, json);
    case Map<?, ?> map -> writeMap(map, json);
    // A document that is not a graph element (an embedded document, a record of a document type) is its properties
    case Document document -> writeProperties(document, json);
    case Identifiable identifiable -> writeString(identifiable.getIdentity().toString(), json);
    // A BINARY value (what util.compress answers) is the array of its signed bytes, as in APOC
    case byte[] bytes -> {
      json.append('[');
      for (int i = 0; i < bytes.length; i++) {
        if (i > 0)
          json.append(',');
        json.append(bytes[i]);
      }
      json.append(']');
    }
    case Iterable<?> iterable -> writeArray(iterable, json);
    // A list stored as ARRAY_OF_LONGS / ARRAY_OF_DOUBLES is a primitive array: serialized as the JSON array it is, not as the
    // Java identity string "[J@..." (issue #11582)
    case Object array when MultiValue.isSequenceArray(array) -> writeArray(MultiValue.getMultiValueAsList(array), json);
    // Temporals (a native java.time / java.util.Date value is read as its Cypher type first) and anything else: quoted text
    default -> writeString(TemporalUtil.convertFromStorage(value).toString(), json);
    }
  }

  private static void writeArray(final Iterable<?> values, final StringBuilder json) {
    json.append('[');
    boolean first = true;
    for (final Object element : values) {
      if (!first)
        json.append(',');
      first = false;
      write(element, json);
    }
    json.append(']');
  }

  private static void writeMap(final Map<?, ?> map, final StringBuilder json) {
    json.append('{');
    boolean first = true;
    for (final Map.Entry<?, ?> entry : map.entrySet()) {
      if (!first)
        json.append(',');
      first = false;
      writeString(String.valueOf(entry.getKey()), json);
      json.append(':');
      write(entry.getValue(), json);
    }
    json.append('}');
  }

  private static void writeProperties(final Document document, final StringBuilder json) {
    json.append('{');
    boolean first = true;
    for (final String name : document.getPropertyNames()) {
      if (!first)
        json.append(',');
      first = false;
      writeString(name, json);
      json.append(':');
      write(TemporalUtil.convertFromStorage(document, name), json);
    }
    json.append('}');
  }

  /**
   * APOC writes a node standing alone (or in a map or list) as {@code id, type, labels, properties} and a node inside a path
   * as {@code id, type, properties, labels}: the key order of each is kept so the text is the same as APOC's.
   */
  private static void writeNode(final Vertex vertex, final boolean inPath, final StringBuilder json) {
    json.append("{\"id\":");
    writeString(vertex.getIdentity().toString(), json);
    json.append(",\"type\":\"node\"");
    if (inPath) {
      writeNodeProperties(vertex, json);
      writeLabels(vertex, json);
    } else {
      writeLabels(vertex, json);
      writeNodeProperties(vertex, json);
    }
    json.append('}');
  }

  private static void writeLabels(final Vertex vertex, final StringBuilder json) {
    final List<String> labels = Labels.getLabels(vertex);
    if (labels.isEmpty())
      return;
    json.append(",\"labels\":");
    writeArray(labels, json);
  }

  private static void writeNodeProperties(final Document document, final StringBuilder json) {
    final Set<String> names = document.getPropertyNames();
    if (names.isEmpty())
      return;
    json.append(",\"properties\":");
    writeProperties(document, json);
  }

  /**
   * A relationship standing alone is {@code id, type, label, start, end, properties}; inside a path it is
   * {@code start, end, id, label, type, properties}, each end node in the matching layout.
   */
  private static void writeRelationship(final Edge edge, final boolean inPath, final StringBuilder json) {
    if (inPath) {
      json.append('{');
      writeEnds(edge, true, json);
      json.append(",\"id\":");
      writeString(edge.getIdentity().toString(), json);
      json.append(",\"label\":");
      writeString(edge.getTypeName(), json);
      json.append(",\"type\":\"relationship\"");
    } else {
      json.append("{\"id\":");
      writeString(edge.getIdentity().toString(), json);
      json.append(",\"type\":\"relationship\",\"label\":");
      writeString(edge.getTypeName(), json);
      json.append(',');
      writeEnds(edge, false, json);
    }
    writeNodeProperties(edge, json);
    json.append('}');
  }

  private static void writeEnds(final Edge edge, final boolean inPath, final StringBuilder json) {
    json.append("\"start\":");
    writeNode(edge.getOutVertex(), inPath, json);
    json.append(",\"end\":");
    writeNode(edge.getInVertex(), inPath, json);
  }

  private static void writePath(final TraversalPath path, final StringBuilder json) {
    json.append('[');
    boolean first = true;
    for (final Object element : path.toAlternatingList()) {
      if (!first)
        json.append(',');
      first = false;
      if (element instanceof Edge edge)
        writeRelationship(edge, true, json);
      else
        writeNode((Vertex) element, true, json);
    }
    json.append(']');
  }

  /**
   * Quotes a string as APOC's writer does: the quote, the backslash and the control characters are escaped (the short forms
   * where JSON has one, else the six-character unicode escape with upper-case hex digits), every other character is written as
   * it is.
   */
  private static void writeString(final String text, final StringBuilder json) {
    json.append('"');
    for (int i = 0; i < text.length(); i++) {
      final char c = text.charAt(i);
      switch (c) {
      case '"' -> json.append("\\\"");
      case '\\' -> json.append("\\\\");
      case '\b' -> json.append("\\b");
      case '\f' -> json.append("\\f");
      case '\n' -> json.append("\\n");
      case '\r' -> json.append("\\r");
      case '\t' -> json.append("\\t");
      default -> {
        if (c < 0x20)
          json.append("\\u00").append(HEX_CHARS[c >> 4]).append(HEX_CHARS[c & 0xF]);
        else
          json.append(c);
      }
      }
    }
    json.append('"');
  }
}
