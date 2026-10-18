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
package com.arcadedb.gremlin.io.graphml;

import com.arcadedb.integration.misc.ValueTypeHints;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.GraphWriter;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Writes a graph as standard GraphML, that any GraphML reader takes, keeping the type of every property value (issue #10202).
 * <p>
 * TinkerPop's writer declares each key with the type of the first value it finds and writes every value with
 * {@code toString()}: a {@code byte[]} became its Java identity ({@code [B@...}), a BigDecimal, a date, a list or a map a string,
 * and a key whose values had two types made the whole file unreadable. Here a key is declared with a GraphML type only when
 * every value of it has that type, and a value GraphML has no type for is written as text with its ArcadeDB type in an
 * {@code arcadedb:type} attribute of its {@code <data>} element - an attribute of another namespace, which the GraphML schema
 * allows and other readers ignore, reading the text. {@link ArcadeGraphMLReader} reads the value back through it.
 * <p>
 * The element and label layout is TinkerPop's ({@code labelV}, {@code labelE}), so its reader takes the file too. A link is
 * written as the text of its RID: no GraphML reader re-points it at the vertex the import creates.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ArcadeGraphMLWriter implements GraphWriter {
  public static final String ARCADEDB_NAMESPACE = "https://arcadedb.com/xmlns/graphml";
  public static final String ARCADEDB_PREFIX    = "arcadedb";
  public static final String TYPE_ATTRIBUTE     = "type";

  @Override
  public void writeGraph(final OutputStream outputStream, final Graph graph) throws IOException {
    // TWO PASSES, AS TINKERPOP'S WRITER: THE KEYS ARE DECLARED BEFORE THE GRAPH, WITH THE TYPE OF EVERY VALUE THEY TAKE
    final Map<String, String> vertexKeys = new LinkedHashMap<>();
    for (final Iterator<Vertex> it = graph.vertices(); it.hasNext(); )
      collectKeys(it.next(), vertexKeys);
    final Map<String, String> edgeKeys = new LinkedHashMap<>();
    for (final Iterator<Edge> it = graph.edges(); it.hasNext(); )
      collectKeys(it.next(), edgeKeys);

    final Map<String, String> vertexKeyIds = keyIds(vertexKeys, edgeKeys, GraphMLConstants.VERTEX_SUFFIX);
    final Map<String, String> edgeKeyIds = keyIds(edgeKeys, vertexKeys, GraphMLConstants.EDGE_SUFFIX);

    try {
      final XMLStreamWriter writer = XMLOutputFactory.newInstance().createXMLStreamWriter(outputStream, StandardCharsets.UTF_8.name());
      writer.writeStartDocument(StandardCharsets.UTF_8.name(), "1.0");
      writer.writeStartElement(GraphMLConstants.GRAPHML);
      writer.writeDefaultNamespace(GraphMLConstants.GRAPHML_XMLNS);
      writer.writeNamespace(GraphMLConstants.XML_SCHEMA_NAMESPACE_TAG, "http://www.w3.org/2001/XMLSchema-instance");
      writer.writeNamespace(ARCADEDB_PREFIX, ARCADEDB_NAMESPACE);
      writer.writeAttribute("http://www.w3.org/2001/XMLSchema-instance", GraphMLConstants.XML_SCHEMA_LOCATION_ATTRIBUTE,
          GraphMLConstants.GRAPHML_XMLNS + " " + GraphMLConstants.DEFAULT_GRAPHML_SCHEMA_LOCATION);

      writeKey(writer, GraphMLConstants.LABEL_V, GraphMLConstants.NODE, GraphMLConstants.LABEL_V, GraphMLConstants.STRING);
      for (final Map.Entry<String, String> key : vertexKeys.entrySet())
        writeKey(writer, vertexKeyIds.get(key.getKey()), GraphMLConstants.NODE, key.getKey(), key.getValue());
      writeKey(writer, GraphMLConstants.LABEL_E, GraphMLConstants.EDGE, GraphMLConstants.LABEL_E, GraphMLConstants.STRING);
      for (final Map.Entry<String, String> key : edgeKeys.entrySet())
        writeKey(writer, edgeKeyIds.get(key.getKey()), GraphMLConstants.EDGE, key.getKey(), key.getValue());

      writer.writeStartElement(GraphMLConstants.GRAPH);
      writer.writeAttribute(GraphMLConstants.ID, GraphMLConstants.G);
      writer.writeAttribute(GraphMLConstants.EDGEDEFAULT, GraphMLConstants.DIRECTED);

      for (final Iterator<Vertex> it = graph.vertices(); it.hasNext(); ) {
        final Vertex vertex = it.next();
        writer.writeStartElement(GraphMLConstants.NODE);
        writer.writeAttribute(GraphMLConstants.ID, vertex.id().toString());
        writeData(writer, GraphMLConstants.LABEL_V, vertex.label(), GraphMLConstants.STRING);
        for (final Iterator<VertexProperty<Object>> props = vertex.properties(); props.hasNext(); ) {
          final VertexProperty<Object> property = props.next();
          writeData(writer, vertexKeyIds.get(property.key()), property.value(), vertexKeys.get(property.key()));
        }
        writer.writeEndElement();
      }

      for (final Iterator<Edge> it = graph.edges(); it.hasNext(); ) {
        final Edge edge = it.next();
        writer.writeStartElement(GraphMLConstants.EDGE);
        writer.writeAttribute(GraphMLConstants.ID, edge.id().toString());
        writer.writeAttribute(GraphMLConstants.SOURCE, edge.vertices(Direction.OUT).next().id().toString());
        writer.writeAttribute(GraphMLConstants.TARGET, edge.vertices(Direction.IN).next().id().toString());
        writeData(writer, GraphMLConstants.LABEL_E, edge.label(), GraphMLConstants.STRING);
        for (final Iterator<Property<Object>> props = edge.properties(); props.hasNext(); ) {
          final Property<Object> property = props.next();
          writeData(writer, edgeKeyIds.get(property.key()), property.value(), edgeKeys.get(property.key()));
        }
        writer.writeEndElement();
      }

      writer.writeEndElement(); // graph
      writer.writeEndElement(); // graphml
      writer.writeEndDocument();
      writer.flush();
      writer.close();
    } catch (final XMLStreamException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void writeVertex(final OutputStream outputStream, final Vertex v, final Direction direction) {
    throw new UnsupportedOperationException("GraphML does not allow for a partial structure");
  }

  @Override
  public void writeVertex(final OutputStream outputStream, final Vertex v) {
    throw new UnsupportedOperationException("GraphML does not allow for a partial structure");
  }

  @Override
  public void writeEdge(final OutputStream outputStream, final Edge e) {
    throw new UnsupportedOperationException("GraphML does not allow for a partial structure");
  }

  @Override
  public void writeVertices(final OutputStream outputStream, final Iterator<Vertex> vertexIterator, final Direction direction) {
    throw new UnsupportedOperationException("GraphML does not allow for a partial structure");
  }

  @Override
  public void writeVertices(final OutputStream outputStream, final Iterator<Vertex> vertexIterator) {
    throw new UnsupportedOperationException("GraphML does not allow for a partial structure");
  }

  @Override
  public void writeVertexProperty(final OutputStream outputStream, final VertexProperty vp) {
    throw new UnsupportedOperationException("GraphML does not allow for a partial structure");
  }

  @Override
  public void writeProperty(final OutputStream outputStream, final Property p) {
    throw new UnsupportedOperationException("GraphML does not allow for a partial structure");
  }

  @Override
  public void writeObject(final OutputStream outputStream, final Object object) {
    throw new UnsupportedOperationException("GraphML does not allow for a partial structure");
  }

  /** The GraphML type of each key: the type all its values share, or string when they do not share one. */
  private static void collectKeys(final Element element, final Map<String, String> keys) {
    for (final Iterator<? extends Property<Object>> props = element.properties(); props.hasNext(); ) {
      final Property<Object> property = props.next();
      final String type = graphMLType(property.value());
      keys.merge(property.key(), type, (previous, current) -> previous.equals(current) ? previous : GraphMLConstants.STRING);
    }
  }

  /** Unique key ids: a name both vertices and edges use gets the suffix of its element, as TinkerPop's writer does. */
  private static Map<String, String> keyIds(final Map<String, String> keys, final Map<String, String> otherKeys, final String suffix) {
    final Map<String, String> ids = new LinkedHashMap<>();
    for (final String name : keys.keySet())
      ids.put(name, otherKeys.containsKey(name) || name.equals(GraphMLConstants.LABEL_V) || name.equals(GraphMLConstants.LABEL_E) ?
          name + suffix :
          name);
    return ids;
  }

  /** The GraphML type of a value, string for every value GraphML has no type for. */
  private static String graphMLType(final Object value) {
    return switch (value) {
      case Boolean b -> GraphMLConstants.BOOLEAN;
      case Integer i -> GraphMLConstants.INT;
      case Long l -> GraphMLConstants.LONG;
      case Float f -> GraphMLConstants.FLOAT;
      case Double d -> GraphMLConstants.DOUBLE;
      case null, default -> GraphMLConstants.STRING;
    };
  }

  private static void writeKey(final XMLStreamWriter writer, final String id, final String forElement, final String name,
      final String type) throws XMLStreamException {
    writer.writeStartElement(GraphMLConstants.KEY);
    writer.writeAttribute(GraphMLConstants.ID, id);
    writer.writeAttribute(GraphMLConstants.FOR, forElement);
    writer.writeAttribute(GraphMLConstants.ATTR_NAME, name);
    writer.writeAttribute(GraphMLConstants.ATTR_TYPE, type);
    writer.writeEndElement();
  }

  private static void writeData(final XMLStreamWriter writer, final String keyId, final Object value, final String keyType)
      throws XMLStreamException {
    if (value == null)
      return;

    writer.writeStartElement(GraphMLConstants.DATA);
    writer.writeAttribute(GraphMLConstants.KEY, keyId);

    if (graphMLType(value).equals(keyType) && (!keyType.equals(GraphMLConstants.STRING) || value instanceof String))
      // THE KEY'S OWN TYPE: PLAIN GRAPHML
      writer.writeCharacters(value.toString());
    else {
      final ValueTypeHints.Encoded encoded = ValueTypeHints.encode(value, true);
      final String typeAttribute = typeAttribute(value, encoded.hint());
      if (typeAttribute != null)
        writer.writeAttribute(ARCADEDB_PREFIX, ARCADEDB_NAMESPACE, TYPE_ATTRIBUTE, typeAttribute);
      writer.writeCharacters(encoded.json() instanceof String text ? text : String.valueOf(encoded.json()));
    }
    writer.writeEndElement();
  }

  /**
   * What {@code arcadedb:type} says of a value: its type tree, {@code JSON} for a list or map whose elements JSON reads back
   * exactly, the name of a GraphML type in a key of another type, nothing for a text.
   */
  private static String typeAttribute(final Object value, final Object hint) {
    if (hint != null)
      return hint.toString();
    return switch (value) {
      case Collection<?> c -> ValueTypeHints.JSON;
      case Map<?, ?> m -> ValueTypeHints.JSON;
      case Object[] a -> ValueTypeHints.JSON;
      case Boolean b -> "BOOLEAN";
      case Integer i -> "INTEGER";
      case Double d -> "DOUBLE";
      default -> null;
    };
  }
}
