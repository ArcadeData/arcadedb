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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.gremlin.ArcadeGraph;
import com.arcadedb.integration.misc.ValueTypeHints;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Reads GraphML, the files {@link ArcadeGraphMLWriter} writes and any other, giving every value its type back (issue #10202): a
 * value with an {@code arcadedb:type} attribute is read through it, any other through the GraphML type of its key, as
 * TinkerPop's reader does. Labels are read from TinkerPop's {@code labelV} / {@code labelE} keys.
 * <p>
 * A key whose values do not all parse as its declared type no longer aborts the whole import with a NumberFormatException:
 * such a value is kept as the text it is.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ArcadeGraphMLReader implements GraphReader {
  private static final long BATCH_SIZE = 10_000;

  private record Key(String name, String type) {
  }

  private record PendingEdge(String id, String source, String target, String label, Map<String, Object> properties) {
  }

  @Override
  public void readGraph(final InputStream inputStream, final Graph graph) throws IOException {
    final XMLInputFactory factory = XMLInputFactory.newInstance();
    // NO DTD, NO EXTERNAL ENTITY: A GRAPHML FILE IS DATA, NOT A WAY TO REACH THE SERVER'S FILES OR NETWORK
    factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
    factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);

    final DatabaseInternal database = graph instanceof ArcadeGraph arcade ? (DatabaseInternal) arcade.getDatabase() : null;
    final boolean transactional = graph.features().graph().supportsTransactions();

    final Map<String, Key> keys = new HashMap<>();
    final Map<String, Object> vertexIds = new HashMap<>();
    final List<PendingEdge> pendingEdges = new ArrayList<>();
    long counter = 0;

    try {
      final XMLStreamReader reader = factory.createXMLStreamReader(inputStream);

      String elementId = null;
      String source = null;
      String target = null;
      String label = null;
      Map<String, Object> properties = null;
      boolean inVertex = false;
      boolean inEdge = false;

      while (reader.hasNext()) {
        final int event = reader.next();
        if (event == XMLStreamConstants.START_ELEMENT) {
          switch (reader.getLocalName()) {
          case GraphMLConstants.KEY -> keys.put(reader.getAttributeValue(null, GraphMLConstants.ID),
              new Key(reader.getAttributeValue(null, GraphMLConstants.ATTR_NAME), reader.getAttributeValue(null, GraphMLConstants.ATTR_TYPE)));
          case GraphMLConstants.NODE -> {
            inVertex = true;
            elementId = reader.getAttributeValue(null, GraphMLConstants.ID);
            label = Vertex.DEFAULT_LABEL;
            properties = new LinkedHashMap<>();
          }
          case GraphMLConstants.EDGE -> {
            inEdge = true;
            elementId = reader.getAttributeValue(null, GraphMLConstants.ID);
            source = reader.getAttributeValue(null, GraphMLConstants.SOURCE);
            target = reader.getAttributeValue(null, GraphMLConstants.TARGET);
            label = Edge.DEFAULT_LABEL;
            properties = new LinkedHashMap<>();
          }
          case GraphMLConstants.DATA -> {
            if (properties == null)
              break;
            final Key key = keys.get(reader.getAttributeValue(null, GraphMLConstants.KEY));
            final String typeAttribute = reader.getAttributeValue(ArcadeGraphMLWriter.ARCADEDB_NAMESPACE, ArcadeGraphMLWriter.TYPE_ATTRIBUTE);
            final String text = reader.getElementText();
            if (key == null)
              break;
            if ((inVertex && GraphMLConstants.LABEL_V.equals(key.name())) || (inEdge && GraphMLConstants.LABEL_E.equals(key.name())))
              label = text;
            else
              properties.put(key.name(), value(database, text, key.type(), typeAttribute));
          }
          default -> {
          }
          }
        } else if (event == XMLStreamConstants.END_ELEMENT) {
          final String name = reader.getLocalName();
          if (GraphMLConstants.NODE.equals(name)) {
            final Vertex vertex = graph.addVertex(keyValues(label, properties));
            vertexIds.put(elementId, vertex.id());
            inVertex = false;
            properties = null;
            counter = commitIfDue(graph, transactional, ++counter);
          } else if (GraphMLConstants.EDGE.equals(name)) {
            final PendingEdge edge = new PendingEdge(elementId, source, target, label, properties);
            // AN EDGE MAY NAME A NODE THE FILE DECLARES LATER: KEPT FOR THE END OF THE FILE
            if (vertexIds.containsKey(source) && vertexIds.containsKey(target)) {
              addEdge(graph, vertexIds, edge);
              counter = commitIfDue(graph, transactional, ++counter);
            } else
              pendingEdges.add(edge);
            inEdge = false;
            properties = null;
          }
        }
      }
      reader.close();

      for (final PendingEdge edge : pendingEdges) {
        if (!vertexIds.containsKey(edge.source()) || !vertexIds.containsKey(edge.target()))
          throw new IOException("Edge '" + edge.id() + "' connects node '" + (vertexIds.containsKey(edge.source()) ?
              edge.target() :
              edge.source()) + "', which the file does not declare");
        addEdge(graph, vertexIds, edge);
        counter = commitIfDue(graph, transactional, ++counter);
      }

      if (transactional)
        graph.tx().commit();
    } catch (final XMLStreamException e) {
      throw new IOException(e);
    }
  }

  private static long commitIfDue(final Graph graph, final boolean transactional, final long counter) {
    if (transactional && counter % BATCH_SIZE == 0)
      graph.tx().commit();
    return counter;
  }

  private static void addEdge(final Graph graph, final Map<String, Object> vertexIds, final PendingEdge edge) {
    final Vertex out = graph.vertices(vertexIds.get(edge.source())).next();
    final Vertex in = graph.vertices(vertexIds.get(edge.target())).next();
    final Object[] keyValues = new Object[edge.properties().size() * 2];
    int i = 0;
    for (final Map.Entry<String, Object> entry : edge.properties().entrySet()) {
      keyValues[i++] = entry.getKey();
      keyValues[i++] = entry.getValue();
    }
    out.addEdge(edge.label(), in, keyValues);
  }

  private static Object[] keyValues(final String label, final Map<String, Object> properties) {
    final Object[] keyValues = new Object[2 + properties.size() * 2];
    keyValues[0] = T.label;
    keyValues[1] = label;
    int i = 2;
    for (final Map.Entry<String, Object> entry : properties.entrySet()) {
      keyValues[i++] = entry.getKey();
      keyValues[i++] = entry.getValue();
    }
    return keyValues;
  }

  /** A value read through its {@code arcadedb:type}, or else through the GraphML type of its key. */
  static Object value(final DatabaseInternal database, final String text, final String keyType, final String typeAttribute) {
    if (typeAttribute != null && !typeAttribute.isEmpty()) {
      final char first = typeAttribute.charAt(0);
      final Object hint = first == '[' ? new JSONArray(typeAttribute) : first == '{' ? new JSONObject(typeAttribute) : typeAttribute;
      final boolean jsonText = !(hint instanceof String token) || ValueTypeHints.JSON.equals(token)
          || ValueTypeHints.EMBEDDED.equals(token) || token.startsWith("ARRAY_OF_");
      final Object json = jsonText ? parseJson(text) : text;
      if (ValueTypeHints.JSON.equals(hint))
        return UNTYPED.apply(json);
      return ValueTypeHints.decode(database, json, hint, UNTYPED);
    }

    if (keyType == null)
      return text;
    try {
      return switch (keyType) {
        case GraphMLConstants.BOOLEAN -> Boolean.valueOf(text);
        case GraphMLConstants.INT -> Integer.valueOf(text);
        case GraphMLConstants.LONG -> Long.valueOf(text);
        case GraphMLConstants.FLOAT -> Float.valueOf(text);
        case GraphMLConstants.DOUBLE -> Double.valueOf(text);
        default -> text;
      };
    } catch (final NumberFormatException e) {
      // A VALUE THAT IS NOT OF ITS KEY'S TYPE STAYS THE TEXT IT IS, RATHER THAN ABORTING THE WHOLE FILE
      return text;
    }
  }

  private static Object parseJson(final String text) {
    final String trimmed = text.trim();
    if (trimmed.startsWith("["))
      return new JSONArray(trimmed);
    if (trimmed.startsWith("{"))
      return new JSONObject(trimmed);
    return trimmed;
  }

  /** A JSON value as plain Java: what a value with no type of its own is read as. */
  private static final Function<Object, Object> UNTYPED = new Function<>() {
    @Override
    public Object apply(final Object json) {
      if (json instanceof JSONArray array) {
        final List<Object> list = new ArrayList<>(array.length());
        for (int i = 0; i < array.length(); i++)
          list.add(apply(array.get(i)));
        return list;
      }
      if (json instanceof JSONObject object) {
        final Map<String, Object> map = new LinkedHashMap<>();
        for (final String key : object.keySet())
          map.put(key, apply(object.get(key)));
        return map;
      }
      return json;
    }
  };

  @Override
  public Vertex readVertex(final InputStream inputStream, final Function<Attachable<Vertex>, Vertex> vertexAttachMethod) {
    throw new UnsupportedOperationException("GraphML does not allow for a partial structure");
  }

  @Override
  public Vertex readVertex(final InputStream inputStream, final Function<Attachable<Vertex>, Vertex> vertexAttachMethod,
      final Function<Attachable<Edge>, Edge> edgeAttachMethod, final Direction attachEdgesOfThisDirection) {
    throw new UnsupportedOperationException("GraphML does not allow for a partial structure");
  }

  @Override
  public Iterator<Vertex> readVertices(final InputStream inputStream, final Function<Attachable<Vertex>, Vertex> vertexAttachMethod,
      final Function<Attachable<Edge>, Edge> edgeAttachMethod, final Direction attachEdgesOfThisDirection) {
    throw new UnsupportedOperationException("GraphML does not allow for a partial structure");
  }

  @Override
  public Edge readEdge(final InputStream inputStream, final Function<Attachable<Edge>, Edge> edgeAttachMethod) {
    throw new UnsupportedOperationException("GraphML does not allow for a partial structure");
  }

  @Override
  public VertexProperty readVertexProperty(final InputStream inputStream,
      final Function<Attachable<VertexProperty>, VertexProperty> vertexPropertyAttachMethod) {
    throw new UnsupportedOperationException("GraphML does not allow for a partial structure");
  }

  @Override
  public Property readProperty(final InputStream inputStream, final Function<Attachable<Property>, Property> propertyAttachMethod) {
    throw new UnsupportedOperationException("GraphML does not allow for a partial structure");
  }

  @Override
  public <C> C readObject(final InputStream inputStream, final Class<? extends C> clazz) {
    throw new UnsupportedOperationException("GraphML does not allow for a partial structure");
  }
}
