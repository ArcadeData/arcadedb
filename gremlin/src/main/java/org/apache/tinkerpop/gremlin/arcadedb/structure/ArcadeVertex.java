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
package org.apache.tinkerpop.gremlin.arcadedb.structure;

import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.schema.EdgeType;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.*;

/**
 * Created by Enrico Risa on 30/07/2018.
 */
public class ArcadeVertex extends ArcadeElement<com.arcadedb.graph.Vertex> implements Vertex {

  protected ArcadeVertex(final ArcadeGraph graph, final com.arcadedb.graph.Vertex baseElement) {
    super(graph, baseElement);
  }

  @Override
  public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
    if (null == inVertex)
      throw Graph.Exceptions.argumentCanNotBeNull("inVertex");
    ElementHelper.validateLabel(label);
    ElementHelper.legalPropertyKeyValueArray(keyValues);

    if (ElementHelper.getIdValue(keyValues).isPresent())
      throw Edge.Exceptions.userSuppliedIdsNotSupported();

    this.graph.tx().readWrite();
    ArcadeVertex vertex = (ArcadeVertex) inVertex;

    //ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);

    if (!this.graph.getDatabase().getSchema().existsType(label))
      this.graph.getDatabase().getSchema().createEdgeType(label);
    else if (!(this.graph.getDatabase().getSchema().getType(label) instanceof EdgeType))
      throw new IllegalArgumentException("Type '" + label + "' is not a edge");

    com.arcadedb.graph.Vertex baseElement = getBaseElement();

    MutableEdge edge = baseElement.newEdge(label, vertex.getBaseElement(), true);
    ArcadeEdge arcadeEdge = new ArcadeEdge(graph, edge);
    ElementHelper.attachProperties(arcadeEdge, keyValues);
    edge.save();
    return arcadeEdge;
  }

  @Override
  public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
    ElementHelper.validateProperty(key, value);
    ArcadeProperty.validateValue(value);
    if (ElementHelper.getIdValue(keyValues).isPresent())
      throw Vertex.Exceptions.userSuppliedIdsNotSupported();

    if (cardinality != VertexProperty.Cardinality.single)
      throw VertexProperty.Exceptions.multiPropertiesNotSupported();
    if (keyValues.length > 0)
      throw VertexProperty.Exceptions.metaPropertiesNotSupported();

    this.graph.tx().readWrite();

    final MutableVertex mutableElement = baseElement.modify();

    mutableElement.set(key, value);
    mutableElement.save();

    if (mutableElement != baseElement)
      // REPLACE WITH MUTABLE ELEMENT
      baseElement = mutableElement;

    return new ArcadeVertexProperty<>(this, key, value);
  }

  @Override
  public <V> VertexProperty<V> property(final String key, final V value) {
    ElementHelper.validateProperty(key, value);
    ArcadeProperty.validateValue(value);
    this.graph.tx().readWrite();

    final MutableVertex mutableElement = baseElement.modify();
    mutableElement.set(key, value);
    mutableElement.save();

    if (mutableElement != baseElement)
      // REPLACE WITH MUTABLE ELEMENT
      baseElement = mutableElement;

    return new ArcadeVertexProperty<>(this, key, value);
  }

  @Override
  public <V> VertexProperty<V> property(final String key) {
    final V value = (V) baseElement.get(key);
    if (value != null)
      return new ArcadeVertexProperty<>(this, key, value);
    return VertexProperty.empty();
  }

  @Override
  public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
    final List<ArcadeVertexProperty> props;
    if (propertyKeys == null || propertyKeys.length == 0) {
      final Set<String> propNames = baseElement.getPropertyNames();
      props = new ArrayList<>(propNames.size());
      for (String p : propNames) {
        final V value = (V) baseElement.get(p);
        if (value != null)
          props.add(new ArcadeVertexProperty<>(this, p, value));
      }
    } else {
      props = new ArrayList<>(propertyKeys.length);
      for (String p : propertyKeys) {
        final V value = (V) baseElement.get(p);
        if (value != null)
          props.add(new ArcadeVertexProperty<>(this, p, value));
      }
    }
    return (Iterator) props.iterator();
  }

  @Override
  public Set<String> keys() {
    return baseElement.getPropertyNames();
  }

  @Override
  public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
    final List<Edge> result = new ArrayList<>();

    if (edgeLabels.length == 0)
      for (com.arcadedb.graph.Edge edge : this.baseElement.getEdges(ArcadeGraph.mapDirection(direction)))
        result.add(new ArcadeEdge(this.graph, edge));
    else
      for (com.arcadedb.graph.Edge edge : this.baseElement.getEdges(ArcadeGraph.mapDirection(direction), edgeLabels))
        result.add(new ArcadeEdge(this.graph, edge));

    return result.iterator();
  }

  @Override
  public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
    final List<Vertex> result = new ArrayList<>();

    if (edgeLabels.length == 0)
      for (com.arcadedb.graph.Vertex vertex : this.baseElement.getVertices(ArcadeGraph.mapDirection(direction)))
        result.add(new ArcadeVertex(this.graph, vertex));
    else
      for (com.arcadedb.graph.Vertex vertex : this.baseElement.getVertices(ArcadeGraph.mapDirection(direction), edgeLabels))
        result.add(new ArcadeVertex(this.graph, vertex));

    return result.iterator();
  }

  @Override
  public String toString() {
    return StringFactory.vertexString(this);
  }
}
