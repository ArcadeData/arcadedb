/*
 * Copyright 2023 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.arcadedb.gremlin;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.*;

/**
 * Created by Enrico Risa on 30/07/2018.
 */
public abstract class ArcadeElement<T extends Document> implements Element, Identifiable {

  protected       T           baseElement;
  protected final ArcadeGraph graph;

  protected ArcadeElement(final ArcadeGraph graph, final T baseElement, final Object... keyValues) {
    this.baseElement = baseElement;
    this.graph = graph;
    attachProperties(keyValues);
  }

  @Override
  public String label() {
    return baseElement.getTypeName();
  }

  @Override
  public <V> V value(final String key) throws NoSuchElementException {
    final V value = (V) baseElement.get(key);
    if (value == null)
      throw Property.Exceptions.propertyDoesNotExist(this, key);
    return value;
  }

  @Override
  public <V> Iterator<V> values(final String... propertyKeys) {
    final List<V> props;
    if (propertyKeys == null || propertyKeys.length == 0) {
      final Map<String, Object> properties = baseElement.toMap(false);
      props = new ArrayList<>(properties.size());
      for (final Map.Entry<String, Object> p : properties.entrySet()) {
        if (p.getValue() != null)
          props.add((V) p.getValue());
      }
    } else {
      props = new ArrayList<>(propertyKeys.length);
      for (final String p : propertyKeys) {
        final V value = (V) baseElement.get(p);
        if (value != null)
          props.add(value);
      }
    }
    return props.iterator();
  }

  @Override
  public void remove() {
    this.graph.tx().readWrite();
    this.graph.deleteElement(this);
  }

  @Override
  public Object id() {
    final RID rid = baseElement.getIdentity();
    return rid != null ? rid.toString() : null;
  }

  @Override
  public Graph graph() {
    return this.graph;
  }

  @Override
  public boolean equals(final Object object) {
    return ElementHelper.areEqual(this, object);
  }

  @Override
  public int hashCode() {
    return ElementHelper.hashCode(this);
  }

  public T getBaseElement() {
    return baseElement;
  }

  /**
   * Assign key/value pairs as properties to an {@link Vertex}.  If the value of {@link org.apache.tinkerpop.gremlin.structure.T#id} or {@link org.apache.tinkerpop.gremlin.structure.T#label} is
   * in the set of pairs, then they are ignored. The {@link VertexProperty.Cardinality} of the key is determined from
   * the {@link Graph.Features.VertexFeatures}.
   *
   * @param propertyKeyValues the key/value pairs to assign to the {@code element}
   *
   * @throws ClassCastException       if the value of the key is not a {@link String}
   * @throws IllegalArgumentException if the value of {@code element} is null
   */
  protected void attachProperties(final Object... propertyKeyValues) {
    if (propertyKeyValues == null || propertyKeyValues.length == 0)
      return;

    final boolean allowNullPropertyValues = this instanceof Vertex ?//
        graph.features().vertex().supportsNullPropertyValues() : this instanceof Edge ?//
        graph.features().edge().supportsNullPropertyValues() ://
        graph.features().vertex().properties().supportsNullPropertyValues();

    final MutableDocument mutableElement = baseElement.modify();

    for (int i = 0; i < propertyKeyValues.length; i = i + 2) {
      if (!propertyKeyValues[i].equals(org.apache.tinkerpop.gremlin.structure.T.id) && !propertyKeyValues[i].equals(
          org.apache.tinkerpop.gremlin.structure.T.label)) {
        final String propertyName = (String) propertyKeyValues[i];
        final Object propertyValue = propertyKeyValues[i + 1];

        ElementHelper.validateProperty(propertyName, propertyValue);
        ArcadeProperty.validateValue(propertyValue);

        if (!allowNullPropertyValues && null == propertyValue)
          mutableElement.remove(propertyName);
        else
          mutableElement.set(propertyName, propertyValue);
      }
    }

    if (mutableElement != baseElement)
      // REPLACE WITH MUTABLE ELEMENT
      baseElement = (T) mutableElement;
  }

  @Override
  public RID getIdentity() {
    return baseElement != null ? baseElement.getIdentity() : null;
  }

  @Override
  public Record getRecord() {
    return baseElement;
  }

  @Override
  public Record getRecord(final boolean loadContent) {
    return baseElement;
  }

  @Override
  public Document asDocument() {
    return baseElement.asDocument();
  }

  @Override
  public Document asDocument(final boolean loadContent) {
    return baseElement.asDocument(loadContent);
  }

  @Override
  public com.arcadedb.graph.Vertex asVertex() {
    return baseElement.asVertex();
  }

  @Override
  public com.arcadedb.graph.Vertex asVertex(final boolean loadContent) {
    return baseElement.asVertex(loadContent);
  }

  @Override
  public com.arcadedb.graph.Edge asEdge() {
    return baseElement.asEdge();
  }

  @Override
  public com.arcadedb.graph.Edge asEdge(final boolean loadContent) {
    return baseElement.asEdge(loadContent);
  }
}
