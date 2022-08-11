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
import org.apache.commons.collections.iterators.ArrayIterator;
import org.apache.commons.collections.iterators.SingletonIterator;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.*;

/**
 * Created by Enrico Risa on 30/07/2018.
 */
public class ArcadeEdge extends ArcadeElement<com.arcadedb.graph.Edge> implements Edge {

  protected ArcadeEdge(final ArcadeGraph graph, final com.arcadedb.graph.Edge baseElement) {
    super(graph, baseElement);
  }

  @Override
  public Vertex outVertex() {
    return new ArcadeVertex(graph, baseElement.getOutVertex());
  }

  @Override
  public Vertex inVertex() {
    return new ArcadeVertex(graph, baseElement.getInVertex());
  }

  @Override
  public Iterator<Vertex> vertices(final Direction direction) {
    switch (direction) {
    case IN:
      return new SingletonIterator(new ArcadeVertex(graph, baseElement.getInVertex()));
    case OUT:
      return new SingletonIterator(new ArcadeVertex(graph, baseElement.getOutVertex()));
    case BOTH:
      return new ArrayIterator(new Vertex[] { new ArcadeVertex(graph, baseElement.getOutVertex()), new ArcadeVertex(graph, baseElement.getInVertex()) });
    default:
      throw new IllegalArgumentException("Direction " + direction + " not supported");
    }
  }

  @Override
  public <V> Property<V> property(final String key, final V value) {
    ElementHelper.validateProperty(key, value);
    ArcadeProperty.validateValue(value);
    this.graph.tx().readWrite();

    final MutableEdge mutableElement = baseElement.modify();
    mutableElement.set(key, value);
    mutableElement.save();

    if (mutableElement != baseElement)
      // REPLACE WITH MUTABLE ELEMENT
      baseElement = mutableElement;

    return new ArcadeProperty<>(this, key, value);
  }

  @Override
  public <V> Property<V> property(final String key) {
    final V value = (V) baseElement.get(key);
    if (value != null)
      return new ArcadeProperty<>(this, key, value);
    return Property.empty();
  }

  @Override
  public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
    final List<ArcadeProperty> props;
    if (propertyKeys == null || propertyKeys.length == 0) {
      final Set<String> propNames = baseElement.getPropertyNames();
      props = new ArrayList<>(propNames.size());
      for (String p : propNames) {
        final V value = (V) baseElement.get(p);
        if (value != null)
          props.add(new ArcadeProperty<>(this, p, value));
      }
    } else {
      props = new ArrayList<>(propertyKeys.length);
      for (String p : propertyKeys) {
        final V value = (V) baseElement.get(p);
        if (value != null)
          props.add(new ArcadeProperty<>(this, p, value));
      }
    }
    return (Iterator) props.iterator();
  }

  @Override
  public String toString() {
    return StringFactory.edgeString(this);
  }
}
