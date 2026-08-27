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
package com.arcadedb.gremlin;

import com.arcadedb.database.MutableEmbeddedDocument;
import com.arcadedb.graph.MutableVertex;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by Enrico Risa on 30/07/2018.
 */
public class ArcadeVertexProperty<T> implements VertexProperty<T> {

  private static final String ID_SEPARATOR = "-";

  protected final String       key;
  protected final T            value;
  protected final ArcadeVertex vertex;

  /**
   * Cached only once the vertex has a RID, because {@code equals()}/{@code hashCode()} both go through {@link #id()}
   * and would otherwise rebuild the string on every probe of a {@code Set}, a {@code Map} or a {@code dedup()}.
   */
  private String id;

  protected ArcadeVertexProperty(final ArcadeVertex vertex, final String key, final T value) {
    this.vertex = vertex;
    this.key = key;
    this.value = value;
  }

  @Override
  public String key() {
    return key;
  }

  @Override
  public T value() throws NoSuchElementException {
    return value;
  }

  @Override
  public boolean isPresent() {
    return value != null;
  }

  @Override
  public Vertex element() {
    return vertex;
  }

  @Override
  public void remove() {
    graph().tx().readWrite();

    final MutableVertex mutableElement = vertex.baseElement.modify();
    mutableElement.remove(key);
    mutableElement.save();

    if (mutableElement != vertex.baseElement)
      // REPLACE WITH MUTABLE ELEMENT
      vertex.baseElement = mutableElement;

  }

  /**
   * A vertex property has single cardinality here, so {@code (vertex, key)} is its identity. TinkerPop's
   * {@code ElementHelper} compares vertex properties by id alone, so the id must be unique: deriving it from a sum of
   * hash codes made distinct properties of the same vertex compare equal and be deduplicated away (issue #6823).
   * The vertex id is a RID, which never contains the separator, so no pair of (vertex, key) can produce the same id.
   */
  @Override
  public Object id() {
    String result = id;
    if (result == null) {
      final Object vertexId = this.vertex.id();
      // AN UNSAVED VERTEX HAS NO RID YET: FALL BACK TO ITS TRANSIENT ID SO id() NEITHER THROWS NOR COLLIDES.
      result = (vertexId != null ? vertexId : this.vertex.transientId()) + ID_SEPARATOR + this.key;
      if (vertexId != null)
        // ONLY A RID IS FINAL: AN UNSAVED VERTEX WILL GET ONE LATER AND THE ID HAS TO FOLLOW IT.
        id = result;
    }
    return result;
  }

  @Override
  public <V> Property<V> property(final String key, final V value) {
    if (this.value instanceof MutableEmbeddedDocument document) {
      document.set(key, value);
      return new ArcadeVertexProperty<V>(vertex, key, value);
    } else
      throw new UnsupportedOperationException();
  }

  /**
   * Not supported.
   */
  @Override
  public <U> Iterator<Property<U>> properties(final String... propertyKeys) {
    return Collections.emptyIterator();
  }

  @Override
  public boolean equals(final Object object) {
    return ElementHelper.areEqual(this, object);
  }

  @Override
  public int hashCode() {
    return ElementHelper.hashCode((Element) this);
  }

  @Override
  public String toString() {
    return StringFactory.propertyString(this);
  }
}
