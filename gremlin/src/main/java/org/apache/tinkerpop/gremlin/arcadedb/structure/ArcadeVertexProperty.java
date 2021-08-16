/*
 * Copyright 2021 Arcade Data Ltd
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

package org.apache.tinkerpop.gremlin.arcadedb.structure;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by Enrico Risa on 30/07/2018.
 */
public class ArcadeVertexProperty<T> implements VertexProperty<T> {

  protected final String       key;
  protected final T            value;
  protected final ArcadeVertex vertex;

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
    vertex.getBaseElement().remove(key);
    vertex.getBaseElement().save();
  }

  @Override
  public Object id() {
    return (long) (this.key.hashCode() + this.value.hashCode() + this.vertex.id().hashCode());
  }

  @Override
  public <V> Property<V> property(final String key, final V value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <U> Iterator<Property<U>> properties(final String... propertyKeys) {
    throw new UnsupportedOperationException();
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
