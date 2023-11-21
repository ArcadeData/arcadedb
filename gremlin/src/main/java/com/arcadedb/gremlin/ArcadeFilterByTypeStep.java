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

import com.arcadedb.database.BasicDatabase;
import com.arcadedb.database.Record;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.VertexType;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Configuring;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;

import java.util.*;
import java.util.function.*;

public class ArcadeFilterByTypeStep<S, E extends Element> extends AbstractStep<S, E> implements AutoCloseable, Configuring {
  protected final     String                typeName;
  protected           Parameters            parameters = new Parameters();
  protected final     Class<E>              returnClass;
  protected           boolean               isStart;
  protected           boolean               done       = false;
  private             Traverser.Admin<S>    head       = null;
  private             Iterator<E>           iterator   = EmptyIterator.instance();
  protected transient Supplier<Iterator<E>> iteratorSupplier;

  public ArcadeFilterByTypeStep(final Traversal.Admin traversal, final Class returnClass, final boolean isStart,
      final String typeName) {
    super(traversal);
    this.returnClass = returnClass;
    this.isStart = isStart;

    if (typeName == null)
      throw new IllegalArgumentException("Type is null");

    final ArcadeGraph graph = (ArcadeGraph) traversal.getGraph().get();

    final String bucketName;
    if (typeName.startsWith("bucket:")) {
      bucketName = typeName.substring("bucket:".length());
      final DocumentType type = graph.getDatabase().getSchema().getTypeByBucketName(bucketName);
      if (type == null)
        this.typeName = null;
      else
        this.typeName = type.getName();
    } else {
      bucketName = null;
      this.typeName = typeName;
    }

    if (!graph.getDatabase().getSchema().existsType(this.typeName))
      return;

    final DocumentType type = graph.getDatabase().getSchema().getType(this.typeName);

    final BasicDatabase database = graph.getDatabase();

    if (Vertex.class.isAssignableFrom(this.returnClass)) {
      if (!(type instanceof VertexType))
        throw new IllegalArgumentException("Type '" + this.typeName + "' is not a vertex type");

      final Iterator<Record> rawIterator =
          bucketName == null ? database.iterateType(this.typeName, true) : database.iterateBucket(bucketName);
      iteratorSupplier = () -> new Iterator<>() {
        @Override
        public boolean hasNext() {
          return rawIterator.hasNext();
        }

        @Override
        public E next() {
          return (E) new ArcadeVertex(graph, rawIterator.next().asVertex());
        }
      };

    } else if (Edge.class.isAssignableFrom(this.returnClass)) {
      if (!(type instanceof EdgeType))
        throw new IllegalArgumentException("Type '" + this.typeName + "' is not an edge type");

      final Iterator<Record> rawIterator =
          bucketName == null ? database.iterateType(this.typeName, true) : database.iterateBucket(bucketName);
      iteratorSupplier = () -> new Iterator<>() {
        @Override
        public boolean hasNext() {
          return rawIterator.hasNext();
        }

        @Override
        public E next() {
          return (E) new ArcadeEdge(graph, rawIterator.next().asEdge());
        }
      };
    } else
      throw new IllegalArgumentException("Unsupported returning class '" + returnClass + "'");
  }

  public String toString() {
    return StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(Locale.ENGLISH), typeName);
  }

  @Override
  public Parameters getParameters() {
    return this.parameters;
  }

  @Override
  public void configure(final Object... keyValues) {
    this.parameters.set(null, keyValues);
  }

  @Override
  protected Traverser.Admin<E> processNextStart() {
    while (true) {
      if (this.iterator.hasNext()) {
        return this.isStart ?
            this.getTraversal().getTraverserGenerator().generate(this.iterator.next(), (Step) this, 1l) :
            this.head.split(this.iterator.next(), this);
      } else {
        if (this.isStart) {
          if (this.done)
            throw FastNoSuchElementException.instance();
          else {
            this.done = true;
            this.iterator = null == this.iteratorSupplier ? EmptyIterator.instance() : this.iteratorSupplier.get();
          }
        } else {
          this.head = this.starts.next();
          this.iterator = null == this.iteratorSupplier ? EmptyIterator.instance() : this.iteratorSupplier.get();
        }
      }
    }
  }

  @Override
  public void reset() {
    super.reset();
    this.head = null;
    this.done = false;
    this.iterator = EmptyIterator.instance();
  }

  @Override
  public int hashCode() {
    return Objects.hash(returnClass, typeName);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (!(o instanceof ArcadeFilterByTypeStep))
      return false;
    if (!super.equals(o))
      return false;
    final ArcadeFilterByTypeStep<?, ?> that = (ArcadeFilterByTypeStep<?, ?>) o;
    return returnClass.equals(that.returnClass) && typeName.equals(that.typeName);
  }

  /**
   * Attempts to close an underlying iterator if it is of type {@link CloseableIterator}. Graph providers may choose
   * to return this interface containing their vertices and edges if there are expensive resources that might need to
   * be released at some point.
   */
  @Override
  public void close() {
    CloseableIterator.closeIterator(iterator);
  }
}
