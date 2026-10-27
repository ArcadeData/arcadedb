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

import com.arcadedb.database.BasicDatabase;
import com.arcadedb.database.Record;
import com.arcadedb.schema.DocumentType;
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

import java.util.Iterator;
import java.util.Locale;
import java.util.Objects;

public class ArcadeFilterByTypeStep<S, E extends Element> extends AbstractStep<S, E> implements AutoCloseable, Configuring {
  /** A {@code hasLabel()} value with this prefix names one bucket rather than a type. */
  public static final String                BUCKET_PREFIX = ArcadeLabels.BUCKET_PREFIX;
  protected final     String                typeName;
  private final       String                label;
  private final       String                bucketName;
  private final       ArcadeGraph           graph;
  protected           Parameters            parameters = new Parameters();
  protected final     Class<E>              returnClass;
  protected           boolean               isStart;
  protected           boolean               done       = false;
  private             Traverser.Admin<S>    head       = null;
  private             Iterator<E>           iterator   = EmptyIterator.instance();

  public ArcadeFilterByTypeStep(final Traversal.Admin traversal, final Class returnClass, final boolean isStart,
      final String typeName) {
    super(traversal);
    this.returnClass = returnClass;
    this.isStart = isStart;

    if (typeName == null)
      throw new IllegalArgumentException("Type is null");

    this.graph = (ArcadeGraph) traversal.getGraph().get();

    this.label = typeName;
    this.bucketName = ArcadeLabels.bucketName(typeName);
    if (bucketName != null) {
      final DocumentType type = ArcadeLabels.typeOf(graph.getDatabase().getSchema(), typeName);
      this.typeName = type != null ? type.getName() : null;
    } else
      this.typeName = typeName;

    if (!Vertex.class.isAssignableFrom(this.returnClass) && !Edge.class.isAssignableFrom(this.returnClass))
      throw new IllegalArgumentException("Unsupported returning class '" + returnClass + "'");

    // THE SCHEMA IS RESOLVED WHEN THE ITERATOR IS OPENED, NOT HERE: THIS CONSTRUCTOR RUNS WHEN THE TRAVERSAL IS COMPILED, AND AN
    // EARLIER STEP OF THE SAME TRAVERSAL (addV()) MAY CREATE THE TYPE BEFORE THIS STEP EXECUTES (ISSUE #9335).
  }

  @SuppressWarnings("unchecked")
  private Iterator<E> openIterator() {
    final BasicDatabase database = graph.getDatabase();

    // A bucket may have been added to a type (or the type created) after compilation: resolve the label again, through the
    // resolver ArcadeCountGlobalStep uses too, so a count() after this step cannot answer a label differently (#10763). It also
    // filters inside the current kind: a vertex traversal filtered by an edge type matches nothing, and vice versa (#5223)
    final DocumentType type = ArcadeLabels.typeOf(database.getSchema(), label, returnClass);
    if (type == null)
      return EmptyIterator.instance();
    final String resolvedTypeName = type.getName();

    if (Vertex.class.isAssignableFrom(this.returnClass)) {
      final Iterator<Record> rawIterator = bucketName == null ?
          database.iterateType(resolvedTypeName, true) :
          database.iterateBucket(bucketName);
      return new Iterator<>() {
        @Override
        public boolean hasNext() {
          return rawIterator.hasNext();
        }

        @Override
        public E next() {
          return (E) new ArcadeVertex(graph, rawIterator.next().asVertex());
        }
      };
    }

    // A LIGHTWEIGHT EDGE HAS NO RECORD, SO THE BUCKET SCAN OF ITS TYPE IS EMPTY BY CONSTRUCTION (#9142)
    final boolean lightweight = bucketName == null && LightweightEdges.isHeldBy(type);

    final Iterator<? extends Record> rawIterator = lightweight ?
        LightweightEdges.ofType(database, resolvedTypeName) :
        bucketName == null ? database.iterateType(resolvedTypeName, true) : database.iterateBucket(bucketName);
    return new CloseableIterator<E>() {
      @Override
      public void close() {
        CloseableIterator.closeIterator(rawIterator);
      }

      @Override
      public boolean hasNext() {
        return rawIterator.hasNext();
      }

      @Override
      public E next() {
        return (E) new ArcadeEdge(graph, rawIterator.next().asEdge());
      }
    };
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
            this.iterator = openIterator();
          }
        } else {
          this.head = this.starts.next();
          this.iterator = openIterator();
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
    return returnClass.equals(that.returnClass) && Objects.equals(typeName, that.typeName) && Objects.equals(bucketName, that.bucketName);
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
