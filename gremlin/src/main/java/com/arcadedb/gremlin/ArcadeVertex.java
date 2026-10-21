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

import com.arcadedb.database.Database;
import com.arcadedb.database.EmbeddedDocument;
import com.arcadedb.database.MutableEmbeddedDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.GraphTraversalProvider;
import com.arcadedb.graph.GraphTraversalProviderRegistry;
import com.arcadedb.graph.GraphViewPins;
import com.arcadedb.graph.LightEdge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Enrico Risa on 30/07/2018.
 */
public class ArcadeVertex extends ArcadeElement<com.arcadedb.graph.Vertex> implements Vertex {

  private static final AtomicLong TRANSIENT_IDS = new AtomicLong();

  /**
   * Assigned lazily, and only for a vertex that has no RID yet, so a saved vertex never pays for it.
   */
  private volatile String transientId;

  protected ArcadeVertex(final ArcadeGraph graph, final com.arcadedb.graph.Vertex baseElement, final Object... keyValues) {
    super(graph, baseElement, keyValues);
  }

  /**
   * Returns an identifier unique to this instance, for the window in which the vertex has no RID yet. Unlike
   * {@link System#identityHashCode}, which the JDK never promises to be collision-free, a counter cannot hand the same
   * value to two vertices, so the ids of their properties cannot collide either (see {@link ArcadeVertexProperty#id()}).
   */
  String transientId() {
    String id = transientId;
    if (id == null) {
      synchronized (this) {
        id = transientId;
        if (id == null)
          transientId = id = "?" + TRANSIENT_IDS.incrementAndGet();
      }
    }
    return id;
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
    final ArcadeVertex vertex = (ArcadeVertex) inVertex;
    // AN EDGE FROM OR TO A REMOVED VERTEX WOULD BE COMMITTED WITHOUT IT (#10196)
    checkNotRemoved();
    vertex.checkNotRemoved();

    //ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);

    final String typeName;
    final String bucketName;
    if (label.startsWith("bucket:")) {
      bucketName = label.substring("bucket:".length());
      final DocumentType type = graph.getDatabase().getSchema().getTypeByBucketName(bucketName);
      if (type == null)
        typeName = null;
      else
        typeName = type.getName();
    } else {
      bucketName = null;
      typeName = label;
    }

    // getOrCreate: two traversals adding the first edge of a label concurrently both end with the type (issue #10145)
    if (!this.graph.getDatabase().getSchema().existsType(typeName))
      this.graph.getDatabase().getSchema().getOrCreateEdgeType(typeName);
    else if (!(this.graph.getDatabase().getSchema().getType(typeName) instanceof EdgeType))
      throw new IllegalArgumentException("Type '" + typeName + "' is not a edge");

    final com.arcadedb.graph.Vertex baseElement = getBaseElement();

    final MutableEdge edge = baseElement.newEdge(label, vertex.getBaseElement());
    final ArcadeEdge arcadeEdge = new ArcadeEdge(graph, edge, keyValues);
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
    checkNotRemoved();

    final MutableVertex mutableElement = baseElement.modify();
    // The key and value come from the traversal, not from the record read: no stale-read refusal (#8610, #8707)
    mutableElement.clearBasedOnStaleRead();

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
    checkNotRemoved();

    final MutableVertex mutableElement = baseElement.modify();
    // The key and value come from the traversal, not from the record read: no stale-read refusal (#8610, #8707)
    mutableElement.clearBasedOnStaleRead();
    mutableElement.set(key, value);
    mutableElement.save();

    if (mutableElement != baseElement)
      // REPLACE WITH MUTABLE ELEMENT
      baseElement = mutableElement;

    return new ArcadeVertexProperty<>(this, key, value);
  }

  public VertexProperty<EmbeddedDocument> embed(final String key, final String typeName) {
    this.graph.tx().readWrite();
    checkNotRemoved();

    final MutableVertex mutableElement = baseElement.modify();
    // The key and value come from the traversal, not from the record read: no stale-read refusal (#8610, #8707)
    mutableElement.clearBasedOnStaleRead();
    final MutableEmbeddedDocument embedded = mutableElement.newEmbeddedDocument(typeName, key);
    mutableElement.save();

    if (mutableElement != baseElement)
      // REPLACE WITH MUTABLE ELEMENT
      baseElement = mutableElement;

    return new ArcadeVertexProperty<>(this, key, embedded);
  }

  @Override
  public <V> VertexProperty<V> property(final String key) {
    if (isRemoved())
      return VertexProperty.empty();
    final V value = (V) baseElement.get(key);
    if (value != null)
      return new ArcadeVertexProperty<>(this, key, value);
    return VertexProperty.empty();
  }

  @Override
  public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
    if (isRemoved())
      return Collections.emptyIterator();
    final List<ArcadeVertexProperty> props;
    if (propertyKeys == null || propertyKeys.length == 0) {
      final Set<String> propNames = baseElement.getPropertyNames();
      props = new ArrayList<>(propNames.size());
      for (final String p : propNames) {
        final V value = (V) baseElement.get(p);
        if (value != null)
          props.add(new ArcadeVertexProperty<>(this, p, value));
      }
    } else {
      props = new ArrayList<>(propertyKeys.length);
      for (final String p : propertyKeys) {
        final V value = (V) baseElement.get(p);
        if (value != null)
          props.add(new ArcadeVertexProperty<>(this, p, value));
      }
    }
    return (Iterator) props.iterator();
  }

  @Override
  public Set<String> keys() {
    if (isRemoved())
      return Collections.emptySet();
    return baseElement.getPropertyNames();
  }

  @Override
  public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
    // A terminated statement stops here, at every step of a traversal, and every so often inside a supernode (#9689)
    ArcadeGraph.checkNotTerminated();
    // A REMOVED VERTEX HAS NO EDGES: ITS RECORD STILL POINTS TO THE LISTS ITS DELETE FREED (#10196)
    if (isRemoved())
      return Collections.emptyIterator();
    // READ AS THE TRAVERSAL PULLS THEM: AN EXISTENCE TEST OR A limit() OVER A SUPERNODE STOPS AT THE FIRST ENTRIES (#10308)
    final Iterator<com.arcadedb.graph.Edge> source = (edgeLabels.length == 0 ?
        this.baseElement.getEdges(ArcadeGraph.mapDirection(direction)) :
        this.baseElement.getEdges(ArcadeGraph.mapDirection(direction), edgeLabels)).iterator();
    return new IteratorWalk<com.arcadedb.graph.Edge, Edge>(source) {
      @Override
      protected Edge wrap(final com.arcadedb.graph.Edge edge) {
        return exists(edge) ? new ArcadeEdge(graph, edge) : null;
      }
    };
  }

  /** Filters out deleted edges. A lightweight edge has no record to look for: it exists as long as the vertex lists it (#9142). */
  private boolean exists(final com.arcadedb.graph.Edge edge) {
    return edge instanceof LightEdge || graph.getDatabase().existsRecord(edge.getIdentity());
  }

  @Override
  public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
    // A terminated statement stops here, at every step of a traversal, and every so often inside a supernode (#9689)
    ArcadeGraph.checkNotTerminated();
    if (isRemoved())
      return Collections.emptyIterator();
    // CSR fast path: use GAV provider when available for O(1) neighbor lookup. Pinned, so the node id, the neighbours and their
    // RIDs come from one snapshot whatever the view does meanwhile (issue #10415)
    final GraphTraversalProvider provider = graph.getDatabase() instanceof Database db
        ? GraphViewPins.pinAlone(edgeLabels.length == 0
            ? GraphTraversalProviderRegistry.findProvider(db)
            : GraphTraversalProviderRegistry.findProvider(db, edgeLabels))
        : null;
    if (provider != null) {
      final int nodeId = provider.getNodeId(baseElement.getIdentity());
      if (nodeId >= 0) {
        final com.arcadedb.graph.Vertex.DIRECTION dir = ArcadeGraph.mapDirection(direction);
        final int[] neighborIds = edgeLabels.length == 0
            ? provider.getNeighborIds(nodeId, dir)
            : provider.getNeighborIds(nodeId, dir, edgeLabels);
        // THE IDS ARE ONE ARRAY OF PRIMITIVES, BUT EACH NEIGHBOR IS LOOKED UP AND WRAPPED WHEN THE TRAVERSAL PULLS IT (#10308)
        return new NeighborIdWalk(provider, neighborIds);
      }
    }

    // OLTP fallback, read as the traversal pulls the neighbors: an existence test or a limit() over a supernode stops at the first
    // entries (#10308)
    final Iterator<com.arcadedb.graph.Vertex> source = (edgeLabels.length == 0 ?
        this.baseElement.getVertices(ArcadeGraph.mapDirection(direction)) :
        this.baseElement.getVertices(ArcadeGraph.mapDirection(direction), edgeLabels)).iterator();
    return new IteratorWalk<com.arcadedb.graph.Vertex, Vertex>(source) {
      @Override
      protected Vertex wrap(final com.arcadedb.graph.Vertex vertex) {
        return graph.getDatabase().existsRecord(vertex.getIdentity()) ? new ArcadeVertex(graph, vertex) : null;
      }
    };
  }

  /**
   * Walks the entries of an adjacency list one at a time, wrapping each when the caller asks for it, in the order of the list and
   * with its duplicates (a parallel edge lists its neighbor twice). An entry whose record is gone is skipped, as it was when the
   * whole list was read up front, but now as of the moment it is reached: a neighbor a traversal deleted after this walk began is
   * not handed out afterwards. The statement termination is checked every {@link ArcadeGraph#TERMINATION_CHECK_INTERVAL} entries
   * walked, inside a supernode (#9689).
   * <p>
   * The walk ends when this vertex is removed meanwhile: its delete freed the lists the walk would go on to read, and a removed
   * vertex has no edges (#10196).
   */
  private abstract class LazyWalk<T> implements Iterator<T> {
    private T   next;
    private int walked;

    protected abstract boolean hasMoreEntries();

    /** @return the element handed out for the next entry, or null when its record is gone and the entry is skipped */
    protected abstract T wrapNextEntry();

    @Override
    public boolean hasNext() {
      while (true) {
        if (isRemoved()) {
          // THE VERTEX WENT: ITS EDGES WENT WITH IT, THE ELEMENT A hasNext() ALREADY PULLED INCLUDED
          next = null;
          return false;
        }
        if (next != null) {
          // PULLED BY AN EARLIER hasNext(): AN ELEMENT REMOVED SINCE IS NOT HANDED OUT, AS ONE REMOVED BEFORE IT WAS PULLED IS NOT
          if (next instanceof ArcadeElement<?> element && element.isRemoved())
            next = null;
          else
            return true;
        }
        if (!hasMore())
          return false;
        if (++walked % ArcadeGraph.TERMINATION_CHECK_INTERVAL == 0)
          ArcadeGraph.checkNotTerminated();
        next = wrapNextEntry();
      }
    }

    private boolean hasMore() {
      try {
        return hasMoreEntries();
      } catch (final RecordNotFoundException e) {
        // A segment of the list was freed under the walk by the delete of this vertex, one the graph did not see (a statement ran
        // it): its edges went with it, so the walk is over
        if (graph.getDatabase().existsRecord(baseElement.getIdentity()))
          throw e;
        return false;
      }
    }

    @Override
    public T next() {
      if (!hasNext())
        throw new NoSuchElementException();
      final T result = next;
      next = null;
      return result;
    }
  }

  /** A {@link LazyWalk} over the entries an iterator of the engine yields. */
  private abstract class IteratorWalk<S, T> extends LazyWalk<T> {
    private final Iterator<S> source;

    IteratorWalk(final Iterator<S> source) {
      this.source = source;
    }

    protected abstract T wrap(S entry);

    @Override
    protected boolean hasMoreEntries() {
      return source.hasNext();
    }

    @Override
    protected T wrapNextEntry() {
      return wrap(source.next());
    }
  }

  /** A {@link LazyWalk} over the neighbor ids a graph view answered, kept as the array of primitives it is. */
  private final class NeighborIdWalk extends LazyWalk<Vertex> {
    private final GraphTraversalProvider provider;
    private final int[]                  neighborIds;
    private       int                    position;

    NeighborIdWalk(final GraphTraversalProvider provider, final int[] neighborIds) {
      this.provider = provider;
      this.neighborIds = neighborIds;
    }

    @Override
    protected boolean hasMoreEntries() {
      return position < neighborIds.length;
    }

    @Override
    protected Vertex wrapNextEntry() {
      final RID rid = provider.getRID(neighborIds[position++]);
      if (rid == null)
        return null;
      try {
        return new ArcadeVertex(graph, rid.asVertex());
      } catch (final RecordNotFoundException e) {
        // vertex deleted since CSR was built - skip
        return null;
      }
    }
  }

  @Override
  public String toString() {
    return StringFactory.vertexString(this);
  }
}
