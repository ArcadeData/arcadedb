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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.graph.LightEdge;
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
  private static final byte LIVE              = 0;
  private static final byte REMOVED_IN_TX     = 1;
  private static final byte REMOVED_COMMITTED = 2;

  /**
   * Set by {@link #remove()} (issue #10196): a traversal keeps the element it dropped - {@code sideEffect(drop())} passes it on -
   * and its in-memory record still holds the properties and the edge lists it had, so without this every later step read and
   * wrote a record that no longer exists. A delete still in its transaction can be rolled back, which brings the element back;
   * one committed never does, even when a later record reuses its RID.
   */
  private volatile byte       removal = LIVE;

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
    // A REMOVED ELEMENT HAS NO PROPERTIES, AS IN TINKERGRAPH
    final V value = isRemoved() ? null : (V) baseElement.get(key);
    if (value == null)
      throw Property.Exceptions.propertyDoesNotExist(this, key);
    return value;
  }

  @Override
  public <V> Iterator<V> values(final String... propertyKeys) {
    if (isRemoved())
      return Collections.emptyIterator();
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
    removal = REMOVED_IN_TX;
    // A ROLLBACK DISCARDS THE CALLBACK, A COMMIT MAKES THE REMOVAL FINAL
    if (graph.getDatabase() instanceof DatabaseInternal database) {
      final TransactionContext tx = database.getTransactionIfExists();
      if (tx != null)
        tx.addAfterCommitCallback(() -> removal = REMOVED_COMMITTED);
    }
  }

  /**
   * Whether this element was removed, the way TinkerGraph answers it: reads of a removed element find no properties, no edges and no
   * adjacent vertices, and writes to it are refused (issue #10196). An element removed through another instance of it - the edges a
   * vertex delete took with it, a vertex looked up twice - is known from the transaction's deleted set, one hash probe. An element
   * removed through this instance is removed for good once the delete committed; while it is in flight, a delete the transaction no
   * longer holds was rolled back, and the element is usable again. A lightweight edge has no record the transaction tracks: one
   * removed through this instance stays removed.
   */
  protected boolean isRemoved() {
    final byte state = removal;
    if (state == REMOVED_COMMITTED)
      return true;
    if (state == LIVE)
      // NO PROBE UNTIL THE GRAPH HAS REMOVED SOMETHING: A READ-ONLY TRAVERSAL PAYS A FIELD READ PER ELEMENT ACCESS
      return graph.hasRemovedElements() && isDeletedInTransaction();
    final RID rid = baseElement.getIdentity();
    if (rid == null || baseElement instanceof LightEdge || isDeletedInTransaction())
      return true;
    if (!(graph.getDatabase() instanceof DatabaseInternal)) {
      // A REMOTE DATABASE HAS NO LOCAL TRANSACTION TO ASK: ITS RECORD IS, AND A RECORD STILL THERE MEANS THE DROP WAS ROLLED BACK
      if (!graph.getDatabase().existsRecord(rid))
        return true;
    }
    removal = LIVE;
    return false;
  }

  /**
   * Refuses a write to a removed element with the exception TinkerGraph raises for it.
   */
  protected void checkNotRemoved() {
    // A WRITE ALWAYS ASKS THE TRANSACTION: THE ELEMENT MAY HAVE BEEN DELETED BY A STATEMENT RATHER THAN THROUGH THE GRAPH
    if (isRemoved() || (removal == LIVE && isDeletedInTransaction()))
      throw elementAlreadyRemoved(this instanceof Vertex ? Vertex.class : Edge.class, id());
  }

  /** The refusal TinkerGraph raises, in its words: TinkerPop's core no longer provides a factory for it. */
  private static IllegalStateException elementAlreadyRemoved(final Class<? extends Element> type, final Object id) {
    return new IllegalStateException(type.getSimpleName() + " with id " + id + " was removed.");
  }

  private boolean isDeletedInTransaction() {
    final RID rid = baseElement.getIdentity();
    if (rid == null || !(graph.getDatabase() instanceof DatabaseInternal database))
      return false;
    final TransactionContext tx = database.getTransactionIfExists();
    return tx != null && tx.isDeletedInTransaction(rid);
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
    // The key and value come from the traversal, not from the record read: no stale-read refusal (#8610, #8707)
    mutableElement.clearBasedOnStaleRead();

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
