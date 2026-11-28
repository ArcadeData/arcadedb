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
package com.arcadedb.query.opencypher.executor;

import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.DeletedEntityException;
import com.arcadedb.graph.Edge;
import com.arcadedb.query.sql.executor.Result;

import java.util.Objects;

/**
 * Marker object placed in results to indicate a deleted entity.
 * When a DELETE clause removes a node or relationship, the variable is replaced
 * with this marker. Subsequent property access or function calls (labels)
 * on the marker will throw DeletedEntityAccess errors per the OpenCypher spec.
 * type() is allowed on deleted relationships, and id() / elementId() answer the id the entity had, as Neo4j does
 * (issue #13582).
 */
public final class DeletedEntityMarker {
  /** A deleted value that is no node nor relationship (a path), whose identity is unknown. */
  public static final DeletedEntityMarker INSTANCE = new DeletedEntityMarker(null, null);

  private final RID    identity;
  private final String relationshipType;

  /**
   * @param identity         the RID the deleted entity had, null when unknown
   * @param relationshipType the type of a deleted relationship, null for a node
   */
  public DeletedEntityMarker(final RID identity, final String relationshipType) {
    this.identity = identity;
    this.relationshipType = relationshipType;
  }

  /**
   * @return the marker of {@code value}, a node or a relationship the statement deleted: it keeps the identity and, for a
   * relationship, the type. {@link #INSTANCE} for anything else
   */
  public static DeletedEntityMarker of(final Object value) {
    if (value instanceof DeletedEntityMarker marker)
      return marker;
    if (value instanceof Edge edge)
      return new DeletedEntityMarker(edge.getIdentity(), edge.getTypeName());
    if (value instanceof Identifiable identifiable)
      return new DeletedEntityMarker(identifiable.getIdentity(), null);
    if (value instanceof Result result && result.isElement())
      return of(result.getElement().orElse(null));
    return INSTANCE;
  }

  /** @return the RID the deleted entity had, null when unknown */
  public RID getIdentity() {
    return identity;
  }

  public String getRelationshipType() {
    return relationshipType;
  }

  public static void checkNotDeleted(final Object obj) {
    if (obj instanceof DeletedEntityMarker)
      throw new DeletedEntityException("DeletedEntityAccess: Cannot access a deleted entity");
  }

  /**
   * Two markers of the same deleted record are equal. A marker whose identity is unknown, or is the type marker every
   * lightweight edge of its type shares (no record, negative position), equals only itself.
   */
  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    return o instanceof DeletedEntityMarker other && isRecordIdentity() && identity.equals(other.identity)
        && Objects.equals(relationshipType, other.relationshipType);
  }

  @Override
  public int hashCode() {
    return isRecordIdentity() ? identity.hashCode() : System.identityHashCode(this);
  }

  private boolean isRecordIdentity() {
    return identity != null && identity.getPosition() >= 0;
  }

  @Override
  public String toString() {
    return "<deleted>";
  }
}
