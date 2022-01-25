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
package com.arcadedb.database;

import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;

/**
 * Interface to identify an object in the database.
 *
 * @author Luca Garulli
 */
public interface Identifiable {
  /**
   * Returns the RID (Record ID) for the current object.
   *
   * @return the {@link RID}
   */
  RID getIdentity();

  /**
   * Returns the generic record.
   *
   * @param loadContent specifies if pre-load the record content
   *
   * @return the {@link Record}
   */
  Record getRecord(boolean loadContent);

  /**
   * Returns the generic record by pre-loading also its content.
   *
   * @return the {@link Record}
   */
  Record getRecord();

  /**
   * Returns the document record. If the record is not a document, a UnsupportedOperationException exception is thrown.
   *
   * @return the {@link Document}
   */
  Document asDocument();

  /**
   * Returns the document record. If the record is not a document, a UnsupportedOperationException exception is thrown.
   *
   * @param loadContent specifies if pre-load the record content
   *
   * @return the {@link Document}
   */
  Document asDocument(final boolean loadContent);

  /**
   * Returns the vertex record. If the record is not a vertex, a UnsupportedOperationException exception is thrown.
   *
   * @return the {@link Vertex}
   */
  Vertex asVertex();

  /**
   * Returns the vertex record. If the record is not a vertex, a UnsupportedOperationException exception is thrown.
   *
   * @param loadContent specifies if pre-load the record content
   *
   * @return the {@link Vertex}
   */
  Vertex asVertex(boolean loadContent);

  /**
   * Returns the edge record. If the record is not an edge, a UnsupportedOperationException exception is thrown.
   * *
   *
   * @return the {@link Edge}
   */
  Edge asEdge();

  /**
   * Returns the edge record. If the record is not an edge, a UnsupportedOperationException exception is thrown.
   *
   * @param loadContent specifies if pre-load the record content
   *
   * @return the {@link Edge}
   */
  Edge asEdge(boolean loadContent);
}
