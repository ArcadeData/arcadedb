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
package com.arcadedb.opencypher.ast;

import com.arcadedb.graph.Vertex;

/**
 * Represents the direction of a relationship in a graph pattern.
 * Maps to ArcadeDB's Vertex.DIRECTION enum.
 */
public enum Direction {
  /** Outgoing relationship: (a)-[r]->(b) */
  OUT,

  /** Incoming relationship: (a)<-[r]-(b) */
  IN,

  /** Bidirectional relationship: (a)-[r]-(b) */
  BOTH;

  /**
   * Converts this direction to ArcadeDB's Vertex.DIRECTION enum.
   *
   * @return corresponding Vertex.DIRECTION value
   */
  public Vertex.DIRECTION toArcadeDirection() {
    return switch (this) {
      case OUT -> Vertex.DIRECTION.OUT;
      case IN -> Vertex.DIRECTION.IN;
      case BOTH -> Vertex.DIRECTION.BOTH;
    };
  }

  /**
   * Returns the opposite direction.
   *
   * @return opposite direction (OUT<->IN, BOTH->BOTH)
   */
  public Direction reverse() {
    return switch (this) {
      case OUT -> IN;
      case IN -> OUT;
      case BOTH -> BOTH;
    };
  }
}
