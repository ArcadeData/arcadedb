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
package com.arcadedb.bolt.structure;

import com.arcadedb.bolt.packstream.PackStreamStructure;
import com.arcadedb.bolt.packstream.PackStreamWriter;

import java.io.IOException;
import java.util.List;

/**
 * BOLT Path structure representing a graph path.
 * Structure signature: 0x50
 * Fields: nodes (List<Node>), rels (List<UnboundRelationship>), indices (List<Integer>)
 *
 * The indices array describes the path by referencing nodes and rels:
 * - Positive index i means traverse rel[i-1] forward to node[i]
 * - Negative index -i means traverse rel[i-1] backward to node[i]
 */
public class BoltPath implements PackStreamStructure {
  public static final byte SIGNATURE = 0x50;

  private final List<BoltNode>                nodes;
  private final List<BoltUnboundRelationship> relationships;
  private final List<Long>                    indices;

  public BoltPath(final List<BoltNode> nodes, final List<BoltUnboundRelationship> relationships, final List<Long> indices) {
    this.nodes = nodes != null ? nodes : List.of();
    this.relationships = relationships != null ? relationships : List.of();
    this.indices = indices != null ? indices : List.of();
  }

  @Override
  public byte getSignature() {
    return SIGNATURE;
  }

  @Override
  public int getFieldCount() {
    return 3;
  }

  @Override
  public void writeTo(final PackStreamWriter writer) throws IOException {
    writer.writeStructureHeader(SIGNATURE, 3);

    // Write nodes list
    writer.writeListHeader(nodes.size());
    for (final BoltNode node : nodes) {
      node.writeTo(writer);
    }

    // Write unbound relationships list
    writer.writeListHeader(relationships.size());
    for (final BoltUnboundRelationship rel : relationships) {
      rel.writeTo(writer);
    }

    // Write indices list
    writer.writeList(indices);
  }

  public List<BoltNode> getNodes() {
    return nodes;
  }

  public List<BoltUnboundRelationship> getRelationships() {
    return relationships;
  }

  public List<Long> getIndices() {
    return indices;
  }

  @Override
  public String toString() {
    return "Path{nodes=" + nodes.size() + ", relationships=" + relationships.size() + ", indices=" + indices + "}";
  }
}
