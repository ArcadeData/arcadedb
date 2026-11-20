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
package com.arcadedb.query.opencypher.procedures.meta;

import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.VertexType;
import com.arcadedb.security.SecurityHelper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * The labels view the {@code meta.*} procedures share: the vertex and edge types the caller may read, the labels each
 * vertex type answers to (a multi-label composite answers to each of its labels), and the labels of the node a RID names,
 * resolved from its bucket without loading it. A node of a type the caller may not read answers no labels, so its label
 * is not disclosed through the relationships that reach it.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
final class MetaLabels {
  private final Database                   database;
  private final List<VertexType>           vertexTypes = new ArrayList<>();
  private final List<EdgeType>             edgeTypes   = new ArrayList<>();
  private final Set<String>                edgeTypeNames = new HashSet<>();
  private final Map<String, List<String>>  labelsByType = new HashMap<>();
  private final Map<Integer, List<String>> labelsByBucket = new HashMap<>();

  MetaLabels(final Database database) {
    this.database = database;
    for (final DocumentType type : SecurityHelper.readableTypes(database)) {
      if (type instanceof VertexType vertexType) {
        vertexTypes.add(vertexType);
        labelsByType.put(type.getName(), Labels.getLabels(type));
      } else if (type instanceof EdgeType edgeType) {
        edgeTypes.add(edgeType);
        edgeTypeNames.add(edgeType.getName());
      }
    }
  }

  List<VertexType> vertexTypes() {
    return vertexTypes;
  }

  List<EdgeType> edgeTypes() {
    return edgeTypes;
  }

  /**
   * Whether the caller may read relationships of the type named {@code name}.
   */
  boolean isReadableEdgeType(final String name) {
    return edgeTypeNames.contains(name);
  }

  /**
   * The labels a vertex of {@code type} carries, sorted; empty for an unlabelled node or a type the caller may not read.
   */
  List<String> labelsOf(final DocumentType type) {
    final List<String> labels = labelsByType.get(type.getName());
    return labels != null ? labels : List.of();
  }

  /**
   * The labels of the node {@code rid} names, from its bucket's type.
   */
  List<String> labelsOf(final RID rid) {
    return labelsOfBucket(rid.getBucketId());
  }

  /**
   * The labels of a node stored in the bucket {@code bucketId}: its type's, empty when the bucket belongs to no type or to
   * one the caller may not read.
   */
  List<String> labelsOfBucket(final int bucketId) {
    return labelsByBucket.computeIfAbsent(bucketId, id -> {
      final DocumentType type = database.getSchema().getTypeByBucketId(id);
      return type != null ? labelsOf(type) : List.of();
    });
  }

  /**
   * Every label some readable vertex type carries, sorted.
   */
  Set<String> allLabels() {
    final Set<String> labels = new TreeSet<>();
    for (final List<String> typeLabels : labelsByType.values())
      labels.addAll(typeLabels);
    return labels;
  }

  /**
   * The readable vertex types whose vertices carry {@code label}.
   */
  List<VertexType> typesCarrying(final String label) {
    final List<VertexType> types = new ArrayList<>();
    for (final VertexType type : vertexTypes)
      if (labelsOf(type).contains(label))
        types.add(type);
    return types;
  }

  /**
   * The number of nodes carrying {@code label}: every vertex type that carries it counted once, so a multi-label node is
   * counted under each of its labels and never twice under one.
   */
  long countLabel(final String label) {
    long count = 0L;
    for (final VertexType type : typesCarrying(label))
      count += database.countType(type.getName(), false);
    return count;
  }

  /**
   * The {@code :`A`:`B`} spelling APOC gives a label set.
   */
  static String pattern(final List<String> labels) {
    final StringBuilder buffer = new StringBuilder();
    for (final String label : labels)
      buffer.append(":`").append(label).append('`');
    return buffer.toString();
  }
}
