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
package com.arcadedb.query.opencypher;

import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;

import java.util.*;

/**
 * Utility class for multi-label support in ArcadeDB.
 * <p>
 * Provides methods to manage composite types for vertices with multiple labels.
 * When a vertex has multiple labels (e.g., Person, Developer), a composite type
 * is automatically created (Developer_Person) that extends all label types.
 * <p>
 * Labels are sorted alphabetically to ensure consistent naming regardless of
 * the order in which labels are specified.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class Labels {

  /**
   * Separator used between label names in composite type names.
   */
  public static final String LABEL_SEPARATOR = "_";

  /**
   * Private constructor to prevent instantiation.
   */
  private Labels() {
  }

  /**
   * Generates composite type name from labels (sorted alphabetically).
   * <p>
   * Examples:
   * <ul>
   *   <li>["Person"] → "Person"</li>
   *   <li>["Person", "Developer"] → "Developer_Person"</li>
   *   <li>["Developer", "Person"] → "Developer_Person" (same, sorted)</li>
   *   <li>["A", "B", "C"] → "A_B_C"</li>
   * </ul>
   *
   * @param labels list of label names
   * @return composite type name, or "V" if labels is null/empty
   */
  public static String getCompositeTypeName(final List<String> labels) {
    if (labels == null || labels.isEmpty())
      return "V";
    if (labels.size() == 1)
      return labels.get(0);

    final List<String> sorted = new ArrayList<>(labels);
    Collections.sort(sorted);
    return String.join(LABEL_SEPARATOR, sorted);
  }

  /**
   * Gets all labels for a vertex.
   * <p>
   * For composite types (types with multiple supertypes), returns the supertype names
   * as the labels. For single-label vertices, returns the type name as the sole label.
   *
   * @param vertex the vertex to get labels from
   * @return list of labels (sorted alphabetically for composite types)
   */
  public static List<String> getLabels(final Vertex vertex) {
    final DocumentType type = vertex.getType();
    final List<DocumentType> superTypes = type.getSuperTypes();

    if (superTypes.isEmpty()) {
      // Single-label vertex - type name is the label
      return List.of(type.getName());
    }

    // Multi-label vertex - supertypes are the labels
    final List<String> labels = new ArrayList<>(superTypes.size());
    for (final DocumentType superType : superTypes)
      labels.add(superType.getName());
    Collections.sort(labels);
    return labels;
  }

  /**
   * Checks if a vertex has a specific label.
   * <p>
   * Uses type inheritance (instanceOf) for checking, so a vertex with
   * composite type Developer_Person will return true for both "Developer"
   * and "Person" labels.
   *
   * @param vertex the vertex to check
   * @param label  the label to look for
   * @return true if the vertex has the specified label
   */
  public static boolean hasLabel(final Vertex vertex, final String label) {
    return vertex.getType().instanceOf(label);
  }

  /**
   * Ensures composite type exists, creating it if necessary.
   * Returns the type name to use for creating vertices.
   * <p>
   * If a single label is provided, creates/returns that type directly.
   * If multiple labels are provided, ensures all base types exist and
   * creates a composite type that extends all of them.
   *
   * @param schema the database schema
   * @param labels list of labels for the vertex
   * @return the type name to use (composite type name if multiple labels)
   */
  public static String ensureCompositeType(final Schema schema, final List<String> labels) {
    if (labels == null || labels.isEmpty())
      return "V";

    if (labels.size() == 1) {
      final String label = labels.get(0);
      schema.getOrCreateVertexType(label);
      return label;
    }

    final String compositeTypeName = getCompositeTypeName(labels);

    if (!schema.existsType(compositeTypeName)) {
      // Ensure all base types exist first
      for (final String label : labels) {
        schema.getOrCreateVertexType(label);
      }

      // Create composite type extending all labels
      // Use the builder to add multiple supertypes
      var builder = schema.buildVertexType()
          .withName(compositeTypeName);

      // Add each label as a supertype
      for (final String label : labels) {
        builder = builder.withSuperType(label);
      }

      builder.create();
    }

    return compositeTypeName;
  }

  /**
   * Checks if a type name appears to be a composite type name.
   * <p>
   * This is a heuristic check based on the presence of the label separator.
   * Note: This may return true for single-label types that contain underscores
   * in their name.
   *
   * @param typeName the type name to check
   * @return true if the type name contains the label separator
   */
  public static boolean isCompositeTypeName(final String typeName) {
    return typeName != null && typeName.contains(LABEL_SEPARATOR);
  }
}
