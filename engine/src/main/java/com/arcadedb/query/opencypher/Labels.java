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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Utility class for multi-label support in ArcadeDB.
 * <p>
 * Provides methods to manage composite types for vertices with multiple labels.
 * When a vertex has multiple labels (e.g., Person, Developer), a composite type
 * is automatically created (Developer~Person) that extends all label types.
 * <p>
 * Labels are sorted alphabetically to ensure consistent naming regardless of
 * the order in which labels are specified.
 * <p>
 * The tilde (~) separator was chosen because it:
 * <ul>
 *   <li>Is rarely used in type/class names by users</li>
 *   <li>Is valid in SQL identifiers (can be quoted with backticks if needed)</li>
 *   <li>Visually suggests "combining" or "together"</li>
 *   <li>Does not conflict with common naming conventions (unlike underscore)</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class Labels {

  /**
   * Separator used between label names in composite type names.
   * Uses tilde (~) to avoid conflicts with user type names that may contain underscores.
   */
  public static final String LABEL_SEPARATOR = "~";

  /**
   * Private constructor to prevent instantiation.
   */
  private Labels() {
  }

  /**
   * Generates composite type name from labels (deduplicated and sorted alphabetically).
   * <p>
   * Duplicate labels are automatically removed, as labels represent set membership
   * (a node either has a label or it doesn't - specifying the same label multiple
   * times is redundant). This matches Neo4j's behavior.
   * <p>
   * Examples:
   * <ul>
   *   <li>["Person"] → "Person"</li>
   *   <li>["Person", "Developer"] → "Developer~Person"</li>
   *   <li>["Developer", "Person"] → "Developer~Person" (same, sorted)</li>
   *   <li>["A", "B", "C"] → "A~B~C"</li>
   *   <li>["Person", "Kebab", "Person"] → "Kebab~Person" (duplicate removed)</li>
   *   <li>["Kebab", "Kebab"] → "Kebab" (all duplicates removed)</li>
   * </ul>
   *
   * @param labels list of label names (duplicates are ignored)
   * @return composite type name, or "V" if labels is null/empty
   */
  public static String getCompositeTypeName(final List<String> labels) {
    if (labels == null || labels.isEmpty())
      return "V";
    if (labels.size() == 1)
      return labels.get(0);

    // Use TreeSet to both deduplicate and sort alphabetically
    final Set<String> uniqueSorted = new TreeSet<>(labels);

    // After deduplication, if only one unique label, return it directly
    if (uniqueSorted.size() == 1)
      return uniqueSorted.iterator().next();

    return String.join(LABEL_SEPARATOR, uniqueSorted);
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
    final String typeName = type.getName();

    // If the vertex was created without labels, it has the base "Vertex" type
    // In Cypher, unlabeled nodes have an empty label list
    if ("Vertex".equals(typeName) || "V".equals(typeName))
      return List.of();

    final List<DocumentType> superTypes = type.getSuperTypes();

    if (superTypes.isEmpty()) {
      // Single-label vertex - type name is the label
      return List.of(typeName);
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
   * composite type Developer~Person will return true for both "Developer"
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
   * If a single label is provided (or after deduplication only one unique label remains),
   * creates/returns that type directly. If multiple unique labels are provided,
   * ensures all base types exist and creates a composite type that extends all of them.
   * <p>
   * Duplicate labels are automatically ignored, matching Neo4j's behavior
   * (GitHub issue #3264).
   *
   * @param schema the database schema
   * @param labels list of labels for the vertex (duplicates are ignored)
   * @return the type name to use (composite type name if multiple unique labels)
   */
  public static String ensureCompositeType(final Schema schema, final List<String> labels) {
    if (labels == null || labels.isEmpty())
      return "V";

    // Deduplicate and sort labels using TreeSet
    final Set<String> uniqueLabels = new TreeSet<>(labels);

    // Handle single label case (including after deduplication)
    if (uniqueLabels.size() == 1) {
      final String label = uniqueLabels.iterator().next();
      schema.getOrCreateVertexType(label);
      return label;
    }

    final String compositeTypeName = String.join(LABEL_SEPARATOR, uniqueLabels);

    // Always ensure all base types exist first, regardless of whether
    // the composite type already exists. This fixes GitHub issue #3266
    // where pre-existing composite types would prevent label type creation.
    for (final String label : uniqueLabels) {
      schema.getOrCreateVertexType(label);
    }

    if (!schema.existsType(compositeTypeName)) {
      // Create composite type extending all labels
      // Use the builder to add multiple supertypes
      var builder = schema.buildVertexType()
          .withName(compositeTypeName);

      // Add each unique label as a supertype
      for (final String label : uniqueLabels) {
        builder = builder.withSuperType(label);
      }

      builder.create();
    } else {
      // Composite type already exists - ensure it has correct supertypes
      // This handles the case where the type was created manually without inheritance
      final var existingType = schema.getType(compositeTypeName);
      for (final String label : uniqueLabels) {
        if (!existingType.instanceOf(label)) {
          existingType.addSuperType(label);
        }
      }
    }

    return compositeTypeName;
  }

  /**
   * Checks if a type name appears to be a composite type name.
   * <p>
   * This is a heuristic check based on the presence of the label separator (~).
   * Type names with underscores are no longer falsely detected as composite types.
   *
   * @param typeName the type name to check
   * @return true if the type name contains the label separator
   */
  public static boolean isCompositeTypeName(final String typeName) {
    return typeName != null && typeName.contains(LABEL_SEPARATOR);
  }
}
