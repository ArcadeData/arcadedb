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

import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * The one place a Gremlin label is resolved against the schema: a type name, or a {@code bucket:<name>} label naming one bucket.
 * Every step and every write that takes a label calls it, so the answer to a label cannot depend on which step happens to read it:
 * {@code hasLabel(X)} and {@code hasLabel(X).count()} used to resolve a missing bucket each their own way, and only one of them
 * guarded it (issue #10763).
 * <p>
 * A read never throws for a label that names nothing: an earlier step of the same traversal may still create the type or the bucket
 * (issue #9335), so a label is resolved when the step runs and matches nothing until then. A write refuses such a label by name.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class ArcadeLabels {
  /** A label with this prefix names one bucket rather than a type. */
  static final String BUCKET_PREFIX = "bucket:";

  private ArcadeLabels() {
  }

  /** The bucket a {@code bucket:} label names, or null for a type label. */
  static String bucketName(final String label) {
    return label.startsWith(BUCKET_PREFIX) ? label.substring(BUCKET_PREFIX.length()) : null;
  }

  /**
   * The type a label matches now, or null when it matches nothing: an absent type, an absent bucket, a bucket no type owns.
   * {@link Schema#getTypeByBucketName(String)} throws for an absent bucket rather than answering null, so it is asked only for a
   * bucket that exists.
   */
  static DocumentType typeOf(final Schema schema, final String label) {
    final String bucketName = bucketName(label);
    if (bucketName == null)
      return schema.getTypeOrNull(label);
    return schema.getBucketByNameIfExists(bucketName) == null ? null : schema.getTypeByBucketName(bucketName);
  }

  /**
   * The type a label matches inside the element kind of a traversal, or null: {@code hasLabel()} filters inside the current kind, so
   * on {@code g.V()} an edge type or the bucket of one matches nothing, and vice versa (issue #5223).
   */
  static DocumentType typeOf(final Schema schema, final String label, final Class<?> elementClass) {
    final DocumentType type = typeOf(schema, label);
    if (type == null)
      return null;
    final boolean matchesKind = Vertex.class.isAssignableFrom(elementClass) ? type instanceof VertexType : type instanceof EdgeType;
    return matchesKind ? type : null;
  }

  /**
   * The name of the type a record written with this label goes to. A type label is answered as it is, because the write creates
   * a type that does not exist yet; a bucket cannot be created by a write, so a {@code bucket:} label naming a bucket that does not
   * exist, or that no type owns, is refused.
   */
  static String typeNameForWrite(final Schema schema, final String label) {
    final String bucketName = bucketName(label);
    if (bucketName == null)
      return label;

    if (schema.getBucketByNameIfExists(bucketName) == null)
      throw new IllegalArgumentException("Bucket '" + bucketName + "' does not exist");
    final DocumentType type = schema.getTypeByBucketName(bucketName);
    if (type == null)
      throw new IllegalArgumentException("Bucket '" + bucketName + "' does not belong to any type");
    return type.getName();
  }
}
