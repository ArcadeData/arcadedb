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
package com.arcadedb.index.columnar;

/**
 * A schema or maintenance event that reaches the columnar projections without dirty pages (spec 6.4): produced by
 * {@link ColumnarReconciler} from the difference between two schema states, or sent directly (a {@code check(fix=true)} repair), and
 * delivered by {@link ColumnarIndexManager#onSchemaEvent} to every {@link ColumnarIndexManager#addSchemaEventListener listener}.
 *
 * @param kind         what changed
 * @param bucketFileId the file id of the projected bucket, -1 for a {@link Kind#HIERARCHY_SOURCE_CHANGED} event
 * @param indexFileId  the file id of the COLUMNAR index of the column, -1 when the event is about the whole bucket or a hierarchy type
 * @param reason       the rebuild reason ({@code create}, {@code schema}, {@code check_fix}, ...), null when nothing is rebuilt
 * @param typeName     the type a {@link Kind#HIERARCHY_SOURCE_CHANGED} event is about, null for the other kinds
 */
public record ColumnarSchemaEvent(Kind kind, int bucketFileId, int indexFileId, String reason, String typeName) {
  /** The rebuild reason of a bucket a {@code check(fix=true)} repaired ({@code DatabaseChecker}). */
  public static final String CHECK_FIX = "check_fix";

  public enum Kind {
    /** The bucket has no COLUMNAR column any more (its last index, its type or the bucket was dropped): its projection is dropped. */
    DROP_PROJECTION,
    /** One COLUMNAR index of the bucket was dropped; the projection keeps the others. */
    DROP_COLUMN,
    /** A new COLUMNAR index on a bucket with records: built by a {@code create} rebuild unless one already covers it. */
    ADD_COLUMN,
    /** Same index, another property type: the column is rebuilt with reason {@code schema}. */
    RETYPE_COLUMN,
    /** Same index, another property name (or another bucket or type name): definition only, the data is kept. */
    RENAME,
    /** A new projected bucket with no record: its projection is usable at once, nothing is rebuilt. */
    CREATE_EMPTY,
    /** The bucket's projection is rebuilt with {@link #reason()} ({@code check_fix} after a repair). */
    REBUILD,
    /** A type a COLUMNAR hierarchy reads ({@code vertexType} or {@code parentEdge}) changed; no projection changes (plan 5). */
    HIERARCHY_SOURCE_CHANGED
  }

  /** A {@link Kind#REBUILD} of the bucket's projection with the reason. */
  public static ColumnarSchemaEvent rebuild(final int bucketFileId, final String reason) {
    return new ColumnarSchemaEvent(Kind.REBUILD, bucketFileId, -1, reason, null);
  }
}
