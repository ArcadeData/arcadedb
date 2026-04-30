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
package com.arcadedb.serializer;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;

/**
 * Test-only entry point for serializer internals. Lives in {@code src/test/java} under the
 * {@code com.arcadedb.serializer} package so it can reach the package-private write path
 * {@link BinarySerializer#writeExternalPropertyValue} without that method having to be public on the
 * production API surface. Use only from regression tests.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class BinarySerializerTestHelper {
  private BinarySerializerTestHelper() {
  }

  /**
   * Writes a value blob directly into a paired external bucket, bypassing the property write path. Used by
   * orphan-cleanup tests to plant a record that no primary record references.
   */
  public static RID injectOrphanExternalRecord(final BinarySerializer serializer, final DatabaseInternal database,
      final int externalBucketId, final byte valueType, final Object value, final String compression) {
    return serializer.writeExternalPropertyValue(database, externalBucketId, null, valueType, value, compression).rid;
  }
}
