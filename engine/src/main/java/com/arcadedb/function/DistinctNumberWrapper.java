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
package com.arcadedb.function;

/**
 * Wraps a {@link Number} for insertion into a {@link java.util.Set} used to eliminate duplicates
 * (e.g. {@code collect(DISTINCT ...)}), keying membership on {@link DistinctNumericKey#canonicalize}
 * while preserving the original value for retrieval. This keeps the first-encountered boxed numeric
 * type (e.g. an {@code Integer} vs. a {@code Double}) in the output, while still deduplicating
 * against later numerically-equal values of a different boxed type. See issue #5789.
 */
public final class DistinctNumberWrapper {
  private final Object original;
  private final Object canonicalKey;

  public DistinctNumberWrapper(final Object original) {
    this.original = original;
    this.canonicalKey = DistinctNumericKey.canonicalize(original);
  }

  public Object getOriginal() {
    return original;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj)
      return true;
    if (!(obj instanceof DistinctNumberWrapper other))
      return false;
    return canonicalKey.equals(other.canonicalKey);
  }

  @Override
  public int hashCode() {
    return canonicalKey.hashCode();
  }
}
