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
package com.arcadedb.index.sparsevector;

import com.arcadedb.database.RID;

/**
 * Single (RID, score) pair returned from a top-K sparse-vector query. Exposes both public final
 * fields ({@code rid}, {@code score}) and accessor methods ({@link #rid()}, {@link #score()}).
 * <p>
 * The fields are the legacy surface used by SQL functions and external callers; new code should
 * prefer the methods so that we can switch to a record (or a class with read-only state) in a
 * future release without breaking source-compatibility for callers. The fields are
 * {@code @Deprecated} to advertise that migration path; they remain functional and there is no
 * planned removal yet.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class RidScore {
  /** @deprecated use {@link #rid()} instead; field will be made {@code private} in a future release. */
  @Deprecated
  public final RID   rid;
  /** @deprecated use {@link #score()} instead; field will be made {@code private} in a future release. */
  @Deprecated
  public final float score;

  public RidScore(final RID rid, final float score) {
    this.rid = rid;
    this.score = score;
  }

  public RID rid() {
    return rid;
  }

  public float score() {
    return score;
  }

  @Override
  public String toString() {
    return rid + ":" + score;
  }
}
