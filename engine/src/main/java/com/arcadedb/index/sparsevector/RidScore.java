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
 * Single (RID, score) pair returned from a top-K sparse-vector query. Immutable value carrier;
 * the previous class with deprecated public fields was migrated to a {@code record} now that
 * every caller has been updated to use the {@link #rid()} / {@link #score()} accessors.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public record RidScore(RID rid, float score) {
  @Override
  public String toString() {
    return rid + ":" + score;
  }
}
