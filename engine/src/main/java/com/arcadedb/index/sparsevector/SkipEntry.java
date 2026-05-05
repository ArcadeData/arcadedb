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
 * One entry of a per-dim skip list. The skip list compresses repeated block-max scans into
 * O(log B) lookups: given a target RID, binary-search by {@link #firstRid()} to find the
 * covering entry, then read {@link #maxWeightToEnd()} as a BMW upper bound on what the
 * remaining tail of the posting list can contribute to any score.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public record SkipEntry(RID firstRid, float maxWeightToEnd, int blockIndex) {
}
