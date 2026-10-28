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
package com.arcadedb.index.vector;

import com.arcadedb.database.RID;

import java.util.Set;

/**
 * The allow-list of a vector search whose members are decided one candidate at a time, by a predicate on the candidate's
 * record, instead of being listed up front: the {@code where} option of {@code vector.neighbors} (issue #10169) and the
 * {@code WHERE} of the Cypher {@code SEARCH} clause (issue #10782).
 * <p>
 * Its {@link #size()} is no count of what it accepts, so it never qualifies for a plan that enumerates the allow-list, and an
 * index asks it only {@link #contains}. The filtered walk keeps expanding until it holds {@code k} members or has met every
 * vector it can reach, so a walk it leaves short of {@code k} is short because the predicate accepts fewer records, not
 * because the graph is damaged: the scan that then completes the answer is reported as the expected cost of a selective
 * predicate, not as a graph to rebuild.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public interface PredicateRidSet extends Set<RID> {
}
