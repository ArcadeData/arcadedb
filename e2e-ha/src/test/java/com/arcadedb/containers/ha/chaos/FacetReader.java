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

package com.arcadedb.containers.ha.chaos;

import com.arcadedb.test.support.FacetSnapshot;

import java.io.IOException;
import java.util.SortedMap;

/**
 * Reads, from one node, the {@code GROUP BY} truth and the facet answer over the same assets, for the same filter.
 */
public interface FacetReader {
  /**
   * @param where a plain filter from {@code FacetSchema.CHECKED_FILTERS}, empty for none
   *
   * @return the counts computed with {@code GROUP BY}, in the key format of {@link FacetSnapshot}
   */
  SortedMap<String, Long> groupBy(int node, String where) throws IOException;

  /**
   * @return the parsed answer of the {@code FACETS} query over the same assets
   */
  FacetSnapshot facets(int node, String where) throws IOException;
}
