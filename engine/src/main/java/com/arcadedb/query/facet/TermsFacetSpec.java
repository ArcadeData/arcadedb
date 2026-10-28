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
package com.arcadedb.query.facet;

import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.WhereClause;

import java.util.List;

/**
 * A facet counting the occurrences of each value.
 *
 * @param include       the only values to return, even at count 0; null or empty when all are candidates
 * @param missingBucket true to add the bucket of the records without a value
 * @param after         the cursor of the AFTER option, null when absent
 */
public record TermsFacetSpec(String alias, String property, Expression expression, WhereClause filter, int top, int minCount, FacetOrder order,
                             List<Object> include, boolean missingBucket, Object after) implements FacetSpec {
}
