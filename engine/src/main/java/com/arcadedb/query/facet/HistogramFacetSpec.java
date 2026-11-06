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

/**
 * A facet counting the values in regular buckets of a date or number property.
 *
 * @param timezone   the zone the buckets are aligned in, null for UTC
 * @param offset     the shift of the bucket boundaries, null for none
 * @param boundsFrom the lower extended bound: buckets exist from here, null for none
 * @param boundsTo   the upper extended bound, null for none
 */
public record HistogramFacetSpec(String alias, String property, Expression expression, WhereClause filter, HistogramInterval interval,
                                 String timezone, String offset, int minCount, Object boundsFrom, Object boundsTo) implements FacetSpec {
}
