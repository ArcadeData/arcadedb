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
 * A facet counting the values in each of a list of ranges.
 *
 * @param defaultLabel the label of the bucket of the values in no range, null for no such bucket
 */
public record RangeFacetSpec(String alias, String property, Expression expression, WhereClause filter, List<RangeBound> ranges,
                             String defaultLabel) implements FacetSpec {
}
