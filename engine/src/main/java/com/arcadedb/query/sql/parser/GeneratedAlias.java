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
package com.arcadedb.query.sql.parser;

/**
 * The one spelling every alias the planner invents for itself has to start with.
 * <p>
 * The planner rewrites a statement into one the executor can run, and in doing so it names things the user never
 * wrote: the LET a lifted sub-query is moved into ({@link SubQueryCollector}), the projection an aggregate is split
 * across ({@link AggregateProjectionSplit}), the column an ORDER BY or GROUP BY term is materialized as. Those names
 * then travel with the query and have to be resolved again at execution time - and the code doing the resolving has to
 * tell them apart from a property the record might actually own, which it can only do by the prefix. Hence one
 * constant rather than four independent string literals: an alias generator that picked its own prefix would compile,
 * run, and silently resolve to nothing (issue #7054).
 * <p>
 * The prefix is deliberately unspellable as a property name in practice, so the test is a prefix match and nothing
 * more.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GeneratedAlias {
  /** Every planner-generated alias begins with this; nothing a user can write does. */
  public static final String PREFIX = "_$$$";

  private GeneratedAlias() {
  }

  /** Whether {@code name} is an alias the planner generated for itself rather than a name the user wrote. */
  public static boolean is(final String name) {
    return name != null && name.startsWith(PREFIX);
  }
}
