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
package com.arcadedb.graphql.schema;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.exception.CommandParsingException;

/**
 * The size of the answer to one introspection query, counted in entries and bounded by
 * {@link GlobalConfiguration#GRAPHQL_MAX_INTROSPECTION_ENTRIES}. The answer is built in memory and every alias of a selection is
 * answered again under its own response key, so its size is the product of the aliases and fragment spreads written at each level,
 * not just the size of the schema. One instance serves one query and is used by one thread.
 * <p>
 * One entry is one object built, one selection walked (by the expansion of its fragments, or by the object that answers it) and one
 * type or field skipped. The work of a query is charged before it is done, so a query past the limit stops at the entry that
 * passes it, whatever the document asks for beyond.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
final class IntrospectionBudget {
  private final long limit;
  private       long remaining;

  /**
   * @param limit the entries one query may take; 0 or less for no limit
   */
  private IntrospectionBudget(final long limit) {
    this.limit = limit;
    this.remaining = limit > 0 ? limit : Long.MAX_VALUE;
  }

  /**
   * The budget of a query of this database, which reads the limit from the configuration of the database.
   */
  static IntrospectionBudget of(final Database database) {
    return new IntrospectionBudget(database.getConfiguration().getValueAsInteger(GlobalConfiguration.GRAPHQL_MAX_INTROSPECTION_ENTRIES));
  }

  /**
   * Takes {@code entries} from the budget.
   *
   * @throws CommandParsingException when the query has taken more than the limit
   */
  void charge(final int entries) {
    remaining -= entries;
    if (remaining < 0)
      throw new CommandParsingException("Introspection query selects more than " + limit + " entries, see the setting '"
          + GlobalConfiguration.GRAPHQL_MAX_INTROSPECTION_ENTRIES.getKey() + "'");
  }
}
