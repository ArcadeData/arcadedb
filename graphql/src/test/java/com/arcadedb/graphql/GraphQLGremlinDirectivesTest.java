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
package com.arcadedb.graphql;

import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.ResultSet;

public class GraphQLGremlinDirectivesTest extends AbstractGraphQLNativeLanguageDirectivesTest {
  @Override
  protected void defineTypes(final Database database) {
    super.defineTypes(database);
    ResultSet command = database.command("graphql", "type Query {\n" +//
            "  bookById(id: String): Book\n" +//
            "  bookByName(bookNameParameter: String): Book @gremlin(statement: \"g.V().has('name', bookNameParameter)\")\n" +//
            "}");
  }
}
