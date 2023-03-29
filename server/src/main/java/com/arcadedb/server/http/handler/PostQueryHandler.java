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
package com.arcadedb.server.http.handler;

import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.http.HttpServer;

import java.util.*;

public class PostQueryHandler extends PostCommandHandler {
  public PostQueryHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  protected ResultSet executeCommand(final Database database, final String language, final String command, final Map<String, Object> paramMap) {
    final Object params = mapParams(paramMap);

    if (params instanceof Object[])
      return database.query(language, command, (Object[]) params);

    return database.query(language, command, (Map<String, Object>) params);
  }
}
