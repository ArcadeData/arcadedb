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
package com.arcadedb.mongo;

import com.arcadedb.query.ProfiledLimitScope;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import de.bwaldvogel.mongo.bson.Document;

import java.util.Iterator;
import java.util.Map;

/**
 * The rows of the {@code mongo} query language: the documents of a {@link MongoFindCursor}, read from it as the caller asks for them,
 * so a caller that stops early (a row limit of the HTTP endpoints, a client that goes away) never reads, converts or holds the rows
 * past the ones it took. The cursor is released when the last row is taken, and by {@link #close()}.
 * <p>
 * A row is read inside the {@link ProfiledLimitScope}, as the rows of a {@code find} are: the {@code resultSetLimit} of the user's
 * groups was applied by the query when it opened the cursor, and no statement the read runs takes it again.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
final class MongoCursorResultSet implements ResultSet {
  private final MongoFindCursor    cursor;
  private final Iterator<Document> documents;

  MongoCursorResultSet(final MongoFindCursor cursor) {
    this.cursor = cursor;
    this.documents = cursor.documents();
  }

  @Override
  public boolean hasNext() {
    ProfiledLimitScope.enter();
    try {
      return documents.hasNext();
    } finally {
      ProfiledLimitScope.exit();
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Result next() {
    ProfiledLimitScope.enter();
    try {
      return new ResultInternal((Map<String, Object>) ResultInternal.wrap(documents.next()));
    } finally {
      ProfiledLimitScope.exit();
    }
  }

  @Override
  public void close() {
    cursor.close();
  }
}
