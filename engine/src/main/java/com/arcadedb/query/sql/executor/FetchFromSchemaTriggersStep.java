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
package com.arcadedb.query.sql.executor;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Trigger;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityHelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Backs {@code SELECT FROM schema:triggers}, the catalog listing of the database's triggers.
 * <p>
 * Added with issue #7654's message sweep: {@code RemoteSchema.existsTrigger()} already issued this exact query, so
 * on a remote database it answered {@code UnsupportedOperationException: Invalid metadata: triggers} rather than
 * true or false, and the remote client had no way at all to see which triggers a database carries -
 * {@code CREATE TRIGGER} and {@code DROP TRIGGER} were both reachable over the wire while nothing could list what
 * they had produced.
 * <p>
 * A trigger names the type it fires on, so a caller who cannot read that type does not see it, the same
 * hide-rather-than-throw rule {@code schema:types} and {@code schema:indexes} follow (issue #4238): a listing that
 * aborted on the first restricted entry would lock a remote driver out of the entries it IS allowed to see. A
 * trigger whose type no longer exists is listed, not hidden - there is no type to check it against, and a dangling
 * trigger is precisely the kind of thing an operator opens this listing to find.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class FetchFromSchemaTriggersStep extends AbstractExecutionStep {

  private final List<ResultInternal> result = new ArrayList<>();

  private int cursor = 0;

  public FetchFromSchemaTriggersStep(final CommandContext context) {
    super(context);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);

    if (cursor == 0) {
      final long begin = context.isProfiling() ? System.nanoTime() : 0;
      try {
        final DatabaseInternal database = context.getDatabase();
        final Schema           schema   = database.getSchema();

        final List<Trigger> ordered = Arrays.stream(schema.getTriggers())
            .sorted(Comparator.comparing(Trigger::getName, String::compareToIgnoreCase))
            .toList();

        for (final Trigger trigger : ordered) {
          final String typeName = trigger.getTypeName();
          final DocumentType type = typeName != null && schema.existsType(typeName) ? schema.getType(typeName) : null;
          // The DatabaseInternal overload, which resolves the bound user itself, rather than a fourth private copy
          // of the currentUser(context) helper the sibling steps each carry.
          if (type != null && !SecurityHelper.canAccessType(database, type, SecurityDatabaseUser.ACCESS.READ_RECORD))
            continue;

          final ResultInternal r = new ResultInternal(database);
          result.add(r);

          r.setProperty("name", trigger.getName());
          r.setProperty("typeName", typeName);
          r.setProperty("timing", trigger.getTiming().name());
          r.setProperty("event", trigger.getEvent().name());
          r.setProperty("actionType", trigger.getActionType().name());
          r.setProperty("actionCode", trigger.getActionCode());

          context.setVariable("current", r);
        }
      } finally {
        if (context.isProfiling())
          cost += System.nanoTime() - begin;
      }
    }

    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return cursor < result.size();
      }

      @Override
      public Result next() {
        return result.get(cursor++);
      }

      @Override
      public void close() {
      }

      @Override
      public void reset() {
        cursor = 0;
      }
    };
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ FETCH DATABASE METADATA TRIGGERS";
    if (context.isProfiling())
      result += " (" + getCostFormatted() + ")";
    return result;
  }
}
