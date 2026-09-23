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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.exception.TimeoutException;

import java.util.*;

/**
 * Returns an OResult containing metadata regarding the database
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class FetchFromSchemaDatabaseStep extends AbstractExecutionStep {
  boolean served = false;

  public FetchFromSchemaDatabaseStep(final CommandContext context) {
    super(context);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);
    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return !served;
      }

      @Override
      public Result next() {
        final long begin = context.isProfiling() ? System.nanoTime() : 0;
        try {

          if (!served) {
            final Database db = context.getDatabase();

            final ResultInternal result = new ResultInternal(db);
            result.setProperty("name", db.getName());
            result.setProperty("path", db.getDatabasePath());
            result.setProperty("mode", db.getMode());
            result.setProperty("size", db.getSize());
            result.setProperty("dateFormat", db.getSchema().getDateFormat());
            result.setProperty("dateTimeFormat", db.getSchema().getDateTimeFormat());
            result.setProperty("timezone", db.getSchema().getTimeZone().getDisplayName());
            result.setProperty("encoding", db.getSchema().getEncoding());

            final ContextConfiguration dbCfg = db.getConfiguration();
            final Set<String> contextKeys = dbCfg.getContextKeys();

            final List<Map<String, Object>> settings = new ArrayList<>();
            for (GlobalConfiguration cfg : GlobalConfiguration.values()) {
              if (cfg.getScope() == GlobalConfiguration.SCOPE.DATABASE) {
                final Map<String, Object> map = new LinkedHashMap<>();
                map.put("key", cfg.getKey());
                // Reuse, not a fix: this loop only ever walks SCOPE.DATABASE settings, so the credential
                // embedding that publishableValue redacts (arcadedb.server.defaultDatabases, SCOPE.SERVER)
                // cannot reach here, and no DATABASE-scope setting is hidden today either. What it replaces is
                // the third copy of the rule - an ad-hoc "key contains password" check - so that the next
                // setting that does need redacting is covered here by construction rather than by someone
                // remembering this third site exists.
                map.put("value", cfg.publishableValue(dbCfg.getValue(cfg)));
                map.put("description", cfg.getDescription());
                map.put("overridden", contextKeys.contains(cfg.getKey()));
                map.put("default", cfg.publishableValue(cfg.getDefValue()));

                settings.add(map);
              }
            }
            result.setProperty("settings", settings);

            served = true;
            return result;
          }
          throw new NoSuchElementException();
        } finally {
          if (context.isProfiling()) {
            cost += System.nanoTime() - begin;
          }
        }
      }

      @Override
      public void reset() {
        served = false;
      }
    };
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ FETCH DATABASE METADATA";
    if (context.isProfiling()) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }
}
