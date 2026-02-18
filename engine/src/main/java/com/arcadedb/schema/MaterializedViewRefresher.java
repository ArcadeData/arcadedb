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
package com.arcadedb.schema;

import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.logging.Level;

public class MaterializedViewRefresher {

  public static void fullRefresh(final Database database, final MaterializedViewImpl view) {
    view.setStatus(MaterializedViewStatus.BUILDING);
    try {
      final String backingTypeName = view.getBackingTypeName();

      database.transaction(() -> {
        // Delete existing data
        database.command("sql", "DELETE FROM `" + backingTypeName + "`");

        // Execute the defining query and insert results
        try (final ResultSet rs = database.query("sql", view.getQuery())) {
          while (rs.hasNext()) {
            final Result result = rs.next();
            final MutableDocument doc = database.newDocument(backingTypeName);
            for (final String prop : result.getPropertyNames()) {
              if (!prop.startsWith("@"))
                doc.set(prop, result.getProperty(prop));
            }
            doc.save();
          }
        }
      });

      view.updateLastRefreshTime();
      view.setStatus(MaterializedViewStatus.VALID);

    } catch (final Exception e) {
      view.setStatus(MaterializedViewStatus.ERROR);
      LogManager.instance().log(MaterializedViewRefresher.class, Level.SEVERE,
          "Error refreshing materialized view '%s': %s", e, view.getName(), e.getMessage());
      throw e;
    }
  }
}
