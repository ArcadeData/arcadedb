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
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_USERTYPE_VISIBILITY_PUBLIC=true */
package com.arcadedb.query.sql.parser;

import com.arcadedb.database.Database;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.index.Index;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.*;

public class DropIndexStatement extends DDLStatement {

  protected boolean    all      = false;
  protected Identifier name;
  protected boolean    ifExists = false;

  public DropIndexStatement(final int id) {
    super(id);
  }

  @Override
  public ResultSet executeDDL(final CommandContext context) {
    final InternalResultSet rs = new InternalResultSet();
    final Database db = context.getDatabase();

    if (all) {
      for (final Index idx : db.getSchema().getIndexes()) {
        db.getSchema().dropIndex(idx.getName());

        final ResultInternal result = new ResultInternal(db);
        result.setProperty("operation", "drop index");
        result.setProperty("bucketName", idx.getName());
        rs.add(result);
      }

    } else {
      if (!db.getSchema().existsIndex(name.getValue()) && !ifExists) {
        throw new CommandExecutionException("Index not found: " + name.getValue());
      }

      db.getSchema().dropIndex(name.getValue());

      final ResultInternal result = new ResultInternal();
      result.setProperty("operation", "drop index");
      result.setProperty("indexName", name.getValue());
      rs.add(result);
    }

    return rs;
  }

  @Override
  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    builder.append("DROP INDEX ");
    if (all) {
      builder.append("*");
    } else {
      name.toString(params, builder);
    }
    if (ifExists) {
      builder.append(" IF EXISTS");
    }
  }

  @Override
  public DropIndexStatement copy() {
    final DropIndexStatement result = new DropIndexStatement(-1);
    result.all = all;
    result.name = name == null ? null : name.copy();
    return result;
  }

  @Override
  public boolean equals( final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final DropIndexStatement that = (DropIndexStatement) o;

    if (all != that.all)
      return false;
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    int result = (all ? 1 : 0);
    result = 31 * result + (name != null ? name.hashCode() : 0);
    return result;
  }
}
/* JavaCC - OriginalChecksum=51c8221d049e4f114378e4be03797050 (do not edit this line) */
