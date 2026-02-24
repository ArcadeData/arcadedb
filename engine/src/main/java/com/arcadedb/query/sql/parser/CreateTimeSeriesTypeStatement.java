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

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.TimeSeriesTypeBuilder;
import com.arcadedb.schema.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * SQL statement: CREATE TIMESERIES TYPE
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class CreateTimeSeriesTypeStatement extends DDLStatement {

  public Identifier name;
  public boolean    ifNotExists;
  public Identifier timestampColumn;
  public PInteger   shards;
  public long       retentionMs;
  public long       compactionIntervalMs;

  public List<ColumnDef> tags   = new ArrayList<>();
  public List<ColumnDef> fields = new ArrayList<>();

  public CreateTimeSeriesTypeStatement(final int id) {
    super(id);
  }

  @Override
  public ResultSet executeDDL(final CommandContext context) {
    final Schema schema = context.getDatabase().getSchema();

    if (schema.existsType(name.getStringValue())) {
      if (ifNotExists)
        return new InternalResultSet();
      else
        throw new CommandExecutionException("Type '" + name.getStringValue() + "' already exists");
    }

    TimeSeriesTypeBuilder builder = schema.buildTimeSeriesType().withName(name.getStringValue());

    if (timestampColumn != null)
      builder = builder.withTimestamp(timestampColumn.getStringValue());

    for (final ColumnDef tag : tags)
      builder = builder.withTag(tag.name.getStringValue(), Type.getTypeByName(tag.type.getStringValue()));

    for (final ColumnDef field : fields)
      builder = builder.withField(field.name.getStringValue(), Type.getTypeByName(field.type.getStringValue()));

    if (shards != null)
      builder = builder.withShards(shards.getValue().intValue());

    if (retentionMs > 0)
      builder = builder.withRetention(retentionMs);

    if (compactionIntervalMs > 0)
      builder = builder.withCompactionBucketInterval(compactionIntervalMs);

    builder.create();

    final ResultInternal result = new ResultInternal(context.getDatabase());
    result.setProperty("operation", "create timeseries type");
    result.setProperty("typeName", name.getStringValue());
    return new InternalResultSet(result);
  }

  @Override
  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    builder.append("CREATE TIMESERIES TYPE ");
    name.toString(params, builder);

    if (ifNotExists)
      builder.append(" IF NOT EXISTS");

    if (timestampColumn != null) {
      builder.append(" TIMESTAMP ");
      timestampColumn.toString(params, builder);
    }

    if (!tags.isEmpty()) {
      builder.append(" TAGS (");
      for (int i = 0; i < tags.size(); i++) {
        if (i > 0)
          builder.append(", ");
        tags.get(i).name.toString(params, builder);
        builder.append(" ");
        tags.get(i).type.toString(params, builder);
      }
      builder.append(")");
    }

    if (!fields.isEmpty()) {
      builder.append(" FIELDS (");
      for (int i = 0; i < fields.size(); i++) {
        if (i > 0)
          builder.append(", ");
        fields.get(i).name.toString(params, builder);
        builder.append(" ");
        fields.get(i).type.toString(params, builder);
      }
      builder.append(")");
    }

    if (shards != null) {
      builder.append(" SHARDS ");
      shards.toString(params, builder);
    }

    if (retentionMs > 0) {
      builder.append(" RETENTION ");
      builder.append(retentionMs);
    }

    if (compactionIntervalMs > 0) {
      builder.append(" COMPACTION_INTERVAL ");
      builder.append(compactionIntervalMs);
    }
  }

  @Override
  public CreateTimeSeriesTypeStatement copy() {
    final CreateTimeSeriesTypeStatement result = new CreateTimeSeriesTypeStatement(-1);
    result.name = name == null ? null : name.copy();
    result.ifNotExists = ifNotExists;
    result.timestampColumn = timestampColumn == null ? null : timestampColumn.copy();
    result.shards = shards == null ? null : shards.copy();
    result.retentionMs = retentionMs;
    result.compactionIntervalMs = compactionIntervalMs;
    result.tags = new ArrayList<>(tags.size());
    for (final ColumnDef cd : tags)
      result.tags.add(new ColumnDef(cd.name == null ? null : cd.name.copy(), cd.type == null ? null : cd.type.copy()));
    result.fields = new ArrayList<>(fields.size());
    for (final ColumnDef cd : fields)
      result.fields.add(new ColumnDef(cd.name == null ? null : cd.name.copy(), cd.type == null ? null : cd.type.copy()));
    return result;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final CreateTimeSeriesTypeStatement that = (CreateTimeSeriesTypeStatement) o;
    return ifNotExists == that.ifNotExists && retentionMs == that.retentionMs
        && compactionIntervalMs == that.compactionIntervalMs && Objects.equals(name, that.name)
        && Objects.equals(timestampColumn, that.timestampColumn) && Objects.equals(shards, that.shards)
        && Objects.equals(tags, that.tags) && Objects.equals(fields, that.fields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, ifNotExists, timestampColumn, shards, retentionMs, compactionIntervalMs, tags, fields);
  }

  public static class ColumnDef {
    public Identifier name;
    public Identifier type;

    public ColumnDef(final Identifier name, final Identifier type) {
      this.name = name;
      this.type = type;
    }
  }
}
