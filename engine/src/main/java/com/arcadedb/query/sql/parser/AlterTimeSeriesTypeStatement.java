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

import com.arcadedb.engine.timeseries.DownsamplingTier;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.LocalTimeSeriesType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * SQL statement: ALTER TIMESERIES TYPE
 */
public class AlterTimeSeriesTypeStatement extends DDLStatement {

  public Identifier              name;
  public boolean                 addPolicy;
  public List<DownsamplingTier>  tiers = new ArrayList<>();

  public AlterTimeSeriesTypeStatement(final int id) {
    super(id);
  }

  @Override
  public ResultSet executeDDL(final CommandContext context) {
    final DocumentType type = context.getDatabase().getSchema().getType(name.getStringValue());
    if (!(type instanceof LocalTimeSeriesType tsType))
      throw new CommandExecutionException("Type '" + name.getStringValue() + "' is not a TimeSeries type");

    if (addPolicy)
      tsType.setDownsamplingTiers(tiers);
    else
      tsType.setDownsamplingTiers(new ArrayList<>());

    ((LocalSchema) context.getDatabase().getSchema()).saveConfiguration();

    final ResultInternal result = new ResultInternal(context.getDatabase());
    result.setProperty("operation", addPolicy ? "add downsampling policy" : "drop downsampling policy");
    result.setProperty("typeName", name.getStringValue());
    return new InternalResultSet(result);
  }

  @Override
  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    builder.append("ALTER TIMESERIES TYPE ");
    name.toString(params, builder);

    if (addPolicy) {
      builder.append(" ADD DOWNSAMPLING POLICY");
      for (final DownsamplingTier tier : tiers) {
        builder.append(" AFTER ").append(tier.afterMs());
        builder.append(" GRANULARITY ").append(tier.granularityMs());
      }
    } else
      builder.append(" DROP DOWNSAMPLING POLICY");
  }

  @Override
  public AlterTimeSeriesTypeStatement copy() {
    final AlterTimeSeriesTypeStatement result = new AlterTimeSeriesTypeStatement(-1);
    result.name = name == null ? null : name.copy();
    result.addPolicy = addPolicy;
    result.tiers = new ArrayList<>(tiers);
    return result;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final AlterTimeSeriesTypeStatement that = (AlterTimeSeriesTypeStatement) o;
    return addPolicy == that.addPolicy && Objects.equals(name, that.name) && Objects.equals(tiers, that.tiers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, addPolicy, tiers);
  }
}
