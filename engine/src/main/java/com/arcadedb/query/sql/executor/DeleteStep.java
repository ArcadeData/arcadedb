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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.Record;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;

/**
 * Deletes records coming from upstream steps. Only the rows the engine actually deleted are passed on: a delete a
 * {@code BeforeRecordDeleteListener} vetoed is dropped here, so the {@link CountStep} after this step - the count SQL, the Postgres
 * wire and the HTTP API reply - counts deletions rather than candidates, and {@code RETURN BEFORE} returns only the records that are
 * gone (issue #9974). A row that is not a record (nothing to delete) is passed on as it always was.
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class DeleteStep extends AbstractExecutionStep {

  public DeleteStep(final CommandContext context) {
    super(context);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    final ExecutionStepInternal prevStep = checkForPrevious();
    final DatabaseInternal database = context.getDatabase();
    // deletes one row ahead of what it returns: only valid under a plan that drains every pull (see SkippingResultSet)
    return new SkippingResultSet(prevStep, context, nRecords) {
      @Override
      protected Result process(final Result result) {
        final long begin = context.isProfiling() ? System.nanoTime() : 0;
        try {
          if (result.isElement())
            return database.deleteRecordReportingVeto(result.getElement().get()) ? result : null;
          // A PROJECTION THAT CARRIES THE @rid OF A RECORD DELETES THAT RECORD, AS UPDATE WRITES TO IT (ConvertToUpdatableResultStep).
          // ANY OTHER ROW (A PROJECTION, A TIMESERIES SAMPLE) HAS NOTHING TO DELETE: IT USED TO BE COUNTED AS DELETED AND KEPT
          // (issue #10336)
          if (result.getProperty("@rid") instanceof Identifiable identifiable && identifiable.getRecord() instanceof Record record)
            return database.deleteRecordReportingVeto(record) ? result : null;
          throw new CommandExecutionException(ConvertToUpdatableResultStep.NOT_A_RECORD);
        } finally {
          if (context.isProfiling())
            cost += System.nanoTime() - begin;
        }
      }
    };
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    final StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ DELETE");
    if (context.isProfiling())
      result.append(" (").append(getCostFormatted()).append(")");

    return result.toString();
  }

}
