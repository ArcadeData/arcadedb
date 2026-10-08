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
package com.arcadedb.exception;

/**
 * A query was not started by the query admission gate ({@link com.arcadedb.query.sql.executor.QueryAdmissionGate},
 * issue #9518): it waited in the queue longer than {@link com.arcadedb.GlobalConfiguration#QUERY_QUEUE_TIMEOUT}, or the
 * queue already held {@link com.arcadedb.GlobalConfiguration#QUERY_QUEUE_MAX_SIZE} queries when it arrived.
 * <p>
 * Nothing of the query ran, and the server is busy rather than broken, so the same request re-issued later can succeed:
 * it is classified as retryable on every wire (503 on HTTP), like a {@link QueryHeapBudgetExceededException}. It is NOT a
 * {@link NeedRetryException} for the same reason that one is not: it is no transaction conflict, and a commit loop that
 * retried it on the spot would only queue again behind the same load.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class QueryAdmissionException extends CommandExecutionException {
  public QueryAdmissionException(final String message) {
    super(message);
  }
}
