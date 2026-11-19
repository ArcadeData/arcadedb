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
 * A statement that can only run in the implicit transaction a request or an auto-commit query opens for it, sent inside a
 * transaction the caller opened (HTTP {@code /begin}, Bolt {@code BEGIN} or a managed transaction, an embedded {@code begin()}):
 * {@code CALL { ... } IN TRANSACTIONS} commits its batches on its own, which the caller's rollback could not undo (issue
 * #11588). The caller's to resolve - send it outside the transaction - and never retryable: the same statement is refused again
 * while the transaction is open.
 * <p>
 * A {@link CommandExecutionException}, which the refusal was raised as, so every caller that catches it keeps catching it. As
 * that plain type no rule classified it, and HTTP answered it as a server fault (500) and Bolt as
 * {@code Neo.DatabaseError.General.UnknownError} (issue #12161). Classified {@link ErrorCategory#VALIDATION}; HTTP answers it
 * {@code 409 Conflict}, the request conflicting with the state of the caller's session, Bolt
 * {@code Neo.DatabaseError.Transaction.TransactionStartFailed} as Neo4j does, and Postgres {@code 25001} active_sql_transaction.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ImplicitTransactionRequiredException extends CommandExecutionException {
  public ImplicitTransactionRequiredException(final String message) {
    super(message);
  }
}
