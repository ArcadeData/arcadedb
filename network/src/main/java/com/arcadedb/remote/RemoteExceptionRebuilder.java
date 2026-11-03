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
package com.arcadedb.remote;

import com.arcadedb.exception.ArcadeDBException;
import com.arcadedb.exception.ArithmeticErrorException;
import com.arcadedb.exception.BrokenChunkChainException;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParameterMissingException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.exception.DatabaseIsClosedException;
import com.arcadedb.exception.DatabaseIsReadOnlyException;
import com.arcadedb.exception.DatabaseIsReopeningException;
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.exception.DatabaseNotAvailableException;
import com.arcadedb.exception.DatabaseNotFoundException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.DatabaseOperationInProgressException;
import com.arcadedb.exception.EncryptionException;
import com.arcadedb.exception.HeapLimitExceededException;
import com.arcadedb.exception.InvalidDatabaseInstanceException;
import com.arcadedb.exception.InvalidPropertyTypeException;
import com.arcadedb.exception.LockTimeoutException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.PageSnapshotException;
import com.arcadedb.exception.PartialResultTimeoutException;
import com.arcadedb.exception.QueryAdmissionException;
import com.arcadedb.exception.QueryHeapBudgetExceededException;
import com.arcadedb.exception.QueryNotIdempotentException;
import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.exception.RecordVetoedException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.SerializationException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.exception.TransactionCommittedRemotelyException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.exception.TransactionOutcomeUnknownException;
import com.arcadedb.exception.TypeMismatchException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.exception.WALVersionGapException;
import com.arcadedb.function.FunctionExecutionException;
import com.arcadedb.index.IndexException;
import com.arcadedb.network.binary.QuorumNotReachedException;
import com.arcadedb.query.opencypher.parser.FeatureNotImplementedException;
import com.arcadedb.schema.trigger.TriggerExecutionException;
import com.arcadedb.utility.LockException;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;

/**
 * Rebuilds, by the class name the server put on the wire, every exception that carries nothing but its message (issue #11045).
 * Both drivers ({@code RemoteHttpComponent} and the gRPC client) map by hand the exceptions that carry more - a RID, a leader
 * address, the parts of a duplicated key, a reason - and each kept its own partial list of the others, answering every name
 * missing from it with an untyped {@link RemoteException} (HTTP) or with the type the status code suggests (gRPC): a
 * {@code CommandSQLParsingException} or a {@code CommandExecutionException} that embedded code catches by its type then
 * escaped the same {@code catch} over the driver. This is that list, complete, in one place both drivers read after their own
 * structured mappings.
 * <p>
 * An explicit table of factories rather than a class looked up by the name the server sent, as the leader-forwarding path of
 * the Raft module chose for the same reason: nothing a response names is ever loaded or instantiated unless it is listed here,
 * and an exception whose {@code (String)} constructor takes something other than its message (a type name, say) is not
 * rebuilt with the wrong argument. {@code Issue11045RemoteExceptionTypeTest} scans the engine's exception package and fails
 * on a class that neither the drivers nor this table rebuild, so a new exception cannot drift out of it unnoticed; a name that
 * still reaches a driver unlisted (a class of a newer server, or of a plugin) is a {@link RemoteException}, which is an
 * {@link ArcadeDBException} too.
 * <p>
 * {@link IllegalArgumentException} is listed as well: the engine raises it for an unknown query language, and an embedded
 * caller catches it as such.
 * <p>
 * Adding an exception: a new class of {@code com.arcadedb.exception} whose {@code (String)} constructor takes its message gets
 * one line here, and the scanning test fails until it has it. A message-only exception that lives in another package of the
 * engine or of the network module ({@code FeatureNotImplementedException}, {@code TriggerExecutionException},
 * {@code QuorumNotReachedException}, ...) is outside the scan, so whoever adds one adds its line here by hand. An exception
 * that carries more than its message gets a structured mapping in the drivers instead, ahead of this table.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class RemoteExceptionRebuilder {
  private static final Map<String, Function<String, RuntimeException>> FACTORIES = Map.ofEntries(
      // A FAILING STATEMENT
      Map.entry(CommandExecutionException.class.getName(), CommandExecutionException::new),
      Map.entry(ArithmeticErrorException.class.getName(), ArithmeticErrorException::new),
      Map.entry(CommandParsingException.class.getName(), CommandParsingException::new),
      Map.entry(CommandSQLParsingException.class.getName(), CommandSQLParsingException::new),
      Map.entry(CommandSemanticException.class.getName(), CommandSemanticException::new),
      // ITS CONSTRUCTOR TAKES THE NAMES OF THE MISSING PARAMETERS, NOT A MESSAGE: REBUILT AS THE SEMANTIC ERROR IT IS
      Map.entry(CommandParameterMissingException.class.getName(), CommandSemanticException::new),
      Map.entry(FeatureNotImplementedException.class.getName(), FeatureNotImplementedException::new),
      Map.entry(QueryNotIdempotentException.class.getName(), QueryNotIdempotentException::new),
      Map.entry(FunctionExecutionException.class.getName(), FunctionExecutionException::new),
      Map.entry(TriggerExecutionException.class.getName(), TriggerExecutionException::new),
      Map.entry(InvalidPropertyTypeException.class.getName(), InvalidPropertyTypeException::new),
      Map.entry(TypeMismatchException.class.getName(), TypeMismatchException::new),
      Map.entry(IllegalArgumentException.class.getName(), IllegalArgumentException::new),
      // A QUERY REFUSED FOR WHAT IT WOULD COST
      Map.entry(HeapLimitExceededException.class.getName(), HeapLimitExceededException::new),
      Map.entry(QueryAdmissionException.class.getName(), QueryAdmissionException::new),
      Map.entry(QueryHeapBudgetExceededException.class.getName(), QueryHeapBudgetExceededException::new),
      Map.entry(PartialResultTimeoutException.class.getName(), PartialResultTimeoutException::new),
      // RETRYABLE: THE SERVER SENDS THESE WITH 503, AND REBUILT AS THEMSELVES THEY STAY NeedRetryExceptions
      Map.entry(LockTimeoutException.class.getName(), LockTimeoutException::new),
      Map.entry(WALVersionGapException.class.getName(), WALVersionGapException::new),
      // THE STATE OF THE DATABASE OR OF ITS FILES
      Map.entry(DatabaseIsReadOnlyException.class.getName(), DatabaseIsReadOnlyException::new),
      Map.entry(DatabaseIsClosedException.class.getName(), DatabaseIsClosedException::new),
      Map.entry(DatabaseIsReopeningException.class.getName(), DatabaseIsReopeningException::new),
      Map.entry(DatabaseOperationException.class.getName(), DatabaseOperationException::new),
      Map.entry(DatabaseOperationInProgressException.class.getName(), DatabaseOperationInProgressException::new),
      Map.entry(DatabaseMetadataException.class.getName(), DatabaseMetadataException::new),
      Map.entry(InvalidDatabaseInstanceException.class.getName(), InvalidDatabaseInstanceException::new),
      Map.entry(ConfigurationException.class.getName(), ConfigurationException::new),
      Map.entry(EncryptionException.class.getName(), EncryptionException::new),
      Map.entry(SerializationException.class.getName(), SerializationException::new),
      Map.entry(BrokenChunkChainException.class.getName(), BrokenChunkChainException::new),
      Map.entry(PageSnapshotException.class.getName(), PageSnapshotException::new),
      Map.entry(IndexException.class.getName(), IndexException::new),
      Map.entry(LockException.class.getName(), LockException::new),
      Map.entry(TransactionOutcomeUnknownException.class.getName(), TransactionOutcomeUnknownException::new),
      // THE ONES THE HTTP DRIVER ALSO MAPS BY HAND, AHEAD OF THIS TABLE AND WHATEVER THE STATUS, WITH THE ISSUE THAT ADDED EACH:
      // LISTED HERE FOR THE gRPC CLIENT, WHOSE OWN TABLE HAD NO ENTRY FOR SEVERAL OF THEM
      Map.entry(TransactionException.class.getName(), TransactionException::new),
      Map.entry(TransactionCommittedRemotelyException.class.getName(), TransactionCommittedRemotelyException::new),
      Map.entry(ConcurrentModificationException.class.getName(), ConcurrentModificationException::new),
      Map.entry(NeedRetryException.class.getName(), NeedRetryException::new),
      Map.entry(QuorumNotReachedException.class.getName(), QuorumNotReachedException::new),
      Map.entry(TimeoutException.class.getName(), TimeoutException::new),
      Map.entry(QueryTerminatedException.class.getName(), QueryTerminatedException::new),
      Map.entry(DatabaseNotFoundException.class.getName(), DatabaseNotFoundException::new),
      // THE HTTP DRIVER REBUILDS IT WITH THE REASON IN exceptionArgs (ISSUE #10872); WITHOUT ONE IT IS 'UNAVAILABLE'
      Map.entry(DatabaseNotAvailableException.class.getName(), DatabaseNotAvailableException::new),
      Map.entry(SchemaException.class.getName(), SchemaException::new),
      Map.entry(ValidationException.class.getName(), ValidationException::new),
      Map.entry(RecordVetoedException.class.getName(), RecordVetoedException::new),
      Map.entry(SecurityException.class.getName(), SecurityException::new),
      Map.entry(NoSuchElementException.class.getName(), NoSuchElementException::new),
      // THE ROOT ITSELF: A FAILURE THE SERVER RAISED AS A PLAIN ArcadeDBException
      Map.entry(ArcadeDBException.class.getName(), ArcadeDBException::new));

  private RemoteExceptionRebuilder() {
  }

  /**
   * The class names this table rebuilds. It is the authority a census of the drivers' exception mapping has to enumerate
   * from (issue #11199): a scan of a package sees only the classes that happen to live there.
   */
  public static Set<String> rebuildableNames() {
    return FACTORIES.keySet();
  }

  /**
   * The exception {@code className} names, carrying {@code message}, or null when it is not one listed here.
   */
  public static RuntimeException rebuild(final String className, final String message) {
    if (className == null)
      return null;
    final Function<String, RuntimeException> factory = FACTORIES.get(className);
    return factory != null ? factory.apply(message) : null;
  }
}
