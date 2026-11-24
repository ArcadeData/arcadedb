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
package com.arcadedb.gremlin;

import com.arcadedb.gremlin.io.ArcadeIoRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.IO;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.ReadWriting;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IoStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;

public class ArcadeIoRegistrationStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
    implements TraversalStrategy.ProviderOptimizationStrategy {

  private static final ArcadeIoRegistrationStrategy INSTANCE           = new ArcadeIoRegistrationStrategy(false);
  private static final ArcadeIoRegistrationStrategy STATEMENT_INSTANCE = new ArcadeIoRegistrationStrategy(true);

  // WHETHER THE TRAVERSAL RUNS FOR A STATEMENT OF THE QUERY ENGINE, WHICH NEVER OWNS THE COMMIT: SEE forStatement()
  private final boolean statement;

  private ArcadeIoRegistrationStrategy(final boolean statement) {
    this.statement = statement;
  }

  @Override
  public void apply(final Traversal.Admin<?, ?> traversal) {
    // io() READS OR WRITES A FILE AT A CALLER-CHOSEN HOST PATH: RESERVED TO THE SERVER ADMINISTRATOR. THE FLUENT API ONLY
    // OFFERS io() AS A START STEP, BUT WIRE BYTECODE IS A GENERIC INSTRUCTION LIST, SO EVERY STEP IS CHECKED
    boolean checked = false;
    boolean reads = false;
    for (final Object step : traversal.getSteps())
      if (step instanceof IoStep ioStep) {
        if (!checked) {
          checkIoPermission(traversal);
          checked = true;
        }
        ioStep.configure(IO.registry, new ArcadeIoRegistry());
        reads |= ioStep.getMode() == ReadWriting.Mode.READING;
      }

    if (reads && statement)
      leaveTheCommitToTheTransaction(traversal);
  }

  /**
   * Binds the traversal to a graph whose features declare transactions unsupported when a transaction is open (issue #13162).
   * TinkerPop's bulk readers ask that feature of the graph {@code IoStep} hands them, the one of the traversal, before every
   * {@code graph.tx().commit()} they issue, one per {@code batchSize} records (10000) and one at the end. The graph of the query engine
   * JOINS the transaction of the thread, so those commits ended the transaction of the statement, or of the caller, in the middle of
   * the file: a record that failed later left the earlier batches committed, and the caller was left with no transaction to roll back.
   * <p>
   * With no transaction open there is nothing to protect and the reader keeps its commits: that is the result of a statement read
   * after its caller ended the transaction it ran in.
   */
  private static void leaveTheCommitToTheTransaction(final Traversal.Admin<?, ?> traversal) {
    // AN ArcadeGraph: checkIoPermission() REFUSED ANYTHING ELSE
    final ArcadeGraph arcadeGraph = (ArcadeGraph) traversal.getGraph().get();
    if (arcadeGraph.features().graph().supportsTransactions() && arcadeGraph.getDatabase().isTransactionActive())
      traversal.setGraph(ArcadeGraph.openInCallerTransaction(arcadeGraph.getDatabase()));
  }

  private static void checkIoPermission(final Traversal.Admin<?, ?> traversal) {
    // FAIL CLOSED WHEN THE TRAVERSAL CARRIES NO ARCADEGRAPH: WITHOUT ITS DATABASE THE BOUND PRINCIPAL CANNOT BE CHECKED
    if (!(traversal.getGraph().orElse(null) instanceof ArcadeGraph arcadeGraph))
      throw new SecurityException("Cannot verify the permission to use the io() step: the traversal is not bound to an ArcadeDB graph");
    GremlinHostAccessGuard.checkServerAdministrator(arcadeGraph.getDatabase(), "use the io() step");
  }

  public static ArcadeIoRegistrationStrategy instance() {
    return INSTANCE;
  }

  /**
   * The strategy of a traversal that runs for a statement of the query engine ({@code Database.command("gremlin", ...)}). Such a
   * statement runs in the transaction of its caller, or in the one the engine opens for it and commits once, so an {@code io().read()}
   * in it must not commit: see {@link #leaveTheCommitToTheTransaction}. It replaces {@link #instance()} in the strategies of the
   * source it is added to, being of the same class, and does everything that one does.
   * <p>
   * The graph API keeps {@link #instance()}: it has no statement, and there the commit of the reader is the only one an import gets
   * (a transaction that is open may be the one of the AUTO model, opened by an earlier read and never committed by anybody).
   */
  static ArcadeIoRegistrationStrategy forStatement() {
    return STATEMENT_INSTANCE;
  }
}
