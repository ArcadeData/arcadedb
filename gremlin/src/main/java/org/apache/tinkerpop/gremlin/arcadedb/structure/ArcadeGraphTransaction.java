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
package org.apache.tinkerpop.gremlin.arcadedb.structure;

import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction;
import org.apache.tinkerpop.gremlin.structure.util.TransactionException;

/**
 * Created by Enrico Risa on 30/07/2018.
 */
public class ArcadeGraphTransaction extends AbstractThreadLocalTransaction {
  private final ArcadeGraph graph;

  public ArcadeGraphTransaction(ArcadeGraph arcadeGraph) {
    super(arcadeGraph);
    graph = arcadeGraph;
  }

  @Override
  protected void doOpen() {
    graph.getDatabase().begin();
  }

  @Override
  protected void doCommit() throws TransactionException {
    graph.getDatabase().commit();
  }

  @Override
  protected void doRollback() throws TransactionException {
    graph.getDatabase().rollback();
  }

  @Override
  public boolean isOpen() {
    return graph.getDatabase().isTransactionActive();
  }
}
