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

import com.arcadedb.query.ProfiledLimitScope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.NoSuchElementException;

/**
 * The last step of a root traversal run by a user whose security groups set a {@code resultSetLimit}: it lets through that many
 * rows and no more (issue #13087). {@link ArcadeResultSetLimitStrategy} adds it. The quota caps the rows a traversal answers, as
 * it caps the rows of a SQL or an openCypher statement, and never what the traversal reads: the steps before this one see every
 * record, so a filter, a count or an order is computed over all of them.
 * <p>
 * A row is counted by its bulk: a barrier hands over one traverser for many equal rows, and the one that crosses the quota is
 * let through for the rows that are left.
 * <p>
 * A traversal is lazy, so its writes happen while its rows are pulled. One that writes is therefore run to its end once the quota
 * is reached, and only the rows past it are dropped: the quota does not limit a write. So is one that holds a lambda, which may
 * write: only running it tells. One that only reads stops there.
 * <p>
 * Whatever runs while a row is pulled - a SQL statement of a step, a traversal started by another one - is a part of this
 * traversal and not an answer to the user: {@link ProfiledLimitScope} tells every query engine not to cap it on its own.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
final class ArcadeResultSetLimitStep<S> extends AbstractStep<S, S> {
  private final long    limit;
  private final boolean writes;
  private       long    returned;

  /**
   * @param limit  the rows the traversal may answer
   * @param writes whether the traversal writes, or may, so it has to run to its end past the quota
   */
  ArcadeResultSetLimitStep(final Traversal.Admin<?, ?> traversal, final long limit, final boolean writes) {
    super(traversal);
    this.limit = limit;
    this.writes = writes;
  }

  @Override
  protected Traverser.Admin<S> processNextStart() throws NoSuchElementException {
    if (returned >= limit) {
      if (writes)
        drain();
      throw FastNoSuchElementException.instance();
    }

    final Traverser.Admin<S> traverser = pull();
    final long left = limit - returned;
    if (traverser.bulk() > left)
      traverser.setBulk(left);
    returned += traverser.bulk();
    return traverser;
  }

  private Traverser.Admin<S> pull() {
    ProfiledLimitScope.enter();
    try {
      return starts.next();
    } finally {
      ProfiledLimitScope.exit();
    }
  }

  /** Runs the rest of the traversal for what it writes. Asked again once it is exhausted, the steps before this one answer at once. */
  private void drain() {
    ProfiledLimitScope.enter();
    try {
      while (starts.hasNext())
        starts.next();
    } finally {
      ProfiledLimitScope.exit();
    }
  }

  @Override
  public void reset() {
    super.reset();
    returned = 0;
  }

  @Override
  public ArcadeResultSetLimitStep<S> clone() {
    final ArcadeResultSetLimitStep<S> clone = (ArcadeResultSetLimitStep<S>) super.clone();
    clone.returned = 0;
    return clone;
  }

  @Override
  public int hashCode() {
    return super.hashCode() ^ Long.hashCode(limit) ^ Boolean.hashCode(writes);
  }

  @Override
  public String toString() {
    return StringFactory.stepString(this, limit);
  }
}
