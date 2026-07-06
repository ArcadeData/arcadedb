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

/**
 * Mutable, allocation-light accumulator of CRUD and schema mutation counts produced while executing
 * a single command. Held on the {@link CommandContext} and read once when the result set is
 * assembled. All counters are primitive ints; instances are created only for write commands.
 * Not thread-safe: a single command context is written by one query thread at a time; counters are
 * plain ints with no synchronization.
 */
public class QueryStatistics {
  private int nodesCreated;
  private int nodesDeleted;
  private int relationshipsCreated;
  private int relationshipsDeleted;
  private int propertiesSet;
  private int labelsAdded;
  private int labelsRemoved;
  private int indexesAdded;
  private int indexesRemoved;
  private int constraintsAdded;
  private int constraintsRemoved;

  public void incNodesCreated()          { nodesCreated++; }
  public void incNodesDeleted()          { nodesDeleted++; }
  public void incRelationshipsCreated()  { relationshipsCreated++; }
  public void incRelationshipsDeleted()  { relationshipsDeleted++; }
  public void addPropertiesSet(final int n) { propertiesSet += n; }
  public void addLabelsAdded(final int n)   { labelsAdded += n; }
  public void addLabelsRemoved(final int n) { labelsRemoved += n; }
  public void incIndexesAdded()          { indexesAdded++; }
  public void incIndexesRemoved()        { indexesRemoved++; }
  public void incConstraintsAdded()      { constraintsAdded++; }
  public void incConstraintsRemoved()    { constraintsRemoved++; }

  public int getNodesCreated()          { return nodesCreated; }
  public int getNodesDeleted()          { return nodesDeleted; }
  public int getRelationshipsCreated()  { return relationshipsCreated; }
  public int getRelationshipsDeleted()  { return relationshipsDeleted; }
  public int getPropertiesSet()         { return propertiesSet; }
  public int getLabelsAdded()           { return labelsAdded; }
  public int getLabelsRemoved()         { return labelsRemoved; }
  public int getIndexesAdded()          { return indexesAdded; }
  public int getIndexesRemoved()        { return indexesRemoved; }
  public int getConstraintsAdded()      { return constraintsAdded; }
  public int getConstraintsRemoved()    { return constraintsRemoved; }

  public boolean containsUpdates() {
    return nodesCreated != 0 || nodesDeleted != 0 || relationshipsCreated != 0 || relationshipsDeleted != 0
        || propertiesSet != 0 || labelsAdded != 0 || labelsRemoved != 0
        || indexesAdded != 0 || indexesRemoved != 0 || constraintsAdded != 0 || constraintsRemoved != 0;
  }
}
