/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.schema.LocalSchema;

import java.util.List;

/**
 * What is wrong with this node's own copy of a database that a peer's copy can repair (issues #11250, #11258, #11518): a crash
 * recovery that did not replay every transaction, a schema dictionary a power loss emptied, or a unique index whose file has no
 * header page. The one reading of these three states, shared by every path that decides on them: the Raft startup scan, the
 * reopen of a database on a running node, the schema reload of a replicated schema entry and the validation of an installed
 * snapshot. When more than one holds, the first in that order is reported, as the startup scan always did.
 *
 * @param cause            the quarantine cause the damage maps to
 * @param uniqueIndexNames the unavailable unique indexes, for {@link DivergenceCause#UNAVAILABLE_UNIQUE_INDEX} only
 */
record LocalCopyDamage(DivergenceCause cause, List<String> uniqueIndexNames) {

  /**
   * Reads the damage of {@code database}'s local copy, {@code null} when there is none. Three in-memory reads, so cheap enough
   * for every open and every schema reload.
   */
  static LocalCopyDamage of(final DatabaseInternal database) {
    if (database == null)
      return null;
    final DatabaseInternal embedded = database.getEmbedded();
    if (embedded.getTransactionManager().isLastRecoveryIncomplete())
      return new LocalCopyDamage(DivergenceCause.LOCAL_RECOVERY_INCOMPLETE, List.of());
    final LocalSchema schema = embedded.getSchema().getEmbedded();
    if (schema.isDictionaryLostForExistingTypes())
      return new LocalCopyDamage(DivergenceCause.EMPTY_DICTIONARY, List.of());
    final List<String> unavailableUnique = schema.getUnavailableUniqueIndexNames();
    if (!unavailableUnique.isEmpty())
      return new LocalCopyDamage(DivergenceCause.UNAVAILABLE_UNIQUE_INDEX, unavailableUnique);
    return null;
  }

  /** One line for a log message or an exception. */
  String describe() {
    return switch (cause) {
      case LOCAL_RECOVERY_INCOMPLETE -> "its crash recovery did not replay every transaction";
      case EMPTY_DICTIONARY -> "its schema dictionary is empty although the schema declares types";
      default -> "its unique indexes " + uniqueIndexNames + " have no header page";
    };
  }
}
