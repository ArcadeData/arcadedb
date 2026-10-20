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

import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;

/**
 * Raised by a writer that has to hand back the record it created, and so cannot answer "none", when a before-create listener
 * refused the record (issue #10085). A veto is a legitimate refusal and not a fault of the server, so it is a
 * {@link ValidationException}: every protocol classifies it as the caller's request being refused ({@link ErrorCategory#VALIDATION})
 * rather than as a broken server. A writer that can report the refusal - {@code saveReportingVeto()}, {@code createRecordReportingVeto()},
 * {@code GraphEngine.newEdgeReportingVeto()} - answers it as a value instead, which is what a caller that counts the records it
 * wrote must use. The remote drivers raise it from {@code save()} for a create or an update the server reported as vetoed, and
 * answer it as {@code false} from {@code saveReportingVeto()} (issue #10161). They raise it from {@code deleteRecord()} and
 * {@code delete()} for a vetoed delete as well (issue #10230).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RecordVetoedException extends ValidationException {
  public RecordVetoedException(final String message) {
    super(message);
  }

  /** The refusal of a create, worded the same by every writer that raises it. */
  public static RecordVetoedException ofCreate(final String typeName) {
    return new RecordVetoedException("Cannot write a record of type '" + typeName + "': a listener vetoed it");
  }

  /** The refusal of an update of an existing record. */
  public static RecordVetoedException ofUpdate(final String typeName, final RID rid) {
    return new RecordVetoedException("Cannot update record " + rid + " of type '" + typeName + "': a listener vetoed it");
  }

  /**
   * The refusal of a new edge, worded the same by the embedded engine ({@code GraphEngine.newEdge}) and by the remote drivers
   * ({@code vertex.newEdge}), so a caller reads one message whichever way the edge was asked for (issue #10373).
   */
  public static RecordVetoedException ofCreateEdge(final String edgeTypeName, final Identifiable fromVertex,
      final Identifiable toVertex) {
    return new RecordVetoedException("Cannot create edge of type '" + edgeTypeName + "' from "
        + (fromVertex != null ? fromVertex.getIdentity() : null) + " to " + (toVertex != null ? toVertex.getIdentity() : null)
        + ": a beforeCreate listener vetoed it");
  }

  /** The refusal of a delete of an existing record, raised by the remote drivers (issue #10230). */
  public static RecordVetoedException ofDelete(final RID rid) {
    return new RecordVetoedException("Cannot delete record " + rid + ": a listener vetoed it");
  }
}
