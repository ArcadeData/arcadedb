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
 * No open database handle exists for the requested name and the caller forbade loading one
 * (`allowLoad=false`) - either the database was never opened, or it was closed/dropped after the
 * caller last resolved it. Distinct from the generic {@link DatabaseOperationException} it extends so
 * callers that need to answer this specific condition do not have to match on every other unrelated failure
 * that type also carries. See issue #6778.
 * <p>
 * The database EXISTS and cannot serve the request now ({@link ErrorCategory#UNAVAILABLE}); the name that is no
 * database at all is the subtype {@link DatabaseNotFoundException}. Which of the states an operator or the cluster
 * put the database in is the {@link #getReason() reason}, so a client is told it even where the free-form message
 * is concealed, as HTTP conceals it in production mode (issue #10872).
 */
public class DatabaseNotAvailableException extends DatabaseOperationException {
  /** The state that keeps the database from serving a request, each with the label a client is shown for it. */
  public enum Reason {
    /** Not open, and not opened for this caller: no finer state is known. */
    UNAVAILABLE("Database is not available"),
    /** A {@code close database} command closed it, and it stays closed until {@code open database}. */
    CLOSED("Database is closed"),
    /** A restore holds its name and will replace what is on disk. */
    RESTORING("Database is being restored"),
    /** It is being dropped and its files are being deleted. */
    DROPPING("Database is being dropped"),
    /** An HA snapshot install or resync has to reconcile this node's copy before it is served. */
    RECOVERING("Database is being recovered"),
    /** It is open and readable, and refuses writes (a restore holds it, issue #10689). */
    WRITES_REFUSED("Database does not accept writes");

    private final String label;

    Reason(final String label) {
      this.label = label;
    }

    /** The short sentence a client is shown for the state: names it, and reveals nothing of the server. */
    public String label() {
      return label;
    }

    /** The reason {@code name} spells, or {@link #UNAVAILABLE} for one this version does not know, or for none. */
    public static Reason fromName(final String name) {
      if (name != null)
        for (final Reason reason : values())
          if (reason.name().equals(name))
            return reason;
      return UNAVAILABLE;
    }
  }

  private final Reason reason;

  public DatabaseNotAvailableException(final String s) {
    this(s, Reason.UNAVAILABLE);
  }

  public DatabaseNotAvailableException(final String s, final Reason reason) {
    super(s);
    this.reason = reason != null ? reason : Reason.UNAVAILABLE;
  }

  public Reason getReason() {
    return reason;
  }
}
