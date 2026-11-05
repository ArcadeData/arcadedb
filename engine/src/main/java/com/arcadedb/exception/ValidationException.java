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
 * Exception thrown when a constrain is violated.
 * <p>
 * A refusal raised by a declared constraint carries its {@link Kind} (issue #11314), the way {@link AlreadyExistsException}
 * carries the kind of object that holds a name: every refusal is the same {@link ErrorCategory#VALIDATION} to a protocol
 * that only reads the category, and a protocol with a finer vocabulary reads the kind, e.g. the Postgres wire answers a
 * NOT NULL violation {@code 23502} and a value over a maximum length {@code 22001} instead of one {@code 22023} for all.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ValidationException extends ArcadeDBException {

  /** The constraint a refused value violates. */
  public enum Kind {
    /** A MANDATORY property the record does not have. */
    MANDATORY,
    /** A NOTNULL property set to {@code null}. */
    NOT_NULL,
    /** A value under the property's MIN: a number, a date, or the size of a string, a collection or a binary. */
    MIN,
    /** A value over the property's MAX, except the length of a string or a binary, which is {@link #MAX_LENGTH}. */
    MAX,
    /** A string with more characters, or a binary with more bytes, than the property's MAX. */
    MAX_LENGTH,
    /** A value that does not match the property's REGEXP. */
    REGEXP,
    /** A change to a READONLY property of a saved record. */
    READONLY,
    /** A value of a type the property does not take: another type, a link to another type, an embedded document of another type. */
    TYPE,
    /** A string holding a character the storage charset cannot encode. */
    ENCODING;

    /**
     * Whether the refusal is a rule the schema declares on the property, which a protocol with a code for a violated constraint
     * answers with it (Bolt's {@code ConstraintValidationFailed}, MongoDB's {@code DocumentValidationFailure}). Every kind but
     * {@link #ENCODING}, a limit of the storage rather than a rule anybody declared.
     */
    public boolean isDeclaredConstraint() {
      return this != ENCODING;
    }

    /** The kind named by {@code name} ({@link #name()}), or {@code null} when it names none, e.g. sent by a newer server. */
    public static Kind fromName(final String name) {
      if (name != null)
        for (final Kind kind : values())
          if (kind.name().equals(name))
            return kind;
      return null;
    }
  }

  private final Kind kind;

  public ValidationException(final String message) {
    this(null, message);
  }

  public ValidationException(final Kind kind, final String message) {
    super(message);
    this.kind = kind;
  }

  public ValidationException(final String message, final Throwable cause) {
    super(message, cause);
    this.kind = null;
  }

  public ValidationException(final Throwable cause) {
    super(cause);
    this.kind = null;
  }

  /** The constraint the refused value violates, or {@code null} when the refusal is not about one declared constraint. */
  public Kind getKind() {
    return kind;
  }
}
