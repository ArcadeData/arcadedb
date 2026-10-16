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
 * A schema object was asked to be created, or renamed, under a name that is already taken (issue #10153): a type, a property, an
 * index, a constraint, a bucket, a trigger, a view, a function. The one type every such refusal raises, so a wire protocol answers
 * it as what it is - a conflict on the identity, the caller's to resolve and never worth a retry - instead of as a server fault
 * (HTTP 500, Postgres {@code XX000}) or, worse, as the opposite of what happened (the {@code SCHEMA} category's "the schema does not
 * define this", Postgres {@code 42P01} undefined_table).
 * <p>
 * A {@link SchemaException} because that is what the schema API raised for these before, so an embedded caller catching it keeps
 * working. The SQL statements used to pre-check the same names with a {@link CommandExecutionException} of their own, which made
 * the same refusal two different types depending on how it was asked for; they raise this one now.
 * <p>
 * {@link #getKind()} names the kind of object that HOLDS the name, which is not always the kind being created: a constraint
 * refused because an index already carries its name is {@link Kind#INDEX}, as Neo4j words it ("There already exists an index
 * called ..."). It is what lets a protocol pick its own code where it has several (Postgres {@code 42P07} for a relation,
 * {@code 42701} for a column), and it may be {@code null} when a client rebuilt the exception from an answer that did not carry it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlreadyExistsException extends SchemaException {

  /** The kind of schema object that holds the name. */
  public enum Kind {
    TYPE("Type"),
    PROPERTY("Property"),
    INDEX("Index"),
    CONSTRAINT("Constraint"),
    BUCKET("Bucket"),
    TRIGGER("Trigger"),
    MATERIALIZED_VIEW("Materialized view"),
    CONTINUOUS_AGGREGATE("Continuous aggregate"),
    GRAPH_ANALYTICAL_VIEW("Graph analytical view"),
    FUNCTION("Function"),
    FUNCTION_LIBRARY("Function library");

    private final String label;

    Kind(final String label) {
      this.label = label;
    }

    /** The kind as a sentence starts with it, e.g. {@code "Materialized view"}. */
    public String getLabel() {
      return label;
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

  public AlreadyExistsException(final Kind kind, final String message) {
    super(message);
    this.kind = kind;
  }

  /** The kind of object that holds the name, or {@code null} when not known. */
  public Kind getKind() {
    return kind;
  }

  /** The short client-facing label for this conflict, e.g. {@code "Type already exists"}, that no production mode conceals. */
  public String getLabel() {
    return (kind != null ? kind.getLabel() : "Schema object") + " already exists";
  }
}
