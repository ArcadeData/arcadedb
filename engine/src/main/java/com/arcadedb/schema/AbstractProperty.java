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
package com.arcadedb.schema;

import com.arcadedb.database.Database;
import com.arcadedb.database.Record;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.Index;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.serializer.json.JSONObject;

import java.util.*;
import java.util.logging.Level;

public abstract class AbstractProperty implements Property {
  protected final        DocumentType        owner;
  protected final        String              name;
  protected final        Type                type;
  protected final        int                 id;
  protected              Map<String, Object> custom          = new HashMap<>();
  protected              boolean             readonly        = false;
  protected              boolean             mandatory       = false;
  protected              boolean             notNull         = false;
  protected              boolean             hidden          = false;
  // Volatile because BinarySerializer.serializeProperties reads isExternal() outside the schema write lock
  // on the per-record write hot path. The schema lock serialises mutations, but a reader that came in just
  // before setExternal() flipped the bit must observe the latest value to route the value through the
  // correct write path (inline vs paired bucket). volatile is the cheapest correctness fix and matches the
  // memory-model role of {@link LocalDocumentType#ownExternalPropertyCount}'s atomic.
  protected volatile     boolean             external        = false;
  // Compression policy for EXTERNAL property values: "none" | "fast" | "max" | "auto" (legacy alias: "lz4" -> "fast").
  // STORAGE CONVENTION: null means "none" (the default), so toJSON omits the key. Read access MUST go through
  // getCompression(), which materialises null as the literal string "none". LocalProperty.setCompression
  // normalises "none" / null / "" all to null on write. Direct field reads from outside this class would see
  // null when the user wrote "none"; always use the getter.
  protected              String              compression     = null;
  protected              String              max             = null;
  protected              String              min             = null;
  protected              String              regexp          = null;
  protected              String              ofType          = null;
  protected final static Object              DEFAULT_NOT_SET = "<DEFAULT_NOT_SET>";
  // Issue #6134. volatile, and one field rather than two: a reader on the record-create hot path must never observe a
  // new default value paired with the expression compiled for the previous one (or with none at all, which would write
  // the expression's own source text into the record). Publishing the pair as one immutable value makes that
  // impossible; schema mutations themselves are serialised by the schema lock.
  protected volatile     DefaultValue        defaultValue    = DefaultValue.NOT_SET;

  /**
   * A property's default value together with the compiled form of the SQL expression it is written as (issue #6134).
   * <p>
   * A String default is an SQL expression. It is compiled once, when the default is set, and the compiled form is what
   * every record create evaluates - it used to be re-parsed per record, per defaulted property, costing ~3us each time
   * on the insert hot path. The compiled EXPRESSION is cached, never the evaluated result: {@code DEFAULT sysdate()}
   * has to produce a fresh value on every record.
   * <p>
   * <b>One compiled instance is executed concurrently</b> by every thread creating a record of the owning type, which
   * relies on {@code execute()} over the expression tree being side-effect-free. That is not a new requirement this
   * introduced: {@link com.arcadedb.query.sql.parser.StatementCache} hands the same parsed {@code Statement} to every
   * concurrent caller of a given SQL string and {@code SQLQueryEngine.parse} executes it without copying, so every
   * query in the engine already shares its AST across threads. Pinned by
   * {@code PropertyDefaultValueTest.aCompiledDefaultIsSafeToEvaluateConcurrently}.
   * <p>
   * The <i>scope</i> of the sharing is wider here, though, and that is what to weigh when choosing a function for a
   * default: a cached statement is shared only among callers of that one SQL string and only while it stays in the
   * cache, whereas this instance backs every record create of the type for as long as the default is set. A default
   * that calls a user-defined function keeping mutable state would therefore misbehave persistently rather than
   * intermittently.
   *
   * @param value      what the schema stores and {@code schema.json} round-trips: the source text for a String default,
   *                   the value itself for one set through the API, or {@link #DEFAULT_NOT_SET} when none is defined
   * @param expression the compiled expression, or {@code null} when there is nothing to evaluate - the default is
   *                   unset, is not a String, or is a pre-#6134 default already persisted in {@code schema.json} that
   *                   does not compile (see {@link #compileDefaultValue})
   */
  protected record DefaultValue(Object value, Expression expression) {
    static final DefaultValue NOT_SET = new DefaultValue(DEFAULT_NOT_SET, null);
  }

  public AbstractProperty(final DocumentType owner, final String name, final Type type, final int id) {
    this.owner = owner;
    this.name = name;
    this.type = type;
    this.id = id;
  }

  /**
   * Creates an index on this property.
   *
   * @param type   Index type between LSM_TREE and FULL_TEXT
   * @param unique true if the index is unique
   *
   * @return The index instance
   */
  @Override
  public Index createIndex(final Schema.INDEX_TYPE type, final boolean unique) {
    return owner.createTypeIndex(type, unique, name);
  }

  /**
   * Returns an index on this property or creates it if does not exist.
   *
   * @param type   Index type between LSM_TREE and FULL_TEXT
   * @param unique true if the index is unique
   *
   * @return The index instance
   */
  @Override
  public Index getOrCreateIndex(final Schema.INDEX_TYPE type, final boolean unique) {
    return owner.getSchema().buildTypeIndex(owner.getName(), new String[] { name }).withType(type).withUnique(unique)
        .withIgnoreIfExists(true).create();
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Type getType() {
    return type;
  }

  @Override
  public int getId() {
    return id;
  }

  @Override
  public Object getDefaultValueDefinition() {
    final Object value = defaultValue.value();
    return value == DEFAULT_NOT_SET ? null : value;
  }

  @Override
  public Object getDefaultValue() {
    final DefaultValue current = defaultValue;
    if (current.expression() == null)
      // NOTHING TO EVALUATE: THE DEFAULT IS UNSET, IS null, IS NOT A STRING (SET THROUGH THE API, USED VERBATIM), OR IS
      // A PRE-#6134 DEFAULT THAT DOES NOT COMPILE - IN WHICH CASE THIS RETURNS THE SOURCE TEXT, AS IT ALWAYS DID.
      // THE DEFAULT_NOT_SET SENTINEL IS NEVER HANDED OUT: IT USED TO LEAK THROUGH EVERY SCHEMA-REPORTING PATH, WHICH IS
      // WHY `SELECT FROM schema:types` PUT "<DEFAULT_NOT_SET>" IN THE `default` FIELD OF EVERY PLAIN PROPERTY.
      return current.value() == DEFAULT_NOT_SET ? null : current.value();

    final Database database = owner.getSchema().getEmbedded().getDatabase();
    try {
      final Object result = current.expression().execute((Record) null, new BasicCommandContext().setDatabase(database));
      return Type.convert(database, result, type.javaDefaultType);
    } catch (Exception e) {
      // ISSUE #6134: THIS USED TO BE SWALLOWED WITH A BARE `// IGNORE IT`, WHICH FELL THROUGH TO RETURNING THE
      // EXPRESSION'S OWN SOURCE TEXT AND WROTE IT ON EVERY RECORD OF THE TYPE. THE EXPRESSION WAS ACCEPTED AT DDL TIME
      // BECAUSE IT PARSES, SO A FAILURE HERE IS A REAL ERROR (`DEFAULT @rid` RESOLVES ONLY AGAINST A CURRENT RECORD,
      // AND A DEFAULT IS ALWAYS EVALUATED AGAINST NONE) AND MUST BE REPORTED RATHER THAN TURNED INTO GARBAGE DATA.
      throw new SchemaException(
          "Error on evaluating the default value `" + current.value() + "` defined on property '" + owner.getName() + "."
              + name + "'", e);
    }
  }

  /**
   * Compiles a default value into the SQL {@link Expression} that {@link #getDefaultValue()} evaluates on every record
   * create, rejecting - at DDL time, once - the shapes that could only ever produce garbage (issue #6134):
   * <ul>
   * <li>an expression that does not parse, which used to be stored as its own source text on every record of the type
   * with no error at DDL time, no error at insert time and no log line;</li>
   * <li>a bare identifier, which is a field reference: the default expression is always evaluated against a
   * {@code null} record, so it can never resolve and silently yielded {@code null} forever. {@code DEFAULT active} is
   * the obvious thing to type for a literal, and has to be written {@code DEFAULT 'active'}.</li>
   * </ul>
   * While the schema is still hydrating from {@code schema.json} this never throws: an existing database whose schema
   * already holds an invalid default must still open. Such a default is logged as a warning and left uncompiled, which
   * makes {@link #getDefaultValue()} return exactly what it returned before this validation existed.
   *
   * @param value    the default value to compile
   * @param database the database to parse against, passed in because the caller has already resolved it
   *
   * @return the compiled expression, or {@code null} when there is nothing to compile
   *
   * @throws SchemaException if the default cannot ever produce a value and the schema is fully loaded
   */
  protected Expression compileDefaultValue(final Object value, final Database database) {
    if (value == DEFAULT_NOT_SET || !(value instanceof String text))
      return null;

    Expression compiled = null;
    String reason = null;
    try {
      compiled = new SQLAntlrParser(database).parseExpression(text);
      if (compiled.isBaseIdentifier())
        reason = "it is a field reference, not a value: a default value is evaluated with no current record, so it would"
            + " always resolve to null. Quote it as '" + text + "' to use it as a literal";
    } catch (Exception e) {
      // A CAUSE WITH NO MESSAGE CONTRIBUTES NOTHING, SO IT IS LEFT OFF RATHER THAN RENDERED AS A TRAILING ": null".
      reason = "it cannot be parsed as an SQL expression" + (e.getMessage() == null ? "" : ": " + e.getMessage());
    }

    if (reason == null)
      return compiled;

    final String message =
        "Invalid default value `" + text + "` on property '" + owner.getName() + "." + name + "' because " + reason;

    // EVERY REJECTION LEAVES THIS METHOD AS A SchemaException, WHATEVER THE PARSER THREW: THE catch ABOVE TURNS ANY
    // Exception INTO A `reason` FIRST. OrientDBImporter.setImportedDefaultValue CATCHES EXACTLY THIS TYPE TO DECIDE
    // WHETHER TO RETRY THE VALUE QUOTED, SO THE SINGLE EXIT MATTERS - DO NOT LET ANOTHER TYPE ESCAPE FROM HERE.
    if (owner.getSchema().getEmbedded().isSchemaLoaded())
      throw new SchemaException(message);

    // SCHEMA LOAD: NEVER BLOCK THE DATABASE FROM OPENING OVER A DEFAULT THAT WAS ACCEPTED BY AN EARLIER RELEASE.
    LogManager.instance().log(this, Level.WARNING, message + (compiled == null ?
        ". Every new record will keep receiving the expression's own source text as its value" :
        ". Every new record will keep leaving the property unset"));

    // REACHED ONLY ON THE LENIENT PATH - THE STRICT ONE THREW ABOVE - AND `compiled` CARRIES THE OUTCOME OF THE TWO
    // REJECTION KINDS: A BARE IDENTIFIER STILL COMPILES, AND KEEPING IT PRESERVES THE PRE-#6134 null; AN UNPARSEABLE
    // ONE IS null HERE, WHICH IS WHAT MAKES getDefaultValue() FALL BACK TO THE SOURCE TEXT, AS IT USED TO.
    return compiled;
  }

  /**
   * Publishes a default value that carries no compiled expression, for an implementation that never evaluates one - a
   * remote property, which reports the definition the server sent and has no embedded database to evaluate against.
   */
  protected void setDefaultValueDefinition(final Object value) {
    this.defaultValue = new DefaultValue(value, null);
  }

  /**
   * The compiled form of the default value, or {@code null} when there is nothing to evaluate. Visible for testing that
   * the expression is compiled once and not re-parsed per record.
   */
  Expression getDefaultValueExpression() {
    return defaultValue.expression();
  }

  @Override
  public String getOfType() {
    return ofType;
  }

  @Override
  public boolean isReadonly() {
    return readonly;
  }

  @Override
  public boolean isMandatory() {
    return mandatory;
  }

  /**
   * Returns true if the current property has set the constraint `not null`. If true, the property cannot be null.
   */
  @Override
  public boolean isNotNull() {
    return notNull;
  }

  @Override
  public boolean isHidden() {
    return hidden;
  }

  @Override
  public boolean isExternal() {
    return external;
  }

  @Override
  public String getCompression() {
    return compression == null ? "none" : compression;
  }

  @Override
  public String getMax() {
    return max;
  }

  @Override
  public String getMin() {
    return min;
  }

  @Override
  public String getRegexp() {
    return regexp;
  }

  @Override
  public Set<String> getCustomKeys() {
    return Collections.unmodifiableSet(custom.keySet());
  }

  @Override
  public Object getCustomValue(final String key) {
    return custom.get(key);
  }

  @Override
  public JSONObject toJSON() {
    final JSONObject json = new JSONObject();

    json.put("type", type.name);

    if (ofType != null)
      json.put("of", ofType);

    final Object defValue = defaultValue.value();
    if (defValue != DEFAULT_NOT_SET)
      json.put("default", defValue);

    if (readonly)
      json.put("readonly", readonly);
    if (mandatory)
      json.put("mandatory", mandatory);
    if (notNull)
      json.put("notNull", notNull);
    if (hidden)
      json.put("hidden", hidden);
    if (external)
      json.put("external", external);
    if (compression != null && !"none".equalsIgnoreCase(compression))
      json.put("compression", compression);
    if (max != null)
      json.put("max", max);
    if (min != null)
      json.put("min", min);
    if (regexp != null)
      json.put("regexp", regexp);

    json.put("custom", new JSONObject(custom));

    return json;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final AbstractProperty property = (AbstractProperty) o;
    return id == property.id && Objects.equals(name, property.name) && Objects.equals(type, property.type) && Objects.equals(ofType,
        property.ofType);
  }

  @Override
  public int hashCode() {
    return id;
  }
}
