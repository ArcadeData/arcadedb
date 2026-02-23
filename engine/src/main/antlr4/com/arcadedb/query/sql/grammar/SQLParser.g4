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

/**
 * ANTLR4 Parser Grammar for ArcadeDB SQL
 *
 * Converted from JavaCC SQLGrammar.jjt to ANTLR4
 * Phase 2: Complete statement grammars with full details
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
parser grammar SQLParser;

options {
    tokenVocab = SQLLexer;
}

// ============================================================================
// ROOT RULES (Entry Points)
// ============================================================================

/**
 * Main entry point - parses a single SQL statement
 */
parse
    : statement SEMICOLON? EOF
    ;

/**
 * Script entry point - parses multiple SQL statements separated by semicolons
 * Uses scriptStatement to allow FOREACH/WHILE which are NOT available in regular SQL
 */
parseScript
    : (scriptStatement SEMICOLON?)* EOF
    ;

/**
 * Expression entry point - parses a standalone expression
 */
parseExpression
    : expression EOF
    ;

/**
 * Condition entry point - parses a WHERE clause condition
 */
parseCondition
    : whereClause EOF
    ;

// ============================================================================
// STATEMENT DISPATCHER
// ============================================================================

/**
 * Top-level statement rule - dispatches to specific statement types
 * Uses labeled alternatives for ANTLR visitor pattern
 */
statement
    // Query Statements
    : selectStatement                                # selectStmt
    | traverseStatement                              # traverseStmt
    | matchStatement                                 # matchStmt

    // DML Statements
    | insertStatement                                # insertStmt
    | updateStatement                                # updateStmt
    | deleteStatement                                # deleteStmt
    | deleteFunctionStatement                        # deleteFunctionStmt
    | moveVertexStatement                            # moveVertexStmt

    // DDL Statements - CREATE variants
    | CREATE DOCUMENT TYPE createTypeBody            # createDocumentTypeStmt
    | CREATE VERTEX TYPE createTypeBody              # createVertexTypeStmt
    | CREATE EDGE TYPE createEdgeTypeBody            # createEdgeTypeStmt
    | CREATE PROPERTY createPropertyBody             # createPropertyStmt
    | CREATE INDEX createIndexBody                   # createIndexStmt
    | CREATE BUCKET createBucketBody                 # createBucketStmt
    | CREATE VERTEX createVertexBody                 # createVertexStmt
    | CREATE EDGE createEdgeBody                     # createEdgeStmt
    | CREATE TRIGGER createTriggerBody               # createTriggerStmt
    | CREATE MATERIALIZED VIEW createMaterializedViewBody   # createMaterializedViewStmt
    | CREATE TIMESERIES TYPE createTimeSeriesTypeBody       # createTimeSeriesTypeStmt
    | CREATE CONTINUOUS AGGREGATE createContinuousAggregateBody  # createContinuousAggregateStmt

    // DDL Statements - ALTER variants
    | ALTER TYPE alterTypeBody                       # alterTypeStmt
    | ALTER PROPERTY alterPropertyBody               # alterPropertyStmt
    | ALTER BUCKET alterBucketBody                   # alterBucketStmt
    | ALTER DATABASE alterDatabaseBody               # alterDatabaseStmt
    | ALTER MATERIALIZED VIEW alterMaterializedViewBody     # alterMaterializedViewStmt
    | ALTER TIMESERIES TYPE alterTimeSeriesTypeBody        # alterTimeSeriesTypeStmt

    // DDL Statements - DROP variants
    | DROP TYPE dropTypeBody                         # dropTypeStmt
    | DROP PROPERTY dropPropertyBody                 # dropPropertyStmt
    | DROP INDEX dropIndexBody                       # dropIndexStmt
    | DROP BUCKET dropBucketBody                     # dropBucketStmt
    | DROP TRIGGER dropTriggerBody                   # dropTriggerStmt
    | DROP MATERIALIZED VIEW dropMaterializedViewBody       # dropMaterializedViewStmt
    | DROP CONTINUOUS AGGREGATE dropContinuousAggregateBody # dropContinuousAggregateStmt

    // DDL Statements - TRUNCATE variants
    | TRUNCATE TYPE truncateTypeBody                 # truncateTypeStmt
    | TRUNCATE BUCKET truncateBucketBody             # truncateBucketStmt
    | TRUNCATE RECORD truncateRecordBody             # truncateRecordStmt

    // Materialized View Refresh
    | REFRESH MATERIALIZED VIEW refreshMaterializedViewBody # refreshMaterializedViewStmt

    // Continuous Aggregate Refresh
    | REFRESH CONTINUOUS AGGREGATE refreshContinuousAggregateBody # refreshContinuousAggregateStmt

    // Index Management
    | rebuildIndexStatement                          # rebuildIndexStmt

    // Transaction Statements
    | beginStatement                                 # beginStmt
    | commitStatement                                # commitStmt
    | rollbackStatement                              # rollbackStmt

    // Control Flow Statements
    | letStatement                                   # letStmt
    | returnStatement                                # returnStmt
    | ifStatement                                    # ifStmt
    | setGlobalStatement                             # setGlobalStmt

    // Utility Statements
    | explainStatement                               # explainStmt
    | profileStatement                               # profileStmt
    | lockStatement                                  # lockStmt
    | sleepStatement                                 # sleepStmt
    | consoleStatement                               # consoleStmt

    // Database Management
    | importDatabaseStatement                        # importDatabaseStmt
    | exportDatabaseStatement                        # exportDatabaseStmt
    | backupDatabaseStatement                        # backupDatabaseStmt
    | checkDatabaseStatement                         # checkDatabaseStmt
    | alignDatabaseStatement                         # alignDatabaseStmt

    // Function Management
    | defineFunctionStatement                        # defineFunctionStmt
    ;

/**
 * Script statement rule - includes all regular statements PLUS script-only control flow
 * This rule is ONLY used by parseScript, not by parse (regular SQL)
 * FOREACH, WHILE, BREAK, and function calls are script-only and NOT available in regular SQL
 */
scriptStatement
    : statement                                      # scriptRegularStmt
    | foreachStatement                               # foreachStmt
    | whileStatement                                 # whileStmt
    | breakStatement                                 # breakStmt
    | functionCall                                   # functionStmt
    ;

// ============================================================================
// QUERY STATEMENTS
// ============================================================================

/**
 * SELECT statement
 * SELECT [DISTINCT] [projection] [FROM target] [LET clause] [WHERE condition]
 * [GROUP BY] [ORDER BY] [UNWIND] [SKIP_KW n] [LIMIT n] [TIMEOUT n]
 */
selectStatement
    : SELECT projection?
      (FROM fromClause)?
      letClause?
      (WHERE whereClause)?
      groupBy?
      orderBy?
      unwind?
      (
          skip limit?
        | limit skip?
      )?
      timeout?
    ;

/**
 * TRAVERSE statement
 * TRAVERSE [fields] FROM target [MAXDEPTH n] [WHILE condition]
 * [LIMIT n] [STRATEGY strategy]
 */
traverseStatement
    : TRAVERSE (traverseProjectionItem (COMMA traverseProjectionItem)*)?
      FROM fromClause
      (MAXDEPTH pInteger)?
      (WHILE whereClause)?
      limit?
      (STRATEGY (DEPTH_FIRST | BREADTH_FIRST))?
    ;

/**
 * MATCH statement
 * MATCH pattern [, pattern]* RETURN [DISTINCT] items
 * [GROUP BY] [ORDER BY] [UNWIND] [SKIP_KW] [LIMIT]
 */
matchStatement
    : MATCH matchExpression (COMMA (NOT? matchExpression))*
      RETURN DISTINCT? matchReturnItem (COMMA matchReturnItem)*
      groupBy?
      orderBy?
      unwind?
      skip?
      limit?
    ;

matchReturnItem
    : expression nestedProjection? (AS identifier)?
    ;

matchExpression
    : matchPathItem (DOT matchPathItem)*
    ;

matchPathItem
    : matchFilter?
      (matchMethod)*
    ;

matchMethod
    : DOT matchMethodCall matchProperties?                                     // .out('Friend'){as:x}
    | DOT LPAREN nestedMatchPath RPAREN matchProperties?                       // .(out().in(){...}){as:x}
    | (MINUS | ARROW_LEFT) identifier (MINUS | ARROW_RIGHT) matchProperties?  // -Friend->{as:x}
    | DECR GT matchProperties?                                                 // -->{as:x} anonymous outgoing
    | ARROW_LEFT MINUS matchProperties?                                        // <--{as:x} anonymous incoming
    | DECR matchProperties?                                                    // --{as:x} anonymous bidirectional
    ;

nestedMatchPath
    : nestedMatchMethod+
    ;

nestedMatchMethod
    : identifier LPAREN (STAR | expression (COMMA expression)*)? RPAREN matchProperties?  // out('X'){...} without method chains
    | DOT identifier LPAREN (STAR | expression (COMMA expression)*)? RPAREN matchProperties?  // .out('X'){...}
    | identifier matchProperties?                                              // methodName{...}
    | (MINUS | ARROW_LEFT) identifier (MINUS | ARROW_RIGHT) matchProperties?  // -Friend->{as:x}
    | DECR GT matchProperties?                                                 // -->{as:x}
    | ARROW_LEFT MINUS matchProperties?                                        // <--{as:x}
    | DECR matchProperties?                                                    // --{as:x}
    ;

matchMethodCall
    : functionCall
    | identifier
    ;

matchProperties
    : LBRACE (matchFilterItem (COMMA matchFilterItem)*)? RBRACE
    ;

matchFilter
    : functionCall
    | identifier
    | matchProperties
    ;

matchFilterItem
    : matchFilterItemKey COLON expression
    | BUCKET_IDENTIFIER              // bucket:name (complete token)
    | BUCKET_NUMBER_IDENTIFIER        // bucket:123 (complete token)
    ;

matchFilterItemKey
    : identifier
    | TYPE        // type: Person
    | TYPES       // types: [Person, Company]
    | BUCKET      // bucket: bucketName
    | AS          // as: alias
    | WHERE       // where: (condition)
    | WHILE       // while: (condition)
    | MAXDEPTH    // maxdepth: 3
    | OPTIONAL    // optional: true
    | RID         // rid: #1:1
    | PATH_ALIAS  // pathAlias: varName
    | DEPTH_ALIAS // depthAlias: varName
    ;

traverseProjectionItem
    : expression (AS? identifier)?
    ;

nestedProjection
    : COLON LBRACE nestedProjectionItem (COMMA nestedProjectionItem)* RBRACE
    ;

nestedProjectionItem
    : (STAR | (BANG? expression STAR?) ) nestedProjection? (AS identifier)?
    ;

// ============================================================================
// DML STATEMENTS
// ============================================================================

/**
 * INSERT statement
 * INSERT INTO target [(fields)] VALUES (values) | SET field=value | CONTENT {...} | FROM query
 * [RETURN projection] [UNSAFE]
 */
insertStatement
    : INSERT INTO (identifier (BUCKET identifier)? | bucketIdentifier)
      insertBody?
      (RETURN projection)?
      (FROM? (selectStatement | LPAREN selectStatement RPAREN))?
      UNSAFE?
    ;

insertBody
    : LPAREN identifier (COMMA identifier)* RPAREN
      VALUES LPAREN expression (COMMA expression)* RPAREN
      (COMMA LPAREN expression (COMMA expression)* RPAREN)*
    | SET insertSetItem (COMMA insertSetItem)*
    | CONTENT (json | jsonArray | inputParameter)
    ;

insertSetItem
    : identifier EQ expression
    ;

jsonArray
    : LBRACKET (json (COMMA json)*)? RBRACKET
    ;

/**
 * UPDATE statement
 * UPDATE target [SET...] [ADD...] [PUT...] [REMOVE...] [INCREMENT...] [CONTENT...]
 * [UPSERT] [APPLY DEFAULTS] [RETURN BEFORE|AFTER|COUNT projection] [WHERE condition] [LIMIT n] [TIMEOUT n]
 */
updateStatement
    : UPDATE fromClause
      updateOperation+
      UPSERT?
      (APPLY DEFAULTS)?
      (RETURN (BEFORE | AFTER | COUNT) projection?)?
      (WHERE whereClause)?
      limit?
      timeout?
    ;

updateOperation
    : SET updateItem (COMMA updateItem)*
    | ADD updateItem (COMMA updateItem)*
    | PUT updatePutItem (COMMA updatePutItem)*
    | REMOVE updateRemoveItem (COMMA updateRemoveItem)*
    | INCREMENT updateIncrementItem (COMMA updateIncrementItem)*
    | MERGE expression
    | CONTENT (json | jsonArray | inputParameter)
    ;

updateItem
    : identifier modifier? (EQ | PLUSASSIGN | MINUSASSIGN | STARASSIGN | SLASHASSIGN) expression
    ;

updatePutItem
    : identifier EQ expression COMMA expression
    ;

updateRemoveItem
    : expression (EQ expression)?
    ;

updateIncrementItem
    : identifier modifier? EQ expression
    ;

/**
 * DELETE statement
 * DELETE [VERTEX] FROM target [RETURN BEFORE] [WHERE condition] [LIMIT n] [UNSAFE]
 */
deleteStatement
    : DELETE VERTEX?
      FROM fromClause
      (RETURN BEFORE)?
      (WHERE whereClause)?
      limit?
      UNSAFE?
    ;

/**
 * DELETE FUNCTION statement
 * Grammar: DELETE FUNCTION identifier DOT identifier
 */
deleteFunctionStatement
    : DELETE FUNCTION identifier DOT identifier
    ;

/**
 * MOVE VERTEX statement
 * MOVE VERTEX source TO target [SET ...] [MERGE ...]
 * Supports: MOVE VERTEX expr TO type:TypeName or TO TypeName or TO BUCKET:name
 */
moveVertexStatement
    : MOVE VERTEX expression TO (TYPE COLON identifier | BUCKET COLON identifier | identifier)
      (SET updateItem (COMMA updateItem)*)?
      (MERGE expression)?
      (BATCH expression)?
    ;

// ============================================================================
// DDL STATEMENTS - CREATE
// ============================================================================

/**
 * CREATE TYPE body (common for DOCUMENT, VERTEX, EDGE types)
 */
createTypeBody
    : identifier
      (IF NOT EXISTS)?
      (EXTENDS identifier (COMMA identifier)*)?
      (BUCKET bucketIdentifier (COMMA bucketIdentifier)*)?
      (BUCKETS INTEGER_LITERAL)?
      (PAGESIZE INTEGER_LITERAL)?
    ;

/**
 * CREATE TIMESERIES TYPE body
 * Example: CREATE TIMESERIES TYPE SensorData TIMESTAMP ts TAGS (sensor_id STRING) FIELDS (temperature DOUBLE, humidity DOUBLE) SHARDS 4 RETENTION 90 DAYS COMPACTION_INTERVAL 1 HOURS
 */
createTimeSeriesTypeBody
    : identifier
      (IF NOT EXISTS)?
      (TIMESTAMP identifier)?
      (TAGS LPAREN tsTagColumnDef (COMMA tsTagColumnDef)* RPAREN)?
      (FIELDS LPAREN tsFieldColumnDef (COMMA tsFieldColumnDef)* RPAREN)?
      (SHARDS INTEGER_LITERAL)?
      (RETENTION INTEGER_LITERAL (DAYS | HOURS | MINUTES)?)?
      (COMPACTION_INTERVAL INTEGER_LITERAL (DAYS | HOURS | MINUTES)?)?
    ;

tsTagColumnDef
    : identifier identifier
    ;

tsFieldColumnDef
    : identifier identifier
    ;

/**
 * ALTER TIMESERIES TYPE body - add or drop downsampling policy
 * Example: ALTER TIMESERIES TYPE SensorData ADD DOWNSAMPLING POLICY AFTER 7 DAYS GRANULARITY 1 HOURS AFTER 30 DAYS GRANULARITY 1 DAYS
 * Example: ALTER TIMESERIES TYPE SensorData DROP DOWNSAMPLING POLICY
 */
alterTimeSeriesTypeBody
    : identifier ADD DOWNSAMPLING POLICY downsamplingTierClause+
    | identifier DROP DOWNSAMPLING POLICY
    ;

downsamplingTierClause
    : AFTER INTEGER_LITERAL tsTimeUnit GRANULARITY INTEGER_LITERAL tsTimeUnit
    ;

tsTimeUnit
    : DAYS
    | HOURS
    | MINUTES
    | HOUR
    | MINUTE
    ;

/**
 * CREATE EDGE TYPE body (supports UNIDIRECTIONAL)
 */
createEdgeTypeBody
    : identifier
      (IF NOT EXISTS)?
      (EXTENDS identifier (COMMA identifier)*)?
      UNIDIRECTIONAL?
      (BUCKET bucketIdentifier (COMMA bucketIdentifier)*)?
      (BUCKETS INTEGER_LITERAL)?
      (PAGESIZE INTEGER_LITERAL)?
    ;

/**
 * Bucket identifier - can be integer ID, bucket name, BUCKET:name/BUCKET:id syntax,
 * or BUCKET:parameter for parameterized bucket names
 */
bucketIdentifier
    : INTEGER_LITERAL
    | identifier
    | BUCKET_IDENTIFIER
    | BUCKET_NUMBER_IDENTIFIER
    | BUCKET_NAMED_PARAM
    | BUCKET_POSITIONAL_PARAM
    ;

/**
 * CREATE PROPERTY statement
 * CREATE PROPERTY Type.property [IF NOT EXISTS] propertyType [OF ofType] [(attributes)]
 */
createPropertyBody
    : identifier DOT identifier (IF NOT EXISTS)? propertyType (LPAREN propertyAttributes RPAREN)?
    ;

propertyAttributes
    : propertyAttribute (COMMA propertyAttribute)*
    ;

propertyAttribute
    : identifier expression?
    ;

/**
 * Property type specification - supports simple types and complex types like LIST OF INTEGER
 */
propertyType
    : identifier (OF identifier)?
    ;

/**
 * CREATE INDEX statement
 * Named: CREATE INDEX identifier ON TYPE? identifier (properties) [UNIQUE|NOTUNIQUE|FULL_TEXT] [NULL_STRATEGY ...] [ENGINE ...] [METADATA {...}]
 * Unnamed: CREATE INDEX ON identifier (properties) [UNIQUE|NOTUNIQUE|FULL_TEXT] [NULL_STRATEGY ...] [ENGINE ...] [METADATA {...}]
 */
createIndexBody
    : identifier? (IF NOT EXISTS)? ON TYPE? identifier LPAREN indexProperty (COMMA indexProperty)* RPAREN
      indexType?
      (NULL_STRATEGY identifier)?
      (METADATA json)?
      (ENGINE identifier)?
    ;

indexProperty
    : identifier (BY (KEY | VALUE | ITEM))?
    ;

indexType
    : UNIQUE | NOTUNIQUE | FULL_TEXT | identifier
    ;

/**
 * CREATE BUCKET statement
 */
createBucketBody
    : identifier
      (IF NOT EXISTS)?
    ;

/**
 * CREATE VERTEX statement (instance creation)
 * Supports VALUES, SET, and CONTENT clauses similar to INSERT
 */
createVertexBody
    : identifier?
      ( LPAREN identifier (COMMA identifier)* RPAREN
        VALUES LPAREN expression (COMMA expression)* RPAREN
        (COMMA LPAREN expression (COMMA expression)* RPAREN)*
      | SET updateItem (COMMA updateItem)*
      | CONTENT (json | jsonArray | inputParameter)
      )?
    ;

/**
 * CREATE EDGE statement (instance creation)
 */
createEdgeBody
    : identifier?
      FROM fromItem TO fromItem
      (IF NOT EXISTS)?
      (SET updateItem (COMMA updateItem)*)?
      (CONTENT expression)?
      UNIDIRECTIONAL?
      (RETRY INTEGER_LITERAL)?
      (WAIT INTEGER_LITERAL)?
    ;

// ============================================================================
// DDL STATEMENTS - ALTER
// ============================================================================

alterTypeBody
    : identifier alterTypeItem (COMMA alterTypeItem)*
    ;

alterTypeItem
    : NAME identifier
    | SUPERTYPE ((PLUS | MINUS)? identifier (COMMA (PLUS | MINUS)? identifier)*)
    | BUCKETSELECTIONSTRATEGY identifier
    | BUCKET ((PLUS | MINUS) identifier)+
    | CUSTOM identifier EQ expression
    | ALIASES (identifier (COMMA identifier)* | NULL)
    ;

alterPropertyBody
    : identifier DOT identifier alterPropertyItem (COMMA alterPropertyItem)*
    ;

alterPropertyItem
    : NAME identifier
    | TYPE propertyType
    | CUSTOM identifier EQ expression
    | identifier expression?  // Property attributes (MANDATORY, READONLY, REGEXP, etc.)
    ;

alterBucketBody
    : identifier alterBucketItem (COMMA alterBucketItem)*
    ;

alterBucketItem
    : NAME identifier
    | CUSTOM identifier EQ expression
    ;

alterDatabaseBody
    : alterDatabaseItem (COMMA alterDatabaseItem)*
    ;

alterDatabaseItem
    : identifier expression
    ;

// ============================================================================
// DDL STATEMENTS - DROP
// ============================================================================

dropTypeBody
    : (identifier | inputParameter) (IF EXISTS)? UNSAFE?
    ;

dropPropertyBody
    : identifier DOT identifier (IF EXISTS)?
    ;

dropIndexBody
    : (identifier | STAR) (IF EXISTS)?
    ;

dropBucketBody
    : identifier (IF EXISTS)?
    ;

// ============================================================================
// TRIGGER MANAGEMENT
// ============================================================================

/**
 * CREATE TRIGGER statement
 * Syntax: CREATE TRIGGER [IF NOT EXISTS] name (BEFORE|AFTER) (CREATE|READ|UPDATE|DELETE)
 *         ON [TYPE] typeName (EXECUTE SQL 'statement' | EXECUTE JAVASCRIPT 'code' | EXECUTE JAVA 'className')
 */
createTriggerBody
    : (IF NOT EXISTS)? identifier
      triggerTiming triggerEvent
      ON TYPE? identifier
      triggerAction
    ;

triggerTiming
    : BEFORE
    | AFTER
    ;

triggerEvent
    : CREATE
    | READ
    | UPDATE
    | DELETE
    ;

triggerAction
    : EXECUTE identifier STRING_LITERAL
    ;

/**
 * DROP TRIGGER statement
 * Syntax: DROP TRIGGER [IF EXISTS] name
 */
dropTriggerBody
    : (IF EXISTS)? identifier
    ;

// ============================================================================
// DDL STATEMENTS - MATERIALIZED VIEW
// ============================================================================

/**
 * CREATE MATERIALIZED VIEW statement
 * Syntax: CREATE MATERIALIZED VIEW [IF NOT EXISTS] name AS selectStatement [REFRESH MANUAL|INCREMENTAL|EVERY n SECOND|MINUTE|HOUR] [BUCKETS n]
 */
createMaterializedViewBody
    : (IF NOT EXISTS)? identifier
      AS selectStatement
      materializedViewRefreshClause?
      (BUCKETS INTEGER_LITERAL)?
    ;

materializedViewRefreshClause
    : REFRESH MANUAL
    | REFRESH INCREMENTAL
    | REFRESH EVERY INTEGER_LITERAL materializedViewTimeUnit
    ;

materializedViewTimeUnit
    : SECOND | MINUTE | HOUR
    ;

/**
 * DROP MATERIALIZED VIEW statement
 * Syntax: DROP MATERIALIZED VIEW [IF EXISTS] name
 */
dropMaterializedViewBody
    : (IF EXISTS)? identifier
    ;

/**
 * REFRESH MATERIALIZED VIEW statement
 * Syntax: REFRESH MATERIALIZED VIEW name
 */
refreshMaterializedViewBody
    : identifier
    ;

/**
 * ALTER MATERIALIZED VIEW statement
 * Syntax: ALTER MATERIALIZED VIEW name REFRESH ...
 */
alterMaterializedViewBody
    : identifier materializedViewRefreshClause
    ;

// ============================================================================
// DDL STATEMENTS - CONTINUOUS AGGREGATE
// ============================================================================

/**
 * CREATE CONTINUOUS AGGREGATE statement
 * Syntax: CREATE CONTINUOUS AGGREGATE [IF NOT EXISTS] name AS selectStatement
 */
createContinuousAggregateBody
    : (IF NOT EXISTS)? identifier
      AS selectStatement
    ;

/**
 * DROP CONTINUOUS AGGREGATE statement
 * Syntax: DROP CONTINUOUS AGGREGATE [IF EXISTS] name
 */
dropContinuousAggregateBody
    : (IF EXISTS)? identifier
    ;

/**
 * REFRESH CONTINUOUS AGGREGATE statement
 * Syntax: REFRESH CONTINUOUS AGGREGATE name
 */
refreshContinuousAggregateBody
    : identifier
    ;

// ============================================================================
// DDL STATEMENTS - TRUNCATE
// ============================================================================

truncateTypeBody
    : identifier (POLYMORPHIC | UNSAFE)*
    ;

truncateBucketBody
    : identifier UNSAFE?
    ;

truncateRecordBody
    : rid (COMMA rid)*
    ;

// ============================================================================
// INDEX MANAGEMENT
// ============================================================================

rebuildIndexStatement
    : REBUILD INDEX (identifier | STAR) (WITH identifier EQ expression (COMMA identifier EQ expression)*)?
    ;

// ============================================================================
// CONTROL FLOW STATEMENTS
// ============================================================================

/**
 * LET statement
 * LET $variable = expression
 */
letStatement
    : LET letItem (COMMA letItem)*
    ;

letItem
    : identifier EQ (expression | statement | LPAREN statement RPAREN)
    ;

/**
 * SET GLOBAL statement - sets a database-scoped transient variable
 * SET GLOBAL $varname = expression
 */
setGlobalStatement
    : SET GLOBAL identifier EQ expression
    ;

/**
 * RETURN statement
 */
returnStatement
    : RETURN expression?
    ;

/**
 * IF statement
 * IF (condition) { statements } [ELSE { statements }]
 */
ifStatement
    : IF LPAREN orBlock RPAREN LBRACE (scriptStatement SEMICOLON?)* RBRACE
      (ELSE LBRACE (scriptStatement SEMICOLON?)* RBRACE)?
    ;

/**
 * FOREACH statement (script-only)
 * FOREACH (variable IN expression) { statements }
 */
foreachStatement
    : FOREACH LPAREN identifier IN expression RPAREN LBRACE (scriptStatement SEMICOLON?)* RBRACE
    ;

/**
 * WHILE statement (script-only)
 * WHILE (condition) { statements }
 */
whileStatement
    : WHILE LPAREN orBlock RPAREN LBRACE (scriptStatement SEMICOLON?)* RBRACE
    ;

/**
 * BREAK statement (script-only)
 * Breaks out of a FOREACH or WHILE loop
 */
breakStatement
    : BREAK
    ;

// ============================================================================
// TRANSACTION STATEMENTS
// ============================================================================

/**
 * BEGIN statement
 * BEGIN [ISOLATION isolation_level]
 */
beginStatement
    : BEGIN (ISOLATION identifier)?
    ;

/**
 * COMMIT statement
 * COMMIT [RETRY n [ELSE {statements} [AND] (FAIL|CONTINUE)]]
 */
commitStatement
    : COMMIT (RETRY INTEGER_LITERAL (ELSE (LBRACE (scriptStatement SEMICOLON?)* RBRACE)? (AND? (FAIL | CONTINUE))?)?)?
    ;

/**
 * ROLLBACK statement
 */
rollbackStatement
    : ROLLBACK
    ;

// ============================================================================
// UTILITY STATEMENTS
// ============================================================================

explainStatement
    : EXPLAIN statement
    ;

profileStatement
    : PROFILE statement
    ;

/**
 * LOCK statement
 * LOCK TYPE type1, type2, ...
 * LOCK BUCKET bucket1, bucket2, ...
 */
lockStatement
    : LOCK (TYPE | BUCKET) identifier (COMMA identifier)*
    ;

sleepStatement
    : SLEEP expression
    ;

consoleStatement
    : CONSOLE DOT identifier expression
    ;

// ============================================================================
// DATABASE MANAGEMENT
// ============================================================================

importDatabaseStatement
    : IMPORT DATABASE (url)? (WITH settingList)?
    ;

exportDatabaseStatement
    : EXPORT DATABASE url (WITH settingList)?
    ;

backupDatabaseStatement
    : BACKUP DATABASE (url)? (WITH settingList)?
    ;

checkDatabaseStatement
    : CHECK DATABASE
      (TYPE identifier (COMMA identifier)*)?
      (BUCKET (identifier | INTEGER_LITERAL) (COMMA (identifier | INTEGER_LITERAL))*)?
      (FIX)?
      (COMPRESS)?
    ;

alignDatabaseStatement
    : ALIGN DATABASE
    ;

// ============================================================================
// FUNCTION MANAGEMENT
// ============================================================================

defineFunctionStatement
    : DEFINE FUNCTION identifier DOT identifier STRING_LITERAL
      (PARAMETERS LBRACKET parameterList RBRACKET)?
      (LANGUAGE identifier)?
    ;

parameterList
    : identifier (COMMA identifier)*
    ;

// ============================================================================
// CLAUSES
// ============================================================================

/**
 * FROM clause - specifies data source(s)
 */
fromClause
    : fromItem
    ;

fromItem
    : rid (COMMA rid)*                                               # fromRids
    | LBRACKET rid (COMMA rid)* RBRACKET                            # fromRidArray
    | LBRACKET inputParameter (COMMA inputParameter)* RBRACKET      # fromParamArray
    | inputParameter                                                 # fromParam
    | BUCKET_IDENTIFIER                                             # fromBucket
    | BUCKET_NUMBER_IDENTIFIER                                      # fromBucket
    | BUCKET_NAMED_PARAM                                            # fromBucketParameter
    | BUCKET_POSITIONAL_PARAM                                       # fromBucketParameter
    | bucketList                                                    # fromBucketList
    | indexIdentifier                                               # fromIndex
    | schemaIdentifier                                              # fromSchema
    | LPAREN statement RPAREN (modifier)* (AS? identifier)?         # fromSubquery
    | identifier (modifier)* (AS? identifier)?                      # fromIdentifier
    ;

bucketList
    : BUCKET COLON LBRACKET identifier (COMMA identifier)* RBRACKET
    ;

indexIdentifier
    : INDEX_COLON identifier
    | INDEXVALUES_IDENTIFIER
    | INDEXVALUESASC_IDENTIFIER
    | INDEXVALUESDESC_IDENTIFIER
    ;

schemaIdentifier
    : SCHEMA_IDENTIFIER
    ;

/**
 * LET clause - defines variables
 */
letClause
    : LET letItem (COMMA letItem)*
    ;

/**
 * WHERE clause - filters records
 */
whereClause
    : orBlock
    ;

orBlock
    : andBlock (OR andBlock)*
    ;

andBlock
    : notBlock (AND notBlock)*
    ;

notBlock
    : NOT? conditionBlock
    ;

conditionBlock
    : TRUE                                                              # trueCondition
    | FALSE                                                             # falseCondition
    | NULL                                                              # nullCondition
    | expression IS NOT? NULL                                           # isNullCondition
    | expression IS NOT? DEFINED                                        # isDefinedCondition
    | expression NOT? IN (LPAREN (expression (COMMA expression)*)? RPAREN | expression) # inCondition
    | expression NOT? BETWEEN expression AND expression                 # betweenCondition
    | expression CONTAINS (LPAREN whereClause RPAREN | expression)      # containsCondition
    | expression CONTAINSALL (LPAREN whereClause RPAREN | expression)   # containsAllCondition
    | expression CONTAINSANY (LPAREN whereClause RPAREN | expression)   # containsAnyCondition
    | expression CONTAINSKEY expression                                 # containsKeyCondition
    | expression CONTAINSVALUE expression                               # containsValueCondition
    | expression CONTAINSTEXT expression                                # containsTextCondition
    | expression NOT? LIKE expression                                    # likeCondition
    | expression NOT? ILIKE expression                                  # ilikeCondition
    | expression MATCHES expression                                     # matchesCondition
    | expression INSTANCEOF (identifier | STRING_LITERAL)               # instanceofCondition
    | expression comparisonOperator expression                          # comparisonCondition
    | LPAREN whereClause RPAREN                                         # parenthesizedCondition
    ;

comparisonOperator
    : EQ | EQEQ | NE | NEQ | LT | GT | LE | GE | NSEQ
    | NEAR | WITHIN
    ;

/**
 * GROUP BY clause
 */
groupBy
    : GROUP_BY expression (COMMA expression)*
    ;

/**
 * ORDER BY clause
 */
orderBy
    : ORDER_BY orderByItem (COMMA orderByItem)*
    ;

orderByItem
    : expression orderDirection?
    ;

/**
 * Order direction - supports ASC/DESC keywords, TRUE/FALSE boolean alternatives,
 * and input parameters (for parameterized queries).
 * TRUE = ASC (ascending, the default), FALSE = DESC (descending).
 * The boolean/parameter alternatives allow setting sort direction via HTTP API params.
 */
orderDirection
    : ASC | TRUE | DESC | FALSE | inputParameter
    ;

/**
 * UNWIND clause
 */
unwind
    : UNWIND expression (AS? identifier)?
    ;

/**
 * SKIP_KW clause
 */
skip
    : SKIP_KW expression
    ;

/**
 * LIMIT clause
 */
limit
    : LIMIT expression
    ;

/**
 * TIMEOUT clause
 */
timeout
    : TIMEOUT expression
    ;

/**
 * Projection - list of expressions to return
 */
projection
    : DISTINCT? (STAR | projectionItem) (COMMA projectionItem)*
    ;

projectionItem
    : BANG? expression nestedProjection? (AS? identifier)?
    ;

// ============================================================================
// EXPRESSIONS
// ============================================================================

/**
 * Expression hierarchy with left recursion for operator precedence
 * Includes null coalescing operator (??)
 * Note: parenthesizedWhereExpr is placed LAST among non-left-recursive alternatives
 * so ANTLR tries mathExpression first for pure expressions like (1 + 2).
 */
expression
    : expression SC_OR expression                                       # arrayConcat
    | expression NULL_COALESCING expression                             # nullCoalescing
    | mathExpression                                                    # mathExpr
    | NULL                                                              # nullLiteral
    | TRUE                                                              # trueLiteral
    | FALSE                                                             # falseLiteral
    | rid                                                               # ridLiteral
    | json                                                              # jsonLiteral
    | LPAREN whereClause RPAREN                                         # parenthesizedWhereExpr
    ;

/**
 * Math expressions with operator precedence
 * Precedence (highest to lowest):
 * 1. Unary +/-
 * 2. *, /, %
 * 3. +, -
 * 4. <<, >>, >>>
 * 5. &
 * 6. ^
 * 7. |
 */
mathExpression
    : (PLUS | MINUS) mathExpression                                    # unary
    | mathExpression (STAR | SLASH | REM) mathExpression               # multiplicative
    | mathExpression (PLUS | MINUS) mathExpression                     # additive
    | mathExpression (LSHIFT | RSHIFT | RUNSIGNEDSHIFT) mathExpression # shift
    | mathExpression BIT_AND mathExpression                            # bitwiseAnd
    | mathExpression XOR mathExpression                                # bitwiseXor
    | mathExpression BIT_OR mathExpression                             # bitwiseOr
    | baseExpression                                                   # base
    ;

/**
 * Base expressions - literals, identifiers, function calls
 */
baseExpression
    : INTEGER_LITERAL                                                   # integerLiteral
    | FLOATING_POINT_LITERAL                                            # floatLiteral
    | STRING_LITERAL modifier*                                          # stringLiteral
    | CHARACTER_LITERAL modifier*                                       # charLiteral
    | INTEGER_RANGE                                                     # integerRange
    | ELLIPSIS_INTEGER_RANGE                                            # ellipsisIntegerRange
    | THIS                                                              # thisLiteral
    | identifier (DOT identifier)* methodCall* arraySelector* modifier* # identifierChain
    | functionCall                                                      # functionCallExpr
    | inputParameter modifier*                                          # inputParam
    | LPAREN statement RPAREN modifier*                                 # parenthesizedStmt
    | LPAREN expression RPAREN modifier*                                # parenthesizedExpr
    | arrayLiteral modifier*                                            # arrayLit
    | mapLiteral modifier*                                              # mapLit
    | LBRACKET expression FOR identifier IN expression (WHERE whereClause)? RBRACKET # listComprehension
    | caseExpression modifier*                                          # caseExpr
    | extendedCaseExpression modifier*                                  # extendedCaseExpr
    | NULL modifier*                                                    # nullBaseExpr
    ;

/**
 * Simple CASE expression
 * CASE WHEN condition THEN result [WHEN condition THEN result]* [ELSE result] END
 */
caseExpression
    : CASE caseAlternative+ (ELSE expression)? END
    ;

caseAlternative
    : WHEN whereClause THEN expression
    ;

/**
 * Extended CASE expression (with test expression)
 * CASE expression WHEN value THEN result [WHEN value THEN result]* [ELSE result] END
 */
extendedCaseExpression
    : CASE expression extendedCaseAlternative+ (ELSE expression)? END
    ;

extendedCaseAlternative
    : WHEN expression THEN expression
    ;

/**
 * Function call
 * Allows STAR (*) as parameter for aggregate functions like COUNT(*), SUM(*), etc.
 * Supports method call chains: out('Follows').out('Follows')
 * Supports array selectors: someFunc()[0]
 * Supports modifiers: someFunc().asString()
 * Supports nested projections: list({x:1}):{x} (processed before methodCall/arraySelector/modifier)
 */
functionCall
    : identifier LPAREN (STAR | expression (COMMA expression)*)? RPAREN nestedProjection* methodCall* arraySelector* modifier*
    ;

/**
 * Method call on an expression
 */
methodCall
    : DOT identifier LPAREN (expression (COMMA expression)*)? RPAREN
    ;

/**
 * Array selector [index] or [start..end] or [condition]
 * Supports:
 * - Single index: [0]
 * - Range: [0..5] or [0...5]
 * - Condition filter: [name = 'John']
 * - RID: [#12:0]
 */
arraySelector
    : LBRACKET (expression | rid | inputParameter) (COMMA (expression | rid | inputParameter))+ RBRACKET  # arrayMultiSelector
    | LBRACKET expression? RANGE expression? RBRACKET                         # arrayRangeSelector
    | LBRACKET expression? ELLIPSIS expression? RBRACKET                      # arrayEllipsisSelector
    | LBRACKET whereClause RBRACKET                                           # arrayConditionSelector
    | LBRACKET comparisonOperator expression RBRACKET                         # arrayFilterSelector
    | LBRACKET LIKE expression RBRACKET                                       # arrayLikeSelector
    | LBRACKET ILIKE expression RBRACKET                                      # arrayIlikeSelector
    | LBRACKET IN expression RBRACKET                                         # arrayInSelector
    | LBRACKET expression comparisonOperator expression RBRACKET              # arrayBinaryCondSelector
    | LBRACKET (expression | rid | inputParameter) RBRACKET                  # arraySingleSelector
    ;

/**
 * Expression modifier (e.g., .asString(), .size(), .keys())
 * Supports both property access (.identifier) and method calls (.identifier(args))
 */
modifier
    : DOT identifier (LPAREN (expression (COMMA expression)*)? RPAREN)?
    | arraySelector
    ;

/**
 * Input parameter (?, :name, $1)
 */
inputParameter
    : HOOK
    | COLON identifier
    | DOLLAR INTEGER_LITERAL
    ;

/**
 * Array literal [1, 2, 3]
 */
arrayLiteral
    : LBRACKET (expression (COMMA expression)*)? RBRACKET
    ;

/**
 * Map literal {key: value, ...}
 */
mapLiteral
    : LBRACE (mapEntry (COMMA mapEntry)*)? RBRACE
    ;

mapEntry
    : (identifier | STRING_LITERAL) COLON expression
    ;

/**
 * JSON literal (for CONTENT clauses, etc.)
 */
json
    : mapLiteral
    ;

// ============================================================================
// BASIC TYPES
// ============================================================================

/**
 * Record ID: #bucket:position or {rid: expression}
 * Supports negative cluster IDs: #-1:-1
 */
rid
    : LBRACE (RID_ATTR | RID_STRING) COLON expression RBRACE
    | integer COLON integer
    | HASH integer COLON integer
    ;

/**
 * Positive integer
 */
pInteger
    : INTEGER_LITERAL
    ;

/**
 * Integer (positive or negative)
 */
integer
    : MINUS? INTEGER_LITERAL
    ;

/**
 * URL - file://, http://, https://, or classpath:// URLs
 */
url
    : FILE_URL
    | HTTP_URL
    | HTTPS_URL
    | CLASSPATH_URL
    | STRING_LITERAL  // Also allow quoted strings as URLs for backward compatibility
    ;

/**
 * Setting list for WITH clause (e.g., WITH key1 = value1, key2 = value2)
 */
settingList
    : setting (COMMA setting)*
    ;

setting
    : identifier EQ expression
    ;

/**
 * Identifier - plain or quoted
 */
identifier
    : IDENTIFIER
    | QUOTED_IDENTIFIER
    | THIS
    | RID_ATTR
    | OUT_ATTR
    | IN_ATTR
    | TYPE_ATTR
    // Allow common keywords as identifiers
    | NAME
    | VALUE
    | VALUES
    | TYPE
    | TYPES
    | STATUS
    | COUNT
    | DATE
    | TIME
    | TIMESTAMP
    | DEFAULT
    | KEY
    | FORMAT
    | CUSTOM
    | SKIP_KW
    | START
    | CONTENT
    | RID
    | ADD
    | SET
    | IF
    | METADATA
    | VERTEX
    | EDGE
    | LIMIT
    | LINK
    | IN
    | ERROR_KW
    | PROFILE
    | HIDDEN_KW
    | AS
    | WHERE
    | WHILE
    | INDEX
    | LANGUAGE
    | DOCUMENT
    | VIEW
    | REFRESH
    | EVERY
    | SECOND
    | MINUTE
    | HOUR
    | MANUAL
    | INCREMENTAL
    | MATERIALIZED
    | CONTINUOUS
    | AGGREGATE
    | TIMESERIES
    | TAGS
    | FIELDS
    | RETENTION
    | COMPACTION_INTERVAL
    | SHARDS
    | DAYS
    | HOURS
    | MINUTES
    | DOWNSAMPLING
    | POLICY
    | GRANULARITY
    // Additional keywords allowed as identifiers (matching JavaCC parser)
    | PROPERTY
    | BUCKETS
    | BUCKETSELECTIONSTRATEGY
    | DATABASE
    | SUPERTYPE
    | EXTENDS
    | ENGINE
    | ALTER
    | DROP
    | TRUNCATE
    | REBUILD
    | EXPORT
    | IMPORT
    | CHECK
    | EXPLAIN
    | MOVE
    | BEGIN
    | COMMIT
    | ROLLBACK
    | LOCK
    | SLEEP
    | CONSOLE
    | PUT
    | REMOVE
    | MERGE
    | RECORD
    | TO
    | OF
    | OFFSET
    | WITH
    | WITHIN
    | NEAR
    | MINDEPTH
    | ON
    | OFF
    | OPTIONAL
    | EXISTS
    | ELSE
    | CONTINUE
    | FAIL
    | ITEM
    | ALIASES
    | ALIGN
    | EXCEPTION
    | FIND
    | ADDBUCKET
    | REMOVEBUCKET
    | FORCE
    | OPTIMIZE
    | INVERSE
    | GRANT
    | REVOKE
    | READ
    | EXECUTE
    | ALL
    | NONE
    | FUNCTION
    | PARAMETERS
    | ISOLATION
    | DEPTH_ALIAS
    | PATH_ALIAS
    | IDENTIFIED
    | SYSTEM
    | UNIDIRECTIONAL
    | CONTAINS
    ;
