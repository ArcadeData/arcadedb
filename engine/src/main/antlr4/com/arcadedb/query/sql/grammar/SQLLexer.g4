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
 * ANTLR4 Lexer Grammar for ArcadeDB SQL
 *
 * Converted from JavaCC SQLGrammar.jjt to ANTLR4
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
lexer grammar SQLLexer;

// ============================================================================
// KEYWORDS (Case-Insensitive)
// ============================================================================

// Query Keywords
SELECT: S E L E C T;
TRAVERSE: T R A V E R S E;
MATCH: M A T C H;
INSERT: I N S E R T;
CREATE: C R E A T E;
CUSTOM: C U S T O M;
DELETE: D E L E T E;
UPDATE: U P D A T E;
UPSERT: U P S E R T;

// Type Keywords
DOCUMENT: D O C U M E N T;
VERTEX: V E R T E X;
EDGE: E D G E;
TYPE: T Y P E;
TYPES: T Y P E S;
SUPERTYPE: S U P E R T Y P E;

// Clause Keywords
FROM: F R O M;
TO: T O;
WHERE: W H E R E;
WHILE: W H I L E;
INTO: I N T O;
VALUE: V A L U E;
VALUES: V A L U E S;
SET: S E T;
ADD: A D D;
PUT: P U T;
MERGE: M E R G E;
CONTENT: C O N T E N T;
REMOVE: R E M O V E;
INCREMENT: I N C R E M E N T;

// Logical Keywords
AND: A N D;
OR: O R;
NOT: N O T;
IN: I N;
IS: I S;

// Null/Boolean Keywords
NULL: N U L L;
TRUE: T R U E;
FALSE: F A L S E;
DEFINED: D E F I N E D;
DEFINE: D E F I N E;

// Ordering Keywords
ORDER_BY: O R D E R WS+ B Y;
GROUP_BY: G R O U P WS+ B Y;
BY: B Y;
ASC: A S C;
DESC: D E S C;
LIMIT: L I M I T;
SKIP_KW: S K I P;
OFFSET: O F F S E T;
TIMEOUT: T I M E O U T;

// Control Flow Keywords
IF: I F;
ELSE: E L S E;
FOR: F O R;
FOREACH: F O R E A C H;
WHILE_KW: W H I L E;
BREAK: B R E A K;
CONTINUE: C O N T I N U E;
RETURN: R E T U R N;
CASE: C A S E;
WHEN: W H E N;
THEN: T H E N;
END: E N D;

// DDL Keywords
ALTER: A L T E R;
DROP: D R O P;
TRUNCATE: T R U N C A T E;
REBUILD: R E B U I L D;
PROPERTY: P R O P E R T Y;
INDEX: I N D E X;
BUCKET: B U C K E T;
BUCKETS: B U C K E T S;
PAGESIZE: P A G E S I Z E;
SCHEMA: S C H E M A;
DATABASE: D A T A B A S E;

// Constraint Keywords
EXTENDS: E X T E N D S;
POLYMORPHIC: P O L Y M O R P H I C;
UNIDIRECTIONAL: U N I D I R E C T I O N A L;
NULL_STRATEGY: N U L L UNDERSCORE S T R A T E G Y;
FORCE: F O R C E;
HIDDEN_KW: H I D D E N;

// Index Keywords
ENGINE: E N G I N E;
METADATA: M E T A D A T A;

// Transaction Keywords
BEGIN: B E G I N;
COMMIT: C O M M I T;
ROLLBACK: R O L L B A C K;
ISOLATION: I S O L A T I O N;

// Other Keywords
AS: A S;
LET: L E T;
UNWIND: U N W I N D;
WITH: W I T H;
DISTINCT: D I S T I N C T;
COUNT: C O U N T;
EXISTS: E X I S T S;
OPTIONAL: O P T I O N A L;
DEFAULT: D E F A U L T;
DEFAULTS: D E F A U L T S;
APPLY: A P P L Y;
UNIQUE: U N I Q U E;
NOTUNIQUE: N O T U N I Q U E;
FULL_TEXT: F U L L UNDERSCORE T E X T;

// Traversal Keywords
STRATEGY: S T R A T E G Y;
DEPTH_FIRST: D E P T H UNDERSCORE F I R S T;
BREADTH_FIRST: B R E A D T H UNDERSCORE F I R S T;
MAXDEPTH: M A X D E P T H;
MINDEPTH: M I N D E P T H;
DEPTH_ALIAS: D E P T H A L I A S;
PATH_ALIAS: P A T H A L I A S;

// Spatial Keywords
NEAR: N E A R;
WITHIN: W I T H I N;

// Import/Export Keywords
IMPORT: I M P O R T;
EXPORT: E X P O R T;
BACKUP: B A C K U P;
FORMAT: F O R M A T;
OVERWRITE: O V E R W R I T E;

// Misc Keywords
WAIT: W A I T;
RETRY: R E T R Y;
LOCK: L O C K;
RECORD: R E C O R D;
CHECK: C H E C K;
UNSAFE: U N S A F E;
BEFORE: B E F O R E;
AFTER: A F T E R;
OF: O F;
EXCEPTION: E X C E P T I O N;
PROFILE: P R O F I L E;
ON: O N;
OFF: O F F;
COMPRESS: C O M P R E S S;
FIND: F I N D;
BUCKETSELECTIONSTRATEGY: B U C K E T S E L E C T I O N S T R A T E G Y;
NAME: N A M E;
ADDBUCKET: A D D B U C K E T;
REMOVEBUCKET: R E M O V E B U C K E T;
LINK: L I N K;
INVERSE: I N V E R S E;
EXPLAIN: E X P L A I N;
GRANT: G R A N T;
REVOKE: R E V O K E;
READ: R E A D;
EXECUTE: E X E C U T E;
ALL: A L L;
NONE: N O N E;
FUNCTION: F U N C T I O N;
GLOBAL: G L O B A L;
PARAMETERS: P A R A M E T E R S;
LANGUAGE: L A N G U A G E;
TRIGGER: T R I G G E R;
FAIL: F A I L;
FIX: F I X;
SLEEP: S L E E P;
CONSOLE: C O N S O L E;
START: S T A R T;
MOVE: M O V E;
IDENTIFIED: I D E N T I F I E D;
RID: R I D;
SYSTEM: S Y S T E M;
OPTIMIZE: O P T I M I Z E;
ALIASES: A L I A S E S;
ALIGN: A L I G N;
BATCH: B A T C H;
ERROR_KW: E R R O R;
STATUS: S T A T U S;
DATE: D A T E;
TIME: T I M E;
TIMESTAMP: T I M E S T A M P;
MATERIALIZED: M A T E R I A L I Z E D;
VIEW: V I E W;
REFRESH: R E F R E S H;
EVERY: E V E R Y;
SECOND: S E C O N D;
MINUTE: M I N U T E;
HOUR: H O U R;
MANUAL: M A N U A L;
INCREMENTAL: I N C R E M E N T A L;
TIMESERIES: T I M E S E R I E S;
TAGS: T A G S;
FIELDS: F I E L D S;
RETENTION: R E T E N T I O N;
COMPACTION_INTERVAL: C O M P A C T I O N UNDERSCORE I N T E R V A L;
SHARDS: S H A R D S;
DAYS: D A Y S;
HOURS: H O U R S;
MINUTES: M I N U T E S;
DOWNSAMPLING: D O W N S A M P L I N G;
POLICY: P O L I C Y;
GRANULARITY: G R A N U L A R I T Y;
CONTINUOUS: C O N T I N U O U S;
AGGREGATE: A G G R E G A T E;

// ============================================================================
// COMPARISON OPERATORS
// ============================================================================

LIKE: L I K E;
ILIKE: I L I K E;
BETWEEN: B E T W E E N;
CONTAINS: C O N T A I N S;
CONTAINSALL: C O N T A I N S A L L;
CONTAINSANY: C O N T A I N S A N Y;
CONTAINSKEY: C O N T A I N S K E Y;
CONTAINSVALUE: C O N T A I N S V A L U E;
CONTAINSTEXT: C O N T A I N S T E X T;
MATCHES: M A T C H E S;
INSTANCEOF: I N S T A N C E O F;
KEY: K E Y;
ITEM: I T E M;

// ============================================================================
// SPECIAL ATTRIBUTE TOKENS
// ============================================================================

THIS: AT T H I S;
RID_ATTR: AT R I D;
RID_STRING: DQUOTE AT R I D DQUOTE;
OUT_ATTR: AT O U T;
IN_ATTR: AT I N;
TYPE_ATTR: AT T Y P E;
RID_ID_ATTR: AT R I D UNDERSCORE I D;
RID_POS_ATTR: AT R I D UNDERSCORE P O S;
PROPS_ATTR: AT P R O P S;

// ============================================================================
// OPERATORS
// ============================================================================

// Assignment Operators
EQ: '=';
EQEQ: '==';
NSEQ: '<=>';
PLUSASSIGN: '+=';
MINUSASSIGN: '-=';
STARASSIGN: '*=';
SLASHASSIGN: '/=';
ANDASSIGN: '&=';
ORASSIGN: '|=';
XORASSIGN: '^=';
REMASSIGN: '%=';
LSHIFTASSIGN: '<<=';
RSIGNEDSHIFTASSIGN: '>>=';
RUNSIGNEDSHIFTASSIGN: '>>>=';

// Comparison Operators
LT: '<';
GT: '>';
LE: '<=';
GE: '>=';
NE: '!=';
NEQ: '<>';

// Logical Operators
SC_OR: '||';
SC_AND: '&&';
NULL_COALESCING: '??';

// Arithmetic Operators
PLUS: '+';
MINUS: '-';
STAR: '*';
SLASH: '/';
REM: '%';
INCR: '++';
DECR: '--';

// Bitwise Operators
BIT_AND: '&';
BIT_OR: '|';
XOR: '^';
LSHIFT: '<<';
RSHIFT: '>>';
RUNSIGNEDSHIFT: '>>>';

// Other Operators
BANG: '!';
TILDE: '~';
HOOK: '?';
RANGE: '..';
ELLIPSIS: '...';

// Graph/MATCH Arrow Operators (order matters - longer tokens first)
ARROW_RIGHT: '->';
ARROW_LEFT: '<-';

// ============================================================================
// SEPARATORS
// ============================================================================

LPAREN: '(';
RPAREN: ')';
LBRACE: '{';
RBRACE: '}';
LBRACKET: '[';
RBRACKET: ']';
SEMICOLON: ';';
COMMA: ',';
DOT: '.';
COLON: ':';
AT: '@';
DOLLAR: '$';
BACKTICK: '`';
HASH: '#';

// ============================================================================
// SPECIAL IDENTIFIERS
// ============================================================================

INDEX_COLON: I N D E X COLON;

INDEXVALUES_IDENTIFIER
    : I N D E X V A L U E S COLON ('__@recordmap@___')? IDENTIFIER (( DOT | MINUS ) IDENTIFIER)*
    ;

INDEXVALUESASC_IDENTIFIER
    : I N D E X V A L U E S A S C COLON ('__@recordmap@___')? IDENTIFIER (( DOT | MINUS ) IDENTIFIER)*
    ;

INDEXVALUESDESC_IDENTIFIER
    : I N D E X V A L U E S D E S C COLON ('__@recordmap@___')? IDENTIFIER (( DOT | MINUS ) IDENTIFIER)*
    ;

// Parameterized bucket identifiers (must come before BUCKET_IDENTIFIER to take precedence)
BUCKET_NAMED_PARAM: B U C K E T COLON COLON IDENTIFIER;          // bucket::paramName
BUCKET_POSITIONAL_PARAM: B U C K E T COLON HOOK;                 // bucket:?

BUCKET_IDENTIFIER: B U C K E T COLON IDENTIFIER;
BUCKET_NUMBER_IDENTIFIER: B U C K E T COLON INTEGER_LITERAL;
SCHEMA_IDENTIFIER: S C H E M A COLON IDENTIFIER (COLON SCHEMA_NAME_PART)?;
fragment SCHEMA_NAME_PART: (LETTER | [0-9] | '_' | '[' | ']' | '.' | '-')+;

// URL Identifiers
HTTP_URL: 'http://' URL_CHAR+;
HTTPS_URL: 'https://' URL_CHAR+;
FILE_URL: 'file://' URL_CHAR+;
CLASSPATH_URL: 'classpath://' URL_CHAR+;

fragment URL_CHAR
    : ~[" \t\r\n;]  // Exclude spaces in addition to quotes, tabs, newlines, semicolons
    | ESCAPE_SEQUENCE
    ;

// ============================================================================
// LITERALS
// ============================================================================

// Integer Literals
INTEGER_LITERAL
    : DECIMAL_LITERAL [lL]?
    | HEX_LITERAL [lL]?
    | OCTAL_LITERAL [lL]?
    ;

fragment DECIMAL_LITERAL: [1-9] [0-9]*;
fragment HEX_LITERAL: '0' [xX] [0-9a-fA-F]+;
fragment OCTAL_LITERAL: '0' [0-7]*;

// Floating Point Literals
FLOATING_POINT_LITERAL
    : DECIMAL_FLOATING_POINT_LITERAL
    | HEXADECIMAL_FLOATING_POINT_LITERAL
    ;

fragment DECIMAL_FLOATING_POINT_LITERAL
    : [0-9]+ '.' [0-9]* DECIMAL_EXPONENT? [fFdD]?
    | '.' [0-9]+ DECIMAL_EXPONENT? [fFdD]?
    | [0-9]+ DECIMAL_EXPONENT [fFdD]?
    | [0-9]+ DECIMAL_EXPONENT? [fFdD]
    ;

fragment DECIMAL_EXPONENT: [eE] [+\-]? [0-9]+;

fragment HEXADECIMAL_FLOATING_POINT_LITERAL
    : '0' [xX] [0-9a-fA-F]+ ('.')? HEXADECIMAL_EXPONENT [fFdD]?
    | '0' [xX] [0-9a-fA-F]* '.' [0-9a-fA-F]+ HEXADECIMAL_EXPONENT [fFdD]?
    ;

fragment HEXADECIMAL_EXPONENT: [pP] [+\-]? [0-9]+;

// String Literals
STRING_LITERAL
    : DQUOTE ( STRING_ESCAPE_SEQ | ~[\r\n"\\] )* DQUOTE
    | SQUOTE ( STRING_ESCAPE_SEQ | ~[\r\n'\\] )* SQUOTE
    ;

fragment STRING_ESCAPE_SEQ
    : '\\' [ntbrf\\'"/ %?]
    | '\\u' [0-9a-fA-F] [0-9a-fA-F] [0-9a-fA-F] [0-9a-fA-F]
    ;

// Character Literals
CHARACTER_LITERAL
    : SQUOTE ( CHAR_ESCAPE_SEQ | ~['\\\r\n] ) SQUOTE
    ;

fragment CHAR_ESCAPE_SEQ
    : '\\' [ntbrf\\'/"]
    | '\\' [0-7] [0-7]?
    | '\\' [0-3] [0-7] [0-7]
    ;

// Range Literals
INTEGER_RANGE
    : MINUS? INTEGER_LITERAL RANGE MINUS? INTEGER_LITERAL
    ;

ELLIPSIS_INTEGER_RANGE
    : MINUS? INTEGER_LITERAL ELLIPSIS MINUS? INTEGER_LITERAL
    ;

// ============================================================================
// IDENTIFIERS
// ============================================================================

IDENTIFIER
    : (DOLLAR | LETTER) PART_LETTER*
    ;

QUOTED_IDENTIFIER
    : BACKTICK ( ~[`] | '\\`' )+ BACKTICK
    ;

fragment LETTER: [A-Z_a-z];
fragment PART_LETTER: [0-9A-Z_a-z];

// ============================================================================
// COMMENTS
// ============================================================================

LINE_COMMENT
    : '--' ' ' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;

BLOCK_COMMENT
    : '/*' .*? '*/' -> channel(HIDDEN)
    ;

// ============================================================================
// WHITESPACE
// ============================================================================

WS
    : [ \t\r\n\f]+ -> channel(HIDDEN)
    ;

// ============================================================================
// CASE-INSENSITIVE FRAGMENTS
// ============================================================================

fragment A: [aA];
fragment B: [bB];
fragment C: [cC];
fragment D: [dD];
fragment E: [eE];
fragment F: [fF];
fragment G: [gG];
fragment H: [hH];
fragment I: [iI];
fragment J: [jJ];
fragment K: [kK];
fragment L: [lL];
fragment M: [mM];
fragment N: [nN];
fragment O: [oO];
fragment P: [pP];
fragment Q: [qQ];
fragment R: [rR];
fragment S: [sS];
fragment T: [tT];
fragment U: [uU];
fragment V: [vV];
fragment W: [wW];
fragment X: [xX];
fragment Y: [yY];
fragment Z: [zZ];
fragment UNDERSCORE: '_';
fragment DQUOTE: '"';
fragment SQUOTE: '\'';

// ============================================================================
// ESCAPE SEQUENCE FOR URLs
// ============================================================================

fragment ESCAPE_SEQUENCE
    : '\\' [ntbrf\\'/"]
    | '\\' [0-7] [0-7]?
    | '\\' [0-3] [0-7] [0-7]
    ;
