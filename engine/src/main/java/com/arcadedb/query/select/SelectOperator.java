package com.arcadedb.query.select;/*
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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.query.sql.parser.ILikeOperator;
import com.arcadedb.query.sql.parser.LikeOperator;
import com.arcadedb.serializer.BinaryComparator;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Native condition with support for simple operators through inheritance.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public enum SelectOperator {
  or("or", true, 0, false) {
    @Override
    Object eval(final Document record, final Object left, final Object right) {
      final Boolean leftValue = (Boolean) SelectExecutor.evaluateValue(record, left);
      if (leftValue)
        return true;

      return SelectExecutor.evaluateValue(record, right);
    }
  },

  and("and", true, 2, false) {
    @Override
    Object eval(final Document record, final Object left, final Object right) {
      final Boolean leftValue = (Boolean) SelectExecutor.evaluateValue(record, left);
      if (!leftValue)
        return false;

      return SelectExecutor.evaluateValue(record, right);
    }
  },

  /**
   * UNARY: NEGATES ITS LEFT OPERAND, {@code right} IS ALWAYS {@code null} (SEE
   * {@code new SelectTreeNode(operand, SelectOperator.not, null)}, THE SHAPE EVERY IN-TREE USE BUILDS).
   * <p>
   * #8059: THE BODY USED TO BE {@code left == Boolean.FALSE} - A REFERENCE COMPARISON AGAINST THE *UNEVALUATED*
   * OPERAND, WHICH IN A REAL TREE IS A SelectTreeNode AND NEVER Boolean.FALSE, SO THE WHOLE EXPRESSION ANSWERED
   * false FOR EVERY RECORD AND THE QUERY RETURNED NOTHING. EVERY OTHER OPERATOR ROUTES ITS OPERANDS THROUGH
   * SelectExecutor.evaluateValue(); THIS ONE DID NOT.
   */
  not("not", true, 2, true) {
    @Override
    Object eval(final Document record, final Object left, final Object right) {
      final Object leftValue = SelectExecutor.evaluateValue(record, left);
      if (leftValue instanceof Boolean booleanValue)
        return !booleanValue;
      throw new IllegalArgumentException("A boolean operand was expected by 'not' but '" + leftValue + "' was returned");
    }
  },

  eq("=", false, 1, false) {
    @Override
    Object eval(final Document record, final Object left, final Object right) {
      return BinaryComparator.equals(SelectExecutor.evaluateValue(record, left), SelectExecutor.evaluateValue(record, right));
    }
  },

  neq("<>", false, 1, false) {
    @Override
    Object eval(final Document record, final Object left, final Object right) {
      return !BinaryComparator.equals(SelectExecutor.evaluateValue(record, left), SelectExecutor.evaluateValue(record, right));
    }
  },

  /**
   * #8049: AN ORDERING COMPARISON AGAINST A MISSING OR NULL OPERAND IS false, NEVER true.
   * <p>
   * BinaryComparator.compareTo() IS A *SORT* ORDER AND SORTS null FIRST: compareTo(null, x) IS -1 AND
   * compareTo(x, null) IS 1. READ AS A PREDICATE THAT MADE A RECORD WITHOUT THE PROPERTY SATISFY EVERY
   * {@code < } AND {@code <=} ON IT, SO {@code a < 5} MATCHED RECORDS THAT CARRY NO {@code a} AT ALL - AND ONLY
   * WHILE NO INDEX EXISTED ON {@code a}, BECAUSE AN INDEX HOLDS NO ENTRY FOR A RECORD THAT LACKS THE PROPERTY AND
   * SelectExecutor.filterWithIndexesFinalNode() ANSWERS THE LEAF FROM A RANGE SCAN. THE SAME SELECT OVER THE SAME
   * DATA THEREFORE RETURNED A DIFFERENT NUMBER OF ROWS DEPENDING ON THE PHYSICAL SCHEMA, WHICH IS THE ONE THING
   * ADDING AN INDEX MUST NEVER DO.
   * <p>
   * REJECTING null ON *EITHER* SIDE IS SQL'S OWN RULE (A COMPARISON WITH NULL IS UNKNOWN, AND UNKNOWN DOES NOT
   * MATCH), IT IS WHAT {@code between} ALREADY DID ON ITS LEFT OPERAND, AND IT IS WHAT THE INDEX PATH DOES BY
   * CONSTRUCTION - SO ALL FOUR DIRECTIONS, THE INDEX PLAN AND THE FULL SCAN NOW AGREE. USE {@code is null} /
   * {@code is not null} TO ASK ABOUT ABSENCE.
   */
  lt("<", false, 1, false) {
    @Override
    Object eval(final Document record, final Object left, final Object right) {
      final Object leftValue = SelectExecutor.evaluateValue(record, left);
      final Object rightValue = SelectExecutor.evaluateValue(record, right);
      if (leftValue == null || rightValue == null)
        return false;
      return BinaryComparator.compareTo(leftValue, rightValue) < 0;
    }
  },

  /** #8049: see {@link #lt}. */
  le("<=", false, 1, false) {
    @Override
    Object eval(final Document record, final Object left, final Object right) {
      final Object leftValue = SelectExecutor.evaluateValue(record, left);
      final Object rightValue = SelectExecutor.evaluateValue(record, right);
      if (leftValue == null || rightValue == null)
        return false;
      return BinaryComparator.compareTo(leftValue, rightValue) <= 0;
    }
  },

  /** #8049: see {@link #lt}. A null LEFT was already rejected by the sort order; a null RIGHT was not. */
  gt(">", false, 1, false) {
    @Override
    Object eval(final Document record, final Object left, final Object right) {
      final Object leftValue = SelectExecutor.evaluateValue(record, left);
      final Object rightValue = SelectExecutor.evaluateValue(record, right);
      if (leftValue == null || rightValue == null)
        return false;
      return BinaryComparator.compareTo(leftValue, rightValue) > 0;
    }
  },

  /** #8049: see {@link #lt} and {@link #gt}. */
  ge(">=", false, 1, false) {
    @Override
    Object eval(final Document record, final Object left, final Object right) {
      final Object leftValue = SelectExecutor.evaluateValue(record, left);
      final Object rightValue = SelectExecutor.evaluateValue(record, right);
      if (leftValue == null || rightValue == null)
        return false;
      return BinaryComparator.compareTo(leftValue, rightValue) >= 0;
    }
  },

  /**
   * #8170: {@code ilike} and {@code like} evaluate through the SQL operators, so the native API cannot drift from SQL on
   * the operands SQL already handles: a record without the property (or a null pattern) does not match - the native
   * {@code ilike} used to fold the case of a null and throw a {@link NullPointerException} - a non-string value is
   * matched by its string form rather than failing a cast, and a multi-value property matches when any item does. Both
   * sides fold with the ENGLISH locale (issue #7900).
   */
  ilike("ilike", false, 1, false) {
    @Override
    Object eval(final Document record, final Object left, final Object right) {
      return SQL_ILIKE.execute((DatabaseInternal) record.getDatabase(), SelectExecutor.evaluateValue(record, left),
          SelectExecutor.evaluateValue(record, right));
    }
  },

  /** See {@link #ilike}. */
  like("like", false, 1, false) {
    @Override
    Object eval(final Document record, final Object left, final Object right) {
      return SQL_LIKE.execute((DatabaseInternal) record.getDatabase(), SelectExecutor.evaluateValue(record, left),
          SelectExecutor.evaluateValue(record, right));
    }
  },

  in_op("in", false, 1, false) {
    @Override
    Object eval(final Document record, final Object left, final Object right) {
      final Object leftValue = SelectExecutor.evaluateValue(record, left);
      final Object rightValue = SelectExecutor.evaluateValue(record, right);
      if (rightValue instanceof Collection<?> collection) {
        for (final Object item : collection)
          if (BinaryComparator.equals(leftValue, item))
            return true;
        return false;
      }
      return BinaryComparator.equals(leftValue, rightValue);
    }
  },

  between("between", false, 1, false) {
    @Override
    Object eval(final Document record, final Object left, final Object right) {
      final Object leftValue = SelectExecutor.evaluateValue(record, left);
      final Object rightValue = SelectExecutor.evaluateValue(record, right);
      if (rightValue instanceof Object[] range && range.length == 2) {
        // #8049: SAME RULE AS lt/le/gt/ge. THE LOWER-BOUND COMPARE ALREADY REJECTED A null LEFT FOR EVERY NON-null
        // BOUND, SO THIS ONLY CLOSES THE "BOTH THE VALUE AND THE BOUND ARE null" CORNER, BUT IT MAKES THE RULE ONE
        // RULE RATHER THAN AN ACCIDENT OF THE SORT ORDER
        if (leftValue == null || range[0] == null || range[1] == null)
          return false;
        return BinaryComparator.compareTo(leftValue, range[0]) >= 0 && BinaryComparator.compareTo(leftValue, range[1]) <= 0;
      }
      throw new IllegalArgumentException("BETWEEN requires a range of two values");
    }
  },

  is_null("is null", false, 1, true) {
    @Override
    Object eval(final Document record, final Object left, final Object right) {
      return SelectExecutor.evaluateValue(record, left) == null;
    }
  },

  is_not_null("is not null", false, 1, true) {
    @Override
    Object eval(final Document record, final Object left, final Object right) {
      return SelectExecutor.evaluateValue(record, left) != null;
    }
  },

  run("!", true, -1, true) {
    @Override
    Object eval(final Document record, final Object left, final Object right) {
      return SelectExecutor.evaluateValue(record, left);
    }
  };

  public final   String                      name;
  public final   boolean                     logicOperator;
  public final   int                         precedence;
  // #8256: WHETHER THIS OPERATOR EVALUATES ONLY ITS left OPERAND - DECLARED HERE, NEXT TO EACH OPERATOR, SO THE
  // JSON CONDITION PARSER'S ARITY GATE (Select.parseJsonCondition) CAN DERIVE IT FROM THE OPERATOR INSTEAD OF
  // NAMING A HAND-PICKED SUBSET, WHICH IS WHAT LEFT is_null AND is_not_null EXEMPTED ALONGSIDE not
  public final   boolean                     unary;
  private static Map<String, SelectOperator> NAMES = new ConcurrentHashMap<>();
  // STATELESS: execute() READS NOTHING BUT ITS ARGUMENTS, SO ONE INSTANCE IS SHARED BY EVERY QUERY AND THREAD
  private static final LikeOperator          SQL_LIKE  = new LikeOperator();
  private static final ILikeOperator         SQL_ILIKE = new ILikeOperator();

  SelectOperator(final String name, final boolean logicOperator, final int precedence, final boolean unary) {
    this.name = name;
    this.logicOperator = logicOperator;
    this.precedence = precedence;
    this.unary = unary;
  }

  abstract Object eval(final Document record, Object left, Object right);

  public static SelectOperator byName(final String name) {
    if (NAMES.isEmpty()) {
      for (SelectOperator v : values())
        NAMES.put(v.name, v);
    }

    return NAMES.get(name);
  }

  @Override
  public String toString() {
    return name;
  }
}
