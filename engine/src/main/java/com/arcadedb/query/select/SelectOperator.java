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

import com.arcadedb.database.Document;
import com.arcadedb.query.sql.executor.QueryHelper;
import com.arcadedb.serializer.BinaryComparator;

import java.util.*;
import java.util.concurrent.*;

/**
 * Native condition with support for simple operators through inheritance.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public enum SelectOperator {
  or("or", true, 0) {
    @Override
    Object eval(final Document record, final Object left, final Object right) {
      final Boolean leftValue = (Boolean) SelectExecutor.evaluateValue(record, left);
      if (leftValue)
        return true;

      return SelectExecutor.evaluateValue(record, right);
    }
  },

  and("and", true, 2) {
    @Override
    Object eval(final Document record, final Object left, final Object right) {
      final Boolean leftValue = (Boolean) SelectExecutor.evaluateValue(record, left);
      if (!leftValue)
        return false;

      return SelectExecutor.evaluateValue(record, right);
    }
  },

  not("not", true, 2) {
    @Override
    Object eval(final Document record, final Object left, final Object right) {
      return left == Boolean.FALSE;
    }
  },

  eq("=", false, 1) {
    @Override
    Object eval(final Document record, final Object left, final Object right) {
      return BinaryComparator.equals(SelectExecutor.evaluateValue(record, left), SelectExecutor.evaluateValue(record, right));
    }
  },

  neq("<>", false, 1) {
    @Override
    Object eval(final Document record, final Object left, final Object right) {
      return !BinaryComparator.equals(SelectExecutor.evaluateValue(record, left), SelectExecutor.evaluateValue(record, right));
    }
  },

  lt("<", false, 1) {
    @Override
    Object eval(final Document record, final Object left, final Object right) {
      return BinaryComparator.compareTo(SelectExecutor.evaluateValue(record, left), SelectExecutor.evaluateValue(record, right))
          < 0;
    }
  },

  le("<=", false, 1) {
    @Override
    Object eval(final Document record, final Object left, final Object right) {
      return BinaryComparator.compareTo(SelectExecutor.evaluateValue(record, left), SelectExecutor.evaluateValue(record, right))
          <= 0;
    }
  },

  gt(">", false, 1) {
    @Override
    Object eval(final Document record, final Object left, final Object right) {
      return BinaryComparator.compareTo(SelectExecutor.evaluateValue(record, left), SelectExecutor.evaluateValue(record, right))
          > 0;
    }
  },

  ge(">=", false, 1) {
    @Override
    Object eval(final Document record, final Object left, final Object right) {
      return BinaryComparator.compareTo(SelectExecutor.evaluateValue(record, left), SelectExecutor.evaluateValue(record, right))
          >= 0;
    }
  },

  ilike("ilike", false, 1) {
    @Override
    Object eval(final Document record, final Object left, final Object right) {
      return QueryHelper.like(((String) SelectExecutor.evaluateValue(record, left)).toLowerCase(Locale.ENGLISH),
          ((String) SelectExecutor.evaluateValue(record, right)).toLowerCase());
    }
  },

  like("like", false, 1) {
    @Override
    Object eval(final Document record, final Object left, final Object right) {
      return QueryHelper.like((String) SelectExecutor.evaluateValue(record, left),
          (String) SelectExecutor.evaluateValue(record, right));
    }
  },

  in_op("in", false, 1) {
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

  between("between", false, 1) {
    @Override
    Object eval(final Document record, final Object left, final Object right) {
      final Object leftValue = SelectExecutor.evaluateValue(record, left);
      final Object rightValue = SelectExecutor.evaluateValue(record, right);
      if (rightValue instanceof Object[] range && range.length == 2)
        return BinaryComparator.compareTo(leftValue, range[0]) >= 0 && BinaryComparator.compareTo(leftValue, range[1]) <= 0;
      throw new IllegalArgumentException("BETWEEN requires a range of two values");
    }
  },

  is_null("is null", false, 1) {
    @Override
    Object eval(final Document record, final Object left, final Object right) {
      return SelectExecutor.evaluateValue(record, left) == null;
    }
  },

  is_not_null("is not null", false, 1) {
    @Override
    Object eval(final Document record, final Object left, final Object right) {
      return SelectExecutor.evaluateValue(record, left) != null;
    }
  },

  run("!", true, -1) {
    @Override
    Object eval(final Document record, final Object left, final Object right) {
      return SelectExecutor.evaluateValue(record, left);
    }
  };

  public final   String                      name;
  public final   boolean                     logicOperator;
  public final   int                         precedence;
  private static Map<String, SelectOperator> NAMES = new ConcurrentHashMap<>();

  SelectOperator(final String name, final boolean logicOperator, final int precedence) {
    this.name = name;
    this.logicOperator = logicOperator;
    this.precedence = precedence;
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
