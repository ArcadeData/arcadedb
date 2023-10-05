package com.arcadedb.query.nativ;/*
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
 */

import com.arcadedb.database.Document;
import com.arcadedb.serializer.BinaryComparator;

import java.util.*;
import java.util.concurrent.*;

/**
 * Native condition with support for simple operators through inheritance.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public enum NativeOperator {
  or("or", true, 0) {
    @Override
    Object eval(final Document record, final Object left, final Object right) {
      final Boolean leftValue = (Boolean) NativeSelectExecutor.evaluateValue(record, left);
      if (leftValue)
        return true;

      return NativeSelectExecutor.evaluateValue(record, right);
    }
  },

  and("and", true, 2) {
    @Override
    Object eval(final Document record, final Object left, final Object right) {
      final Boolean leftValue = (Boolean) NativeSelectExecutor.evaluateValue(record, left);
      if (!leftValue)
        return false;

      return NativeSelectExecutor.evaluateValue(record, right);
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
      return BinaryComparator.equals(NativeSelectExecutor.evaluateValue(record, left),
          NativeSelectExecutor.evaluateValue(record, right));
    }
  },

  lt("<", false, 1) {
    @Override
    Object eval(final Document record, final Object left, final Object right) {
      return BinaryComparator.compareTo(NativeSelectExecutor.evaluateValue(record, left),
          NativeSelectExecutor.evaluateValue(record, right)) < 0;
    }
  },

  le("<=", false, 1) {
    @Override
    Object eval(final Document record, final Object left, final Object right) {
      return BinaryComparator.compareTo(NativeSelectExecutor.evaluateValue(record, left),
          NativeSelectExecutor.evaluateValue(record, right)) <= 0;
    }
  },

  gt(">", false, 1) {
    @Override
    Object eval(final Document record, final Object left, final Object right) {
      return BinaryComparator.compareTo(NativeSelectExecutor.evaluateValue(record, left),
          NativeSelectExecutor.evaluateValue(record, right)) > 0;
    }
  },

  ge(">=", false, 1) {
    @Override
    Object eval(final Document record, final Object left, final Object right) {
      return BinaryComparator.compareTo(NativeSelectExecutor.evaluateValue(record, left),
          NativeSelectExecutor.evaluateValue(record, right)) >= 0;
    }
  },

  run("!", true, -1) {
    @Override
    Object eval(final Document record, final Object left, final Object right) {
      return NativeSelectExecutor.evaluateValue(record, left);
    }
  };

  public final   String                      name;
  public final   boolean                     logicOperator;
  public final   int                         precedence;
  private static Map<String, NativeOperator> NAMES = new ConcurrentHashMap<>();

  NativeOperator(final String name, final boolean logicOperator, final int precedence) {
    this.name = name;
    this.logicOperator = logicOperator;
    this.precedence = precedence;
  }

  abstract Object eval(final Document record, Object left, Object right);

  public static NativeOperator byName(final String name) {
    if (NAMES.isEmpty()) {
      for (NativeOperator v : values())
        NAMES.put(v.name, v);
    }

    return NAMES.get(name);
  }

  @Override
  public String toString() {
    return name;
  }
}
