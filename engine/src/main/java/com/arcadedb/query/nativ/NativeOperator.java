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

import com.arcadedb.serializer.BinaryComparator;

import java.util.*;

/**
 * Native condition with support for simple operators through inheritance.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public enum NativeOperator {
  or("or", 0) {
    @Override
    Boolean eval(final Object left, final Object right) {
      return left == Boolean.TRUE || right == Boolean.TRUE;
    }
  },

  and("and", 2) {
    @Override
    Boolean eval(final Object left, final Object right) {
      return left == Boolean.TRUE && right == Boolean.TRUE;
    }
  },

  not("not", 2) {
    @Override
    Boolean eval(final Object left, final Object right) {
      return left == Boolean.FALSE;
    }
  },

  eq("==", 1) {
    @Override
    Object eval(final Object left, final Object right) {
      return BinaryComparator.equals(left, right);
    }
  },

  lt("<", 1) {
    @Override
    Object eval(final Object left, final Object right) {
      return BinaryComparator.compareTo(left, right) < 0;
    }
  },

  le("<=", 1) {
    @Override
    Object eval(final Object left, final Object right) {
      return BinaryComparator.compareTo(left, right) <= 0;
    }
  },

  gt(">", 1) {
    @Override
    Object eval(final Object left, final Object right) {
      return BinaryComparator.compareTo(left, right) > 0;
    }
  },

  ge(">=", 1) {
    @Override
    Object eval(final Object left, final Object right) {
      return BinaryComparator.compareTo(left, right) >= 0;
    }
  },

  run("!", -1) {
    @Override
    Object eval(final Object left, final Object right) {
      return left;
    }
  };

  NativeOperator(final String name, final int precedence) {
    this.name = name;
    this.precedence = precedence;
  }

  public final String name;
  public final int    precedence;

  abstract Object eval(Object left, Object right);

  @Override
  public String toString() {
    return name;
  }
}
