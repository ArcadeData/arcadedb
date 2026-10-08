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
package com.arcadedb.server.ha.raft;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

/**
 * The calls test fakes received, in the order they arrived, and the answers they give (issue #9464). It replaces
 * Mockito's {@code verify(...)} / {@code when(...).thenThrow(...)} for the fakes that stand in for collaborators whose
 * effect is an outgoing call - a Raft entry submitted, a leadership transfer asked for - rather than a state a real
 * object would keep.
 * <p>
 * Several fakes can share one log, so a test can assert the ORDER of calls across them (what {@code InOrder} did).
 */
public final class CallLog {
  /** One call: the fake that received it, the method, and its arguments. */
  public record Call(Object target, String method, List<Object> args) {
  }

  private final List<Call> calls = new CopyOnWriteArrayList<>();

  void record(final Object target, final String method, final Object... args) {
    calls.add(new Call(target, method, Collections.unmodifiableList(Arrays.asList(args))));
  }

  /** Every call, in arrival order. */
  public List<Call> calls() {
    return List.copyOf(calls);
  }

  /** The argument lists of every call {@code target} received to {@code method}, in arrival order. */
  public List<List<Object>> argsOf(final Object target, final String method) {
    final List<List<Object>> result = new ArrayList<>();
    for (final Call call : calls)
      if (call.target() == target && call.method().equals(method))
        result.add(call.args());
    return result;
  }

  /** The methods called, across every fake on this log, in arrival order. */
  public List<String> methods() {
    final List<String> result = new ArrayList<>();
    for (final Call call : calls)
      result.add(call.method());
    return result;
  }

  /**
   * The answers one fake gives, by method name. A method with none set answers its own default. Names are checked
   * against the methods the fake overrides, so a typo fails where it is written instead of silently answering the
   * default.
   */
  static final class Answers {
    private final Set<String>                                  methods;
    private final Map<String, Function<Object[], Object>>      answers = new HashMap<>();

    Answers(final Set<String> methods) {
      this.methods = methods;
    }

    void set(final String method, final Function<Object[], Object> answer) {
      if (!methods.contains(method))
        throw new IllegalArgumentException("No recorded method '" + method + "', expected one of " + methods);
      synchronized (answers) {
        answers.put(method, answer);
      }
    }

    Function<Object[], Object> get(final String method) {
      synchronized (answers) {
        return answers.get(method);
      }
    }
  }
}
