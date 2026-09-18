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
package com.arcadedb.remote;

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #7654: every {@code UnsupportedOperationException} the remote client package throws must carry a message
 * that names the method and, where one exists, the working SQL alternative. Before this fix 98 of them were bare,
 * so a remote caller got a stack trace with a null message and no hint of what to do instead.
 * <p>
 * Driven by reflection rather than by a list of method names on purpose. A hand-written per-method test proves
 * only what somebody remembered to write down, and #7654's own predecessor (#7335) scoped itself to four methods
 * out of 102; this walks EVERY public method of the four classes, invokes it with neutral arguments, and holds
 * whatever {@code UnsupportedOperationException} comes out to the contract. A method added later that throws a bare
 * one fails here without anybody having to remember this file exists.
 * <p>
 * Methods that throw anything else, or that return, are ignored: this test is about the shape of the refusal, not
 * about which operations are refused. That is also why the count is asserted at the end - an accidental "nothing
 * was actually exercised" run (a constructor change, say, that makes every invocation fail early) would otherwise
 * pass silently.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7654RemoteUnsupportedMessageTest {

  /** The wording #7335 established and #7654 settled on for the whole package. */
  private static final String EXPECTED_WORDING = "is not supported in remote database";

  @Test
  void everyRemoteRefusalNamesItselfAndTheAlternative() {
    int checked = 0;
    checked += assertAllRefusalsCarryAMessage(RemoteSchema.class, () -> new RemoteSchema(mock(RemoteDatabase.class)));
    checked += assertAllRefusalsCarryAMessage(RemoteBucket.class, () -> new RemoteBucket("testBucket"));
    checked += assertAllRefusalsCarryAMessage(RemoteDocumentType.class, Issue7654RemoteUnsupportedMessageTest::newDocumentType);
    checked += assertAllRefusalsCarryAMessage(RemoteProperty.class, Issue7654RemoteUnsupportedMessageTest::newProperty);

    // The four classes carried 98 bare throws when #7654 was filed, plus the ones #7335 had already given a message
    // to. A floor rather than an equality: adding a remote capability legitimately removes refusals.
    assertThat(checked).isGreaterThanOrEqualTo(90);
  }

  /**
   * Invokes every public instance method of {@code type} on a fresh instance and, for each one that refuses with an
   * {@link UnsupportedOperationException}, asserts the message names the method and uses the package's single
   * wording. A fresh instance per method so a method that mutates state cannot change what the next one answers.
   *
   * @param type    the class whose public methods are exercised
   * @param factory supplies a fresh instance of it for each method
   *
   * @return how many refusals were actually observed
   */
  private <T> int assertAllRefusalsCarryAMessage(final Class<T> type, final Supplier<T> factory) {
    int refusals = 0;

    for (final Method method : type.getDeclaredMethods()) {
      if (!Modifier.isPublic(method.getModifiers()) || Modifier.isStatic(method.getModifiers()) || method.isSynthetic())
        continue;

      final Object[] args = new Object[method.getParameterCount()];
      for (int i = 0; i < args.length; i++)
        args[i] = neutralValue(method.getParameterTypes()[i]);

      final Throwable thrown;
      try {
        method.invoke(factory.get(), args);
        continue;
      } catch (final InvocationTargetException e) {
        thrown = e.getCause();
      } catch (final ReflectiveOperationException e) {
        continue;
      }

      if (!(thrown instanceof UnsupportedOperationException))
        // Some other failure - a mocked RemoteDatabase answering null to a method that IS implemented remotely, say.
        // Not this test's subject.
        continue;

      ++refusals;
      assertThat(thrown.getMessage())
          .as("%s.%s() threw UnsupportedOperationException with no message (issue #7654)", type.getSimpleName(),
              method.getName())
          .isNotNull();
      assertThat(thrown.getMessage())
          .as("%s.%s()'s refusal must name the method that refused", type.getSimpleName(), method.getName())
          .contains(method.getName() + "()");
      assertThat(thrown.getMessage())
          .as("%s.%s()'s refusal must use the package's single wording", type.getSimpleName(), method.getName())
          .contains(EXPECTED_WORDING);
      assertThat(thrown.getMessage())
          .as("%s.%s()'s refusal must say more than that it refused: name the SQL alternative, or why there is none",
              type.getSimpleName(), method.getName())
          .hasSizeGreaterThan((method.getName() + "() " + EXPECTED_WORDING + ".").length());
    }

    return refusals;
  }

  /**
   * A value of {@code parameterType} that is safe to pass anywhere: {@code null} for a reference, the zero/false
   * default for a primitive, and a zero-length array for an array (a {@code null} array would turn a legitimate
   * refusal into a {@link NullPointerException} raised before it).
   *
   * @param parameterType the declared type of the parameter to supply a value for
   *
   * @return a value assignable to it
   */
  private static Object neutralValue(final Class<?> parameterType) {
    if (parameterType.isArray())
      return Array.newInstance(parameterType.getComponentType(), 0);
    if (!parameterType.isPrimitive())
      return null;
    if (parameterType == boolean.class)
      return false;
    if (parameterType == char.class)
      return (char) 0;
    if (parameterType == long.class)
      return 0L;
    if (parameterType == float.class)
      return 0f;
    if (parameterType == double.class)
      return 0d;
    if (parameterType == byte.class)
      return (byte) 0;
    if (parameterType == short.class)
      return (short) 0;
    return 0;
  }

  private static RemoteDocumentType newDocumentType() {
    final RemoteDatabase database = mock(RemoteDatabase.class);
    when(database.getSchema()).thenReturn(mock(RemoteSchema.class));
    return new RemoteDocumentType(database, documentTypeRecord());
  }

  private static RemoteProperty newProperty() {
    final Map<String, Object> record = new HashMap<>();
    record.put("name", "testProperty");
    record.put("type", "STRING");
    record.put("id", 1);
    return new RemoteProperty(mock(DocumentType.class), record);
  }

  private static Result documentTypeRecord() {
    final Map<String, Object> values = new HashMap<>();
    values.put("name", "TestType");
    values.put("bucketSelectionStrategy", "round-robin");
    values.put("buckets", List.of("bucket1"));
    values.put("parentTypes", List.of());
    values.put("properties", new ArrayList<>());
    values.put("custom", Collections.emptyMap());

    final Result record = mock(Result.class);
    when(record.getPropertyNames()).thenReturn(values.keySet());
    for (final Map.Entry<String, Object> entry : values.entrySet())
      when(record.<Object>getProperty(entry.getKey())).thenReturn(entry.getValue());
    return record;
  }
}
