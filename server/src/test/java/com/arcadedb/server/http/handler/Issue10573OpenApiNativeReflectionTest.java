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
package com.arcadedb.server.http.handler;

import io.swagger.v3.core.util.Json;
import io.swagger.v3.oas.models.OpenAPI;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The native image served {@code GET /api/v1/openapi.json} as {@code 200 {}} (#10573): Jackson finds the properties of the
 * swagger models by reflection, which the image answers only for registered classes, so every model serialized as an
 * empty bean. {@code OpenApiModelReflectionFeature} (native module) registers every reachable class under the swagger model
 * and mix-in packages. The native image itself is not built in the unit lanes ({@code native/src/test/scripts/exercise.sh}
 * fetches the document from it), so these tests guard, on the JVM, the assumptions the fix rests on and the handler's answer
 * when they do not hold:
 * <ul>
 *   <li>every bean in the served document is a swagger model, so the feature's package scope covers all of them: a
 *   contributor that puts its own POJO in the document (as an example value, say) would serialize as {@code {}} again
 *   on native only, and fails here instead;</li>
 *   <li>the native build actually loads the feature;</li>
 *   <li>a document that still serializes empty is answered as a server fault, not as {@code 200 {}}.</li>
 * </ul>
 */
class Issue10573OpenApiNativeReflectionTest {
  private static final String MODEL_PACKAGE = "io.swagger.v3.oas.models.";
  private static final String FEATURE       = "com.arcadedb.nativeimage.OpenApiModelReflectionFeature";
  private static final Path   NATIVE_POM    = Path.of("..", "native", "pom.xml");
  private static final Path   FEATURE_SOURCE = Path.of("..", "native", "src", "main", "java", "com", "arcadedb", "nativeimage",
      "OpenApiModelReflectionFeature.java");

  @Test
  void everyBeanInTheServedDocumentIsASwaggerModel() throws Exception {
    final OpenAPI openAPI = new OpenApiSpecGenerator(null).generateSpec();

    final Set<String> models = new TreeSet<>();
    final Set<String> foreign = new TreeSet<>();
    walk(openAPI, models, foreign);

    // a walk that reached nothing would pass for the wrong reason
    assertThat(models).contains(MODEL_PACKAGE + "OpenAPI", MODEL_PACKAGE + "Paths", MODEL_PACKAGE + "PathItem",
        MODEL_PACKAGE + "Operation", MODEL_PACKAGE + "media.Schema", MODEL_PACKAGE + "responses.ApiResponse");
    assertThat(foreign).as("beans in the OpenAPI document outside " + MODEL_PACKAGE
        + ": OpenApiModelReflectionFeature does not register them, so the native image serializes them as {}").isEmpty();

    // and the document Jackson writes from it on the JVM is the real one, the baseline the native image must match
    final String json = Json.mapper().writeValueAsString(openAPI);
    assertThat(json).contains("\"openapi\":\"3.0.3\"", "\"/api/v1/query/{database}\"");
  }

  @Test
  void theNativeBuildLoadsTheFeatureAndTheFeatureCoversTheModelPackage() throws IOException {
    assertThat(Files.isRegularFile(NATIVE_POM)).as(NATIVE_POM.toAbsolutePath()
        + " not found: run this test with the server module as working directory, as Maven Surefire does").isTrue();
    assertThat(Files.readString(NATIVE_POM, StandardCharsets.UTF_8)).contains("<buildArg>--features=" + FEATURE + "</buildArg>");
    assertThat(Files.readString(FEATURE_SOURCE, StandardCharsets.UTF_8)).contains("\"" + MODEL_PACKAGE + "\"");
  }

  /**
   * The native image answered the empty serialization as {@code 200 {}} and cached it for the life of the process. A
   * document without its properties is a server fault: the handler now throws, which the error classification answers 500,
   * and nothing is cached.
   */
  @Test
  void anEmptySerializedDocumentIsAFaultNotADocument() throws Exception {
    assertThatThrownBy(() -> GetOpenApiHandler.requireSerializedDocument("{}"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("not visible to Jackson by reflection");

    final String json = Json.mapper().writeValueAsString(new OpenApiSpecGenerator(null).generateSpec());
    assertThat(GetOpenApiHandler.requireSerializedDocument(json)).isSameAs(json);
  }

  /** Collects the class of every bean reachable from {@code root} the way Jackson reaches it: fields, maps, collections. */
  private static void walk(final Object root, final Set<String> models, final Set<String> foreign) throws IllegalAccessException {
    final Set<Object> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    final Deque<Object> pending = new ArrayDeque<>();
    pending.push(root);
    while (!pending.isEmpty()) {
      final Object value = pending.pop();
      if (!seen.add(value))
        continue;

      final Class<?> type = value.getClass();
      if (value instanceof Map<?, ?> map)
        for (final Map.Entry<?, ?> entry : map.entrySet()) {
          push(pending, entry.getKey());
          push(pending, entry.getValue());
        }
      else if (value instanceof Collection<?> collection)
        for (final Object item : collection)
          push(pending, item);
      else if (type.isArray())
        for (int i = 0; i < Array.getLength(value); i++)
          push(pending, Array.get(value, i));

      final String name = type.getName();
      if (name.startsWith(MODEL_PACKAGE)) {
        models.add(name);
        for (Class<?> c = type; c != null && c.getName().startsWith(MODEL_PACKAGE); c = c.getSuperclass())
          for (final Field field : c.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers()))
              continue;
            field.setAccessible(true);
            push(pending, field.get(value));
          }
      } else if (!isJdkType(type))
        foreign.add(name);
    }
  }

  private static void push(final Deque<Object> pending, final Object value) {
    if (value != null)
      pending.push(value);
  }

  private static boolean isJdkType(final Class<?> type) {
    final Class<?> component = type.isArray() ? type.getComponentType() : type;
    return component.isPrimitive() || component.getName().startsWith("java.");
  }
}
