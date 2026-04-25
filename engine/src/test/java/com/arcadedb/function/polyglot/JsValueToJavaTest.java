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
package com.arcadedb.function.polyglot;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression coverage for {@link JavascriptFunctionDefinition#jsValueToJava}.
 *
 * <p>The previous implementation fell through to {@code result.hasMembers()} for any
 * non-host-object {@link Value}. GraalVM's polyglot interop exposes Java {@link Map} interface
 * methods ({@code get}, {@code put}, {@code remove}, {@code clear}, {@code entrySet}, …) as
 * JS members when the wrapper is a non-hosted view of a host {@link Map} — which is what
 * happens after JS code mutates the map's entries. {@code getMemberKeys()} then enumerated
 * <em>both</em> the data keys and every Map method, which downstream serialization wrote as
 * garbage properties on the parent record (visible later as
 * {@code BinarySerializer "Skipping corrupted property 'remove'/'get'/…"} warnings).
 *
 * <p>This suite pins the post-fix behaviour: a host Map / List that's been mutated from JS
 * round-trips as a clean Map / List with only data keys.
 */
class JsValueToJavaTest {

  private Context context;

  @BeforeEach
  void setUp() {
    context = Context.newBuilder("js").allowAllAccess(true).build();
  }

  @AfterEach
  void tearDown() {
    if (context != null)
      context.close();
  }

  @Test
  void hostMapMutatedFromJsDoesNotLeakInterfaceMethodNames() {
    final LinkedHashMap<String, Object> seed = new LinkedHashMap<>();
    seed.put("status", "DRAFT");
    seed.put("notes", "secret");
    context.getBindings("js").putMember("order", seed);

    // Reproduces the production process script: mutate properties on a host Map, then return
    // it from JS. The Value the embedder receives back is the source of the corruption.
    final Value result = context.eval("js",
        "order.id = 'order-42'; order.lastStatusChange = 1700000000000; delete order.notes; order");

    final Object converted = JavascriptFunctionDefinition.jsValueToJava(result);
    assertThat(converted).isInstanceOf(Map.class);

    @SuppressWarnings("unchecked")
    final Map<String, Object> map = (Map<String, Object>) converted;
    assertThat(map).containsEntry("id", "order-42");
    assertThat(map).containsEntry("status", "DRAFT");
    assertThat(((Number) map.get("lastStatusChange")).longValue()).isEqualTo(1700000000000L);
    assertThat(map).doesNotContainKey("notes");

    // The corruption signature: NONE of the Java Map interface methods may surface as keys.
    assertThat(map).doesNotContainKeys(
        "get", "put", "remove", "clear", "values", "keySet", "entrySet",
        "putAll", "containsKey", "containsValue", "isEmpty", "size",
        "compute", "computeIfAbsent", "computeIfPresent", "merge", "replace",
        "replaceAll", "forEach", "getOrDefault", "hashCode", "toString", "getClass");
  }

  @Test
  void jsArrayOfHostMapsRoundTripsAsListOfPlainMaps() {
    // Production shape: each element is a Java LinkedHashMap that JS mutated. The fix has to
    // recurse into the array correctly so each element drops the method-name leakage.
    final LinkedHashMap<String, Object> a = new LinkedHashMap<>();
    a.put("id", "a-1");
    a.put("status", "PAYMENT_PENDING");
    final LinkedHashMap<String, Object> b = new LinkedHashMap<>();
    b.put("id", "b-1");
    b.put("status", "DRAFT");

    context.getBindings("js").putMember("a", a);
    context.getBindings("js").putMember("b", b);

    // Return as JS array; mutate one element to force the non-host-object code path.
    final Value arr = context.eval("js", "a.touched = true; [a, b]");

    final Object converted = JavascriptFunctionDefinition.jsValueToJava(arr);
    assertThat(converted).isInstanceOf(List.class);

    @SuppressWarnings("unchecked")
    final List<Object> list = (List<Object>) converted;
    assertThat(list).hasSize(2);

    @SuppressWarnings("unchecked")
    final Map<String, Object> first = (Map<String, Object>) list.get(0);
    @SuppressWarnings("unchecked")
    final Map<String, Object> second = (Map<String, Object>) list.get(1);

    assertThat(first).containsEntry("id", "a-1").containsEntry("touched", true);
    assertThat(second).containsEntry("id", "b-1");
    assertThat(first).doesNotContainKeys("get", "put", "remove", "values", "entrySet");
    assertThat(second).doesNotContainKeys("get", "put", "remove", "values", "entrySet");
  }

  @Test
  void hostListMutatedFromJsRoundTripsAsPlainList() {
    final List<String> seed = new ArrayList<>(List.of("alpha", "beta"));
    context.getBindings("js").putMember("items", seed);

    final Value result = context.eval("js", "items.push('gamma'); items");

    final Object converted = JavascriptFunctionDefinition.jsValueToJava(result);
    assertThat(converted).isInstanceOf(List.class);

    @SuppressWarnings("unchecked")
    final List<Object> list = (List<Object>) converted;
    assertThat(list).containsExactly("alpha", "beta", "gamma");
  }

  @Test
  void plainJsObjectStillUsesMemberKeys() {
    // Sanity: the existing behaviour for genuine JS objects must not regress. JS object literals
    // legitimately go through the hasMembers() / getMemberKeys() branch.
    final Value obj = context.eval("js", "({ name: 'alice', age: 30, active: true })");

    final Object converted = JavascriptFunctionDefinition.jsValueToJava(obj);
    assertThat(converted).isInstanceOf(Map.class);

    @SuppressWarnings("unchecked")
    final Map<String, Object> map = (Map<String, Object>) converted;
    assertThat(map).containsEntry("name", "alice").containsEntry("active", true);
    assertThat(((Number) map.get("age")).intValue()).isEqualTo(30);
    assertThat(map).hasSize(3);
  }

  @Test
  void primitivesPassThrough() {
    assertThat(((Number) JavascriptFunctionDefinition.jsValueToJava(context.eval("js", "42"))).intValue()).isEqualTo(42);
    assertThat(JavascriptFunctionDefinition.jsValueToJava(context.eval("js", "'hi'"))).isEqualTo("hi");
    assertThat(JavascriptFunctionDefinition.jsValueToJava(context.eval("js", "true"))).isEqualTo(true);
    assertThat(JavascriptFunctionDefinition.jsValueToJava(context.eval("js", "null"))).isNull();
  }
}
