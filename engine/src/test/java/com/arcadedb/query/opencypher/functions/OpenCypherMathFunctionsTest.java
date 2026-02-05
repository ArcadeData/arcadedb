/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.query.opencypher.functions;

import com.arcadedb.query.opencypher.functions.math.*;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Unit tests for OpenCypher math functions.
 */
class OpenCypherMathFunctionsTest {

  // ============ MathSigmoid tests ============

  @Test
  void mathSigmoidBasic() {
    final MathSigmoid fn = new MathSigmoid();
    assertThat(fn.getName()).isEqualTo("math.sigmoid");

    // sigmoid(0) = 0.5
    assertThat((Double) fn.execute(new Object[]{0}, null)).isCloseTo(0.5, within(0.0001));

    // sigmoid at large positive value approaches 1
    assertThat((Double) fn.execute(new Object[]{10}, null)).isCloseTo(1.0, within(0.001));

    // sigmoid at large negative value approaches 0
    assertThat((Double) fn.execute(new Object[]{-10}, null)).isCloseTo(0.0, within(0.001));
  }

  @Test
  void mathSigmoidNullHandling() {
    final MathSigmoid fn = new MathSigmoid();
    assertThat(fn.execute(new Object[]{null}, null)).isNull();
  }

  @Test
  void mathSigmoidMetadata() {
    final MathSigmoid fn = new MathSigmoid();

    assertThat(fn.getMinArgs()).isEqualTo(1);
    assertThat(fn.getMaxArgs()).isEqualTo(1);
    assertThat(fn.getDescription()).contains("sigmoid");
  }

  // ============ MathSigmoidPrime tests ============

  @Test
  void mathSigmoidPrimeBasic() {
    final MathSigmoidPrime fn = new MathSigmoidPrime();
    assertThat(fn.getName()).isEqualTo("math.sigmoidPrime");

    // sigmoidPrime(0) = 0.25 (derivative at x=0)
    assertThat((Double) fn.execute(new Object[]{0}, null)).isCloseTo(0.25, within(0.0001));

    // sigmoidPrime approaches 0 at extreme values
    assertThat((Double) fn.execute(new Object[]{10}, null)).isCloseTo(0.0, within(0.001));
    assertThat((Double) fn.execute(new Object[]{-10}, null)).isCloseTo(0.0, within(0.001));
  }

  @Test
  void mathSigmoidPrimeNullHandling() {
    final MathSigmoidPrime fn = new MathSigmoidPrime();
    assertThat(fn.execute(new Object[]{null}, null)).isNull();
  }

  // ============ MathTanh tests ============

  @Test
  void mathTanhBasic() {
    final MathTanh fn = new MathTanh();
    assertThat(fn.getName()).isEqualTo("math.tanh");

    // tanh(0) = 0
    assertThat((Double) fn.execute(new Object[]{0}, null)).isCloseTo(0.0, within(0.0001));

    // tanh approaches 1 at large positive value
    assertThat((Double) fn.execute(new Object[]{10}, null)).isCloseTo(1.0, within(0.001));

    // tanh approaches -1 at large negative value
    assertThat((Double) fn.execute(new Object[]{-10}, null)).isCloseTo(-1.0, within(0.001));
  }

  @Test
  void mathTanhNullHandling() {
    final MathTanh fn = new MathTanh();
    assertThat(fn.execute(new Object[]{null}, null)).isNull();
  }

  // ============ MathCosh tests ============

  @Test
  void mathCoshBasic() {
    final MathCosh fn = new MathCosh();
    assertThat(fn.getName()).isEqualTo("math.cosh");

    // cosh(0) = 1
    assertThat((Double) fn.execute(new Object[]{0}, null)).isCloseTo(1.0, within(0.0001));

    // cosh is symmetric: cosh(x) = cosh(-x)
    final double coshPositive = (Double) fn.execute(new Object[]{2}, null);
    final double coshNegative = (Double) fn.execute(new Object[]{-2}, null);
    assertThat(coshPositive).isCloseTo(coshNegative, within(0.0001));
  }

  @Test
  void mathCoshNullHandling() {
    final MathCosh fn = new MathCosh();
    assertThat(fn.execute(new Object[]{null}, null)).isNull();
  }

  // ============ MathSinh tests ============

  @Test
  void mathSinhBasic() {
    final MathSinh fn = new MathSinh();
    assertThat(fn.getName()).isEqualTo("math.sinh");

    // sinh(0) = 0
    assertThat((Double) fn.execute(new Object[]{0}, null)).isCloseTo(0.0, within(0.0001));

    // sinh is antisymmetric: sinh(-x) = -sinh(x)
    final double sinhPositive = (Double) fn.execute(new Object[]{2}, null);
    final double sinhNegative = (Double) fn.execute(new Object[]{-2}, null);
    assertThat(sinhPositive).isCloseTo(-sinhNegative, within(0.0001));
  }

  @Test
  void mathSinhNullHandling() {
    final MathSinh fn = new MathSinh();
    assertThat(fn.execute(new Object[]{null}, null)).isNull();
  }

  // ============ MathMaxLong tests ============

  @Test
  void mathMaxLongBasic() {
    final MathMaxLong fn = new MathMaxLong();
    assertThat(fn.getName()).isEqualTo("math.maxLong");

    assertThat(fn.execute(new Object[]{}, null)).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void mathMaxLongMetadata() {
    final MathMaxLong fn = new MathMaxLong();

    assertThat(fn.getMinArgs()).isEqualTo(0);
    assertThat(fn.getMaxArgs()).isEqualTo(0);
    assertThat(fn.getDescription()).contains("maximum Long");
  }

  // ============ MathMinLong tests ============

  @Test
  void mathMinLongBasic() {
    final MathMinLong fn = new MathMinLong();
    assertThat(fn.getName()).isEqualTo("math.minLong");

    assertThat(fn.execute(new Object[]{}, null)).isEqualTo(Long.MIN_VALUE);
  }

  // ============ MathMaxDouble tests ============

  @Test
  void mathMaxDoubleBasic() {
    final MathMaxDouble fn = new MathMaxDouble();
    assertThat(fn.getName()).isEqualTo("math.maxDouble");

    assertThat(fn.execute(new Object[]{}, null)).isEqualTo(Double.MAX_VALUE);
  }

  // ============ Input type conversion tests ============

  @Test
  void mathFunctionsAcceptDifferentNumberTypes() {
    final MathSigmoid fn = new MathSigmoid();

    // Integer
    assertThat(fn.execute(new Object[]{0}, null)).isNotNull();

    // Long
    assertThat(fn.execute(new Object[]{0L}, null)).isNotNull();

    // Double
    assertThat(fn.execute(new Object[]{0.0}, null)).isNotNull();

    // Float
    assertThat(fn.execute(new Object[]{0.0f}, null)).isNotNull();

    // String (should be parsed)
    assertThat(fn.execute(new Object[]{"0"}, null)).isNotNull();
  }

  // ============ Mathematical identity tests ============

  @Test
  void hyperbolicIdentity() {
    // cosh^2(x) - sinh^2(x) = 1
    final MathCosh coshFn = new MathCosh();
    final MathSinh sinhFn = new MathSinh();

    final double x = 1.5;
    final double coshX = (Double) coshFn.execute(new Object[]{x}, null);
    final double sinhX = (Double) sinhFn.execute(new Object[]{x}, null);

    assertThat(coshX * coshX - sinhX * sinhX).isCloseTo(1.0, within(0.0001));
  }

  @Test
  void tanhIdentity() {
    // tanh(x) = sinh(x) / cosh(x)
    final MathTanh tanhFn = new MathTanh();
    final MathCosh coshFn = new MathCosh();
    final MathSinh sinhFn = new MathSinh();

    final double x = 1.5;
    final double tanhX = (Double) tanhFn.execute(new Object[]{x}, null);
    final double coshX = (Double) coshFn.execute(new Object[]{x}, null);
    final double sinhX = (Double) sinhFn.execute(new Object[]{x}, null);

    assertThat(tanhX).isCloseTo(sinhX / coshX, within(0.0001));
  }
}
