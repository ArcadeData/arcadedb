package com.arcadedb.database;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ProtocolContextTest {
  @Test
  void defaultsToInternal() {
    ProtocolContext.clear();
    assertThat(ProtocolContext.get()).isEqualTo(ProtocolContext.INTERNAL);
  }

  @Test
  void setAndClear() {
    ProtocolContext.set("bolt");
    assertThat(ProtocolContext.get()).isEqualTo("bolt");
    ProtocolContext.clear();
    assertThat(ProtocolContext.get()).isEqualTo(ProtocolContext.INTERNAL);
  }

  @Test
  void nullResetsToInternal() {
    ProtocolContext.set("http");
    ProtocolContext.set(null);
    assertThat(ProtocolContext.get()).isEqualTo(ProtocolContext.INTERNAL);
  }
}
