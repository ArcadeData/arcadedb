package com.arcadedb.query.opencypher.executor.steps;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class CypherStatisticsHelperTest {

  @Test
  void countsPreExistingPropertiesNotReSetAndSkipsInternal() {
    final Set<String> existing = new LinkedHashSet<>();
    existing.add("a");
    existing.add("b");
    existing.add("@type"); // internal - never counted
    final Map<String, Object> replacement = new HashMap<>();
    replacement.put("a", 1); // re-set, not a removal
    // b is removed (absent from replacement) -> counts 1
    assertThat(CypherStatisticsHelper.countRemovedProperties(existing, replacement)).isEqualTo(1);
  }

  @Test
  void nullValuedReplacementIsARemoval() {
    final Set<String> existing = new LinkedHashSet<>();
    existing.add("a");
    final Map<String, Object> replacement = new HashMap<>();
    replacement.put("a", null); // map.get("a") == null -> counts as removed
    assertThat(CypherStatisticsHelper.countRemovedProperties(existing, replacement)).isEqualTo(1);
  }
}
