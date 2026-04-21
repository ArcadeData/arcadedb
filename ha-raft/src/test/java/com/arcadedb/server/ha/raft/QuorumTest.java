package com.arcadedb.server.ha.raft;

import com.arcadedb.exception.ConfigurationException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QuorumTest {

  @Test
  void parseMajority() {
    assertThat(Quorum.parse("majority")).isEqualTo(Quorum.MAJORITY);
    assertThat(Quorum.parse("MAJORITY")).isEqualTo(Quorum.MAJORITY);
  }

  @Test
  void parseAll() {
    assertThat(Quorum.parse("all")).isEqualTo(Quorum.ALL);
    assertThat(Quorum.parse("ALL")).isEqualTo(Quorum.ALL);
  }

  @Test
  void parseInvalidThrows() {
    assertThatThrownBy(() -> Quorum.parse("none"))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("none");
  }

  @Test
  void parseEmptyThrows() {
    assertThatThrownBy(() -> Quorum.parse(""))
        .isInstanceOf(ConfigurationException.class);
  }
}
