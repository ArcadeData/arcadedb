package com.arcadedb.index.vector;

import com.github.jelmerk.knn.Item;

import java.util.*;

public class Word implements Item<String, float[]> {

  private static final long serialVersionUID = 1L;

  private final String  id;
  private final float[] vector;

  public Word(String id, float[] vector) {
    this.id = id;
    this.vector = vector;
  }

  @Override
  public String id() {
    return id;
  }

  @Override
  public float[] vector() {
    return vector;
  }

  @Override
  public int dimensions() {
    return vector.length;
  }

  @Override
  public String toString() {
    return "Word{" + "id='" + id + '\'' + ", vector=" + Arrays.toString(vector) + '}';
  }
}
