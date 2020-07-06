package edu.berkeley.cs186.database.binarySearch;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author jacka
 * @version 1.0 on 7/2/2020
 */
public class BinarySearchTest {

  @Test
  public void testOnlineCase1() {
    final List<Integer> input = new ArrayList<>();
    for (int i = 0; i < 10; ++i) {
      input.add(i);
    }
    for (int i = 0; i < 10; ++i) {
      assertEquals(i, BinarySearch.getFirstGreaterThanOrEqualsTo(input, i));
    }
  }

  @Test
  public void testOnlineCase2() {
    final List<Integer> input = new ArrayList<>();
    input.add(10);
    input.add(20);
    assertEquals(0, BinarySearch.getFirstGreaterThanOrEqualsTo(input, 0));
    assertEquals(1, BinarySearch.getFirstGreaterThanOrEqualsTo(input, 11));
  }

  @Test
  public void testOnlineCase3() {
    final List<Integer> input = new ArrayList<>();
    input.add(10);
    input.add(20);
    assertEquals(-1, BinarySearch.getLastSmallerThanOrEqualsTo(input, 0));
    assertEquals(0, BinarySearch.getLastSmallerThanOrEqualsTo(input, 10));
    assertEquals(0, BinarySearch.getLastSmallerThanOrEqualsTo(input, 11));
    assertEquals(1, BinarySearch.getLastSmallerThanOrEqualsTo(input, 20));
  }
} 