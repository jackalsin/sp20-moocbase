package edu.berkeley.cs186.database.binarySearch;

import java.util.List;

/**
 * @author jacka
 * @version 1.0 on 7/2/2020
 */
public final class BinarySearch {
  private BinarySearch() {
  }

  public static <T extends Comparable<T>> int getFirstGreaterThanOrEqualsTo(final List<T> keys, final T target) {
    if (keys.isEmpty()) {
      return -1;
    }
    int left = 0, right = keys.size() - 1;
    while (left < right) {
      final int mid = left + (right - left) / 2;
      if (keys.get(mid).compareTo(target) >= 0) {
        right = mid;
      } else {
        left = mid + 1;
      }
    }
    if (keys.get(left).compareTo(target) >= 0) {
      return left;
    }
    return -1;
  }

  public static <T extends Comparable<T>> int getLastSmallerThanOrEqualsTo(final List<T> keys, final T target) {
    if (keys.isEmpty()) {
      return -1;
    }
    int left = 0, right = keys.size() - 1;
    while (left < right) {
      final int mid = left + (1 + right - left) / 2;
      if (keys.get(mid).compareTo(target) <= 0) {
        left = mid;
      } else {
        right = mid - 1;
      }
    }
    if (keys.get(left).compareTo(target) <= 0) {
      return left;
    }
    return -1;
  }
}
