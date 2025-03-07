/*
 * Copyright © 2016-2025 The LmdbJava Open Source Project
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
 */
package org.lmdbjava;

import static java.util.Objects.requireNonNull;
import static org.lmdbjava.KeyRangeType.CursorOp.FIRST;
import static org.lmdbjava.KeyRangeType.CursorOp.GET_START_KEY;
import static org.lmdbjava.KeyRangeType.CursorOp.GET_START_KEY_BACKWARD;
import static org.lmdbjava.KeyRangeType.CursorOp.LAST;
import static org.lmdbjava.KeyRangeType.CursorOp.NEXT;
import static org.lmdbjava.KeyRangeType.CursorOp.PREV;
import static org.lmdbjava.KeyRangeType.IteratorOp.CALL_NEXT_OP;
import static org.lmdbjava.KeyRangeType.IteratorOp.RELEASE;
import static org.lmdbjava.KeyRangeType.IteratorOp.TERMINATE;

import java.util.function.Supplier;

/**
 * Key range type.
 *
 * <p>The terminology used in this class is adapted from Google Guava's ranges. Refer to the <a
 * href="https://github.com/google/guava/wiki/RangesExplained">Ranges Explained</a> wiki page for
 * more information. LmddJava prepends either "FORWARD" or "BACKWARD" to denote the iterator order.
 *
 * <p>In the examples below, it is assumed the table has keys 2, 4, 6 and 8.
 */
public enum KeyRangeType {

  /**
   * Starting on the first key and iterate forward until no keys remain.
   *
   * <p>The "start" and "stop" values are ignored.
   *
   * <p>In our example, the returned keys would be 2, 4, 6 and 8.
   */
  FORWARD_ALL(true, false, false),
  /**
   * Start on the passed key (or the first key immediately after it) and iterate forward until no
   * keys remain.
   *
   * <p>The "start" value is required. The "stop" value is ignored.
   *
   * <p>In our example and with a passed search key of 5, the returned keys would be 6 and 8. With a
   * passed key of 6, the returned keys would be 6 and 8.
   */
  FORWARD_AT_LEAST(true, true, false),
  /**
   * Start on the first key and iterate forward until a key equal to it (or the first key
   * immediately after it) is reached.
   *
   * <p>The "stop" value is required. The "start" value is ignored.
   *
   * <p>In our example and with a passed search key of 5, the returned keys would be 2 and 4. With a
   * passed key of 6, the returned keys would be 2, 4 and 6.
   */
  FORWARD_AT_MOST(true, false, true),
  /**
   * Iterate forward between the passed keys, matching on the first keys directly equal to the
   * passed key (or immediately following it in the case of the "start" key, or immediately
   * preceding it in the case of the "stop" key).
   *
   * <p>The "start" and "stop" values are both required.
   *
   * <p>In our example and with a passed search range of 3 - 7, the returned keys would be 4 and 6.
   * With a range of 2 - 6, the keys would be 2, 4 and 6.
   */
  FORWARD_CLOSED(true, true, true),
  /**
   * Iterate forward between the passed keys, matching on the first keys directly equal to the
   * passed key (or immediately following it in the case of the "start" key, or immediately
   * preceding it in the case of the "stop" key). Do not return the "stop" key.
   *
   * <p>The "start" and "stop" values are both required.
   *
   * <p>In our example and with a passed search range of 3 - 8, the returned keys would be 4 and 6.
   * With a range of 2 - 6, the keys would be 2 and 4.
   */
  FORWARD_CLOSED_OPEN(true, true, true),
  /**
   * Start after the passed key (but not equal to it) and iterate forward until no keys remain.
   *
   * <p>The "start" value is required. The "stop" value is ignored.
   *
   * <p>In our example and with a passed search key of 4, the returned keys would be 6 and 8. With a
   * passed key of 3, the returned keys would be 4, 6 and 8.
   */
  FORWARD_GREATER_THAN(true, true, false),
  /**
   * Start on the first key and iterate forward until a key the passed key has been reached (but do
   * not return that key).
   *
   * <p>The "stop" value is required. The "start" value is ignored.
   *
   * <p>In our example and with a passed search key of 5, the returned keys would be 2 and 4. With a
   * passed key of 8, the returned keys would be 2, 4 and 6.
   */
  FORWARD_LESS_THAN(true, false, true),
  /**
   * Iterate forward between the passed keys but not equal to either of them.
   *
   * <p>The "start" and "stop" values are both required.
   *
   * <p>In our example and with a passed search range of 3 - 7, the returned keys would be 4 and 6.
   * With a range of 2 - 8, the key would be 4 and 6.
   */
  FORWARD_OPEN(true, true, true),
  /**
   * Iterate forward between the passed keys. Do not return the "start" key, but do return the
   * "stop" key.
   *
   * <p>The "start" and "stop" values are both required.
   *
   * <p>In our example and with a passed search range of 3 - 8, the returned keys would be 4, 6 and
   * 8. With a range of 2 - 6, the keys would be 4 and 6.
   */
  FORWARD_OPEN_CLOSED(true, true, true),
  /**
   * Start on the last key and iterate backward until no keys remain.
   *
   * <p>The "start" and "stop" values are ignored.
   *
   * <p>In our example, the returned keys would be 8, 6, 4 and 2.
   */
  BACKWARD_ALL(false, false, false),
  /**
   * Start on the passed key (or the first key immediately preceding it) and iterate backward until
   * no keys remain.
   *
   * <p>The "start" value is required. The "stop" value is ignored.
   *
   * <p>In our example and with a passed search key of 5, the returned keys would be 4 and 2. With a
   * passed key of 6, the returned keys would be 6, 4 and 2. With a passed key of 9, the returned
   * keys would be 8, 6, 4 and 2.
   */
  BACKWARD_AT_LEAST(false, true, false),
  /**
   * Start on the last key and iterate backward until a key equal to it (or the first key
   * immediately preceding it it) is reached.
   *
   * <p>The "stop" value is required. The "start" value is ignored.
   *
   * <p>In our example and with a passed search key of 5, the returned keys would be 8 and 6. With a
   * passed key of 6, the returned keys would be 8 and 6.
   */
  BACKWARD_AT_MOST(false, false, true),
  /**
   * Iterate backward between the passed keys, matching on the first keys directly equal to the
   * passed key (or immediately preceding it in the case of the "start" key, or immediately
   * following it in the case of the "stop" key).
   *
   * <p>The "start" and "stop" values are both required.
   *
   * <p>In our example and with a passed search range of 7 - 3, the returned keys would be 6 and 4.
   * With a range of 6 - 2, the keys would be 6, 4 and 2. With a range of 9 - 3, the returned keys
   * would be 8, 6 and 4.
   */
  BACKWARD_CLOSED(false, true, true),
  /**
   * Iterate backward between the passed keys, matching on the first keys directly equal to the
   * passed key (or immediately preceding it in the case of the "start" key, or immediately
   * following it in the case of the "stop" key). Do not return the "stop" key.
   *
   * <p>The "start" and "stop" values are both required.
   *
   * <p>In our example and with a passed search range of 8 - 3, the returned keys would be 8, 6 and
   * 4. With a range of 7 - 2, the keys would be 6 and 4. With a range of 9 - 3, the keys would be
   * 8, 6 and 4.
   */
  BACKWARD_CLOSED_OPEN(false, true, true),
  /**
   * Start immediate prior to the passed key (but not equal to it) and iterate backward until no
   * keys remain.
   *
   * <p>The "start" value is required. The "stop" value is ignored.
   *
   * <p>In our example and with a passed search key of 6, the returned keys would be 4 and 2. With a
   * passed key of 7, the returned keys would be 6, 4 and 2. With a passed key of 9, the returned
   * keys would be 8, 6, 4 and 2.
   */
  BACKWARD_GREATER_THAN(false, true, false),
  /**
   * Start on the last key and iterate backward until the last key greater than the passed "stop"
   * key is reached. Do not return the "stop" key.
   *
   * <p>The "stop" value is required. The "start" value is ignored.
   *
   * <p>In our example and with a passed search key of 5, the returned keys would be 8 and 6. With a
   * passed key of 2, the returned keys would be 8, 6 and 4
   */
  BACKWARD_LESS_THAN(false, false, true),
  /**
   * Iterate backward between the passed keys, but do not return the passed keys.
   *
   * <p>The "start" and "stop" values are both required.
   *
   * <p>In our example and with a passed search range of 7 - 2, the returned keys would be 6 and 4.
   * With a range of 8 - 1, the keys would be 6, 4 and 2. With a range of 9 - 4, the keys would be 8
   * and 6.
   */
  BACKWARD_OPEN(false, true, true),
  /**
   * Iterate backward between the passed keys. Do not return the "start" key, but do return the
   * "stop" key.
   *
   * <p>The "start" and "stop" values are both required.
   *
   * <p>In our example and with a passed search range of 7 - 2, the returned keys would be 6, 4 and
   * 2. With a range of 8 - 4, the keys would be 6 and 4. With a range of 9 - 4, the keys would be
   * 8, 6 and 4.
   */
  BACKWARD_OPEN_CLOSED(false, true, true),

  /**
   * Iterate forward over all entries where the key starts with the key prefix held in startKey.
   *
   * <p>Assuming the table contains keys of the form {@code <int>|<int>} and values
   *
   * <ul>
   *   <li>'200'
   *   <li>'200|202'
   *   <li>'200|204'
   *   <li>'400'
   *   <li>'400|402'
   *   <li>'400|404'
   *   <li>'600'
   *   <li>'600|602'
   *   <li>'600|604'
   * </ul>
   * <p>
   * A startKey of '400' would return keys '400', '400|402' and '400|404' due to the common prefix.
   */
  FORWARD_KEY_PREFIX(true, true, false),
  ;

  private final boolean directionForward;
  private final boolean startKeyRequired;
  private final boolean stopKeyRequired;
  private final CursorOp nextOp;

  KeyRangeType(
      final boolean directionForward,
      final boolean startKeyRequired,
      final boolean stopKeyRequired) {
    this.directionForward = directionForward;
    this.startKeyRequired = startKeyRequired;
    this.stopKeyRequired = stopKeyRequired;
    this.nextOp = directionForward ? NEXT : PREV;
  }

  /**
   * Whether the key space is iterated in the order provided by LMDB.
   *
   * @return true if forward, false if reverse
   */
  public boolean isDirectionForward() {
    return directionForward;
  }

  /**
   * Whether the iteration requires a "start" key.
   *
   * @return true if start key must be non-null
   */
  public boolean isStartKeyRequired() {
    return startKeyRequired;
  }

  /**
   * Whether the iteration requires a "stop" key.
   *
   * @return true if stop key must be non-null
   */
  public boolean isStopKeyRequired() {
    return stopKeyRequired;
  }

  /**
   * Determine the iterator action to take when iterator first begins.
   *
   * <p>The iterator will perform this action and present the resulting key to {@link
   * #iteratorOp(java.util.Comparator, java.lang.Object)} for decision.
   *
   * @return appropriate action in response to this buffer
   */
  CursorOp initialOp() {
    switch (this) {
      case FORWARD_ALL:
        return FIRST;
      case FORWARD_AT_LEAST:
        return GET_START_KEY;
      case FORWARD_AT_MOST:
        return FIRST;
      case FORWARD_CLOSED:
        return GET_START_KEY;
      case FORWARD_CLOSED_OPEN:
        return GET_START_KEY;
      case FORWARD_GREATER_THAN:
        return GET_START_KEY;
      case FORWARD_LESS_THAN:
        return FIRST;
      case FORWARD_OPEN:
        return GET_START_KEY;
      case FORWARD_OPEN_CLOSED:
        return GET_START_KEY;
      case BACKWARD_ALL:
        return LAST;
      case BACKWARD_AT_LEAST:
        return GET_START_KEY_BACKWARD;
      case BACKWARD_AT_MOST:
        return LAST;
      case BACKWARD_CLOSED:
        return GET_START_KEY_BACKWARD;
      case BACKWARD_CLOSED_OPEN:
        return GET_START_KEY_BACKWARD;
      case BACKWARD_GREATER_THAN:
        return GET_START_KEY_BACKWARD;
      case BACKWARD_LESS_THAN:
        return LAST;
      case BACKWARD_OPEN:
        return GET_START_KEY_BACKWARD;
      case BACKWARD_OPEN_CLOSED:
        return GET_START_KEY_BACKWARD;
      case FORWARD_KEY_PREFIX:
        return GET_START_KEY;
      default:
        throw new IllegalStateException("Invalid type");
    }
  }

  /**
   * Determine the iterator action to take when "next" is called or upon request of {@link
   * #iteratorOp(java.util.Comparator, java.lang.Object)}.
   *
   * <p>The iterator will perform this action and present the resulting key to {@link
   * #iteratorOp(java.util.Comparator, java.lang.Object)} for decision.
   *
   * @return appropriate action for this key range type
   */
  CursorOp nextOp() {
    return nextOp;
  }


  // --------------------------------------------------------------------------------


  /**
   * Action now required with the iterator.
   */
  enum IteratorOp {
    /**
     * Consider iterator completed.
     */
    TERMINATE,
    /**
     * Call {@link KeyRange#nextOp()} again and try again.
     */
    CALL_NEXT_OP,
    /**
     * Return the key to the user.
     */
    RELEASE
  }


  // --------------------------------------------------------------------------------


  /**
   * Action now required with the cursor.
   */
  enum CursorOp {
    /**
     * Move to first.
     */
    FIRST,
    /**
     * Move to last.
     */
    LAST,
    /**
     * Get "start" key with {@link GetOp#MDB_SET_RANGE}.
     */
    GET_START_KEY,
    /**
     * Get "start" key with {@link GetOp#MDB_SET_RANGE}, fall back to LAST.
     */
    GET_START_KEY_BACKWARD,
    /**
     * Move forward.
     */
    NEXT,
    /**
     * Move backward.
     */
    PREV
  }


  // --------------------------------------------------------------------------------


  static class IteratorOpTester<T> {
    private final KeyRangeType rangeType;
    private final KeyRange<T> range;
    private final RangeComparator rangeComparator;
    private final PrefixMatcher<T> prefixMatcher;

    // Used to prevent repeated checks for the start key after it has been passed
    private boolean performStartKeyCheck = true;

    IteratorOpTester(final KeyRange<T> range,
                     final RangeComparator rangeComparator,
                     final Supplier<PrefixMatcher<T>> prefixMatcherProvider) {

      this.range = requireNonNull(range, "range required");
      this.rangeType = range.getType();
      this.rangeComparator = requireNonNull(rangeComparator, "rangeComparator required");

      this.prefixMatcher = KeyRangeType.FORWARD_KEY_PREFIX == rangeType
          ? requireNonNull(prefixMatcherProvider, "prefixMatcherProvider required").get()
          : null;
    }

    /**
     * Test buffer to determine whether the entry should be skipped over, consumed or iteration
     * terminated.
     *
     * @param buffer The buffer to test
     * @return The iteration operation.
     */
    IteratorOp test(final T buffer) {
      if (buffer == null) {
        return TERMINATE;
      }
      switch (rangeType) {
        case FORWARD_ALL:
          return RELEASE;
        case FORWARD_AT_LEAST:
          return RELEASE;
        case FORWARD_AT_MOST:
          return rangeComparator.compareToStopKey() > 0 ? TERMINATE : RELEASE;
        case FORWARD_CLOSED:
          return rangeComparator.compareToStopKey() > 0 ? TERMINATE : RELEASE;
        case FORWARD_CLOSED_OPEN:
          return rangeComparator.compareToStopKey() >= 0 ? TERMINATE : RELEASE;
        case FORWARD_GREATER_THAN:
          return notEqualToStartKeyThen(buffer, () -> RELEASE);
        case FORWARD_LESS_THAN:
          return rangeComparator.compareToStopKey() >= 0 ? TERMINATE : RELEASE;
        case FORWARD_OPEN:
          return notEqualToStartKeyThen(
              buffer, () -> rangeComparator.compareToStopKey() >= 0 ? TERMINATE : RELEASE);
        case FORWARD_OPEN_CLOSED:
          return notEqualToStartKeyThen(
              buffer, () -> rangeComparator.compareToStopKey() > 0 ? TERMINATE : RELEASE);
        case BACKWARD_ALL:
          return RELEASE;
        case BACKWARD_AT_LEAST:
          return rangeComparator.compareToStartKey() > 0 ? CALL_NEXT_OP : RELEASE; // rewind
        case BACKWARD_AT_MOST:
          return rangeComparator.compareToStopKey() >= 0 ? RELEASE : TERMINATE;
        case BACKWARD_CLOSED:
          return greaterThanStartKeyThen(() ->
              rangeComparator.compareToStopKey() >= 0 ? RELEASE : TERMINATE);
        case BACKWARD_CLOSED_OPEN:
          return greaterThanStartKeyThen(() ->
              rangeComparator.compareToStopKey() > 0 ? RELEASE : TERMINATE);
        case BACKWARD_GREATER_THAN:
          return rangeComparator.compareToStartKey() >= 0 ? CALL_NEXT_OP : RELEASE;
        case BACKWARD_LESS_THAN:
          return rangeComparator.compareToStopKey() > 0 ? RELEASE : TERMINATE;
        case BACKWARD_OPEN:
          return greaterThanEqualToStartKeyThen(() ->
              rangeComparator.compareToStopKey() > 0 ? RELEASE : TERMINATE);
        case BACKWARD_OPEN_CLOSED:
          return greaterThanEqualToStartKeyThen(() ->
              rangeComparator.compareToStopKey() >= 0 ? RELEASE : TERMINATE);
        case FORWARD_KEY_PREFIX:
          // start is a key prefix
          return prefixMatcher.prefixMatches(buffer, range.getStart()) ? RELEASE : TERMINATE;
        default:
          throw new IllegalStateException("Invalid type");
      }
    }

    /**
     * Skip passed anything that is not equal to the start key, then once found just apply the stopKeyTest
     */
    private IteratorOp notEqualToStartKeyThen(final T buffer, final Supplier<IteratorOp> stopKeyTest) {
      if (performStartKeyCheck) {
        if (buffer.equals(range.getStart())) {
          // Found the start key so skip it, but no need to check for it next time
          performStartKeyCheck = false;
          return CALL_NEXT_OP;
        }
      }
      return stopKeyTest.get();
    }

    /**
     * Skip passed anything that is > start key, then once found just apply the stopKeyTest
     */
    private IteratorOp greaterThanStartKeyThen(final Supplier<IteratorOp> stopKeyTest) {
      if (performStartKeyCheck) {
        if (rangeComparator.compareToStartKey() > 0) {
          return CALL_NEXT_OP;
        } else {
          // Passed the start key so, no need to check for it next time
          performStartKeyCheck = false;
        }
      }
      return stopKeyTest.get();
    }

    /**
     * Skip passed anything that is >= start key, then once found just apply the stopKeyTest
     */
    private IteratorOp greaterThanEqualToStartKeyThen(final Supplier<IteratorOp> stopKeyTest) {
      if (performStartKeyCheck) {
        if (rangeComparator.compareToStartKey() >= 0) {
          return CALL_NEXT_OP;
        } else {
          // Passed the start key so, no need to check for it next time
          performStartKeyCheck = false;
        }
      }
      return stopKeyTest.get();
    }
  }
}
