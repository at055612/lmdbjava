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

import static com.jakewharton.byteunits.BinaryByteUnit.KIBIBYTES;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.DbiFlags.MDB_DUPSORT;
import static org.lmdbjava.DbiFlags.MDB_INTEGERDUP;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.KeyRange.all;
import static org.lmdbjava.KeyRange.allBackward;
import static org.lmdbjava.KeyRange.atLeast;
import static org.lmdbjava.KeyRange.atLeastBackward;
import static org.lmdbjava.KeyRange.atMost;
import static org.lmdbjava.KeyRange.atMostBackward;
import static org.lmdbjava.KeyRange.closed;
import static org.lmdbjava.KeyRange.closedBackward;
import static org.lmdbjava.KeyRange.closedOpen;
import static org.lmdbjava.KeyRange.closedOpenBackward;
import static org.lmdbjava.KeyRange.greaterThan;
import static org.lmdbjava.KeyRange.greaterThanBackward;
import static org.lmdbjava.KeyRange.lessThan;
import static org.lmdbjava.KeyRange.lessThanBackward;
import static org.lmdbjava.KeyRange.open;
import static org.lmdbjava.KeyRange.openBackward;
import static org.lmdbjava.KeyRange.openClosed;
import static org.lmdbjava.KeyRange.openClosedBackward;
import static org.lmdbjava.TestUtils.DB_1;
import static org.lmdbjava.TestUtils.DB_2;
import static org.lmdbjava.TestUtils.DB_3;
import static org.lmdbjava.TestUtils.DB_4;
import static org.lmdbjava.TestUtils.POSIX_MODE;
import static org.lmdbjava.TestUtils.bb;
import static org.lmdbjava.TestUtils.bbNative;
import static org.lmdbjava.TestUtils.getNativeInt;

import com.google.common.primitives.UnsignedBytes;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.lmdbjava.CursorIterable.KeyVal;

/**
 * Test {@link CursorIterable} using {@link DbiFlags#MDB_INTEGERKEY} to ensure that
 * comparators work with native order integer keys.
 */
@Ignore // Waiting for the merge of stroomdev66's cursor tests
@RunWith(Parameterized.class)
public final class CursorIterableIntegerDupTest {

  private static final DbiFlagSet DBI_FLAGS = DbiFlagSet.of(MDB_CREATE, MDB_INTEGERDUP, MDB_DUPSORT);
  private static final BufferProxy<ByteBuffer> BUFFER_PROXY = ByteBufferProxy.PROXY_OPTIMAL;
  private static final List<Map.Entry<Integer, Integer>> INPUT_DATA;

  static {
    // 2 => 21
    // 2 => 22
    // 3 => 31
    // ...
    // 9 => 92
    INPUT_DATA = new ArrayList<>();
    for (int i = 2; i <= 9; i++) {
      final int val1 = (i * 10) + 1;
      final int val2 = (i * 10) + 2;
      INPUT_DATA.add(new AbstractMap.SimpleEntry<>(i, val1));
      INPUT_DATA.add(new AbstractMap.SimpleEntry<>(i, val2));
    }
  }

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  private Env<ByteBuffer> env;
  private Deque<Map.Entry<Integer, Integer>> expectedEntriesDeque;

  /**
   * Injected by {@link #data()} with appropriate runner.
   */
  @SuppressWarnings("ClassEscapesDefinedScope")
  @Parameterized.Parameter
  public DbiFactory dbiFactory;

  @Parameterized.Parameters(name = "{index}: dbi: {0}")
  public static Object[] data() {
    final DbiFactory defaultComparator = new DbiFactory("defaultComparator", env ->
        env.buildDbi()
            .withDbName(DB_1)
            .withDefaultComparator()
            .withDbiFlags(DBI_FLAGS)
            .open());
    final DbiFactory nativeComparator = new DbiFactory("nativeComparator", env ->
        env.buildDbi()
            .withDbName(DB_2)
            .withNativeComparator()
            .withDbiFlags(DBI_FLAGS)
            .open());
    final DbiFactory callbackComparator = new DbiFactory("callbackComparator", env ->
        env.buildDbi()
            .withDbName(DB_3)
            .withCallbackComparator(BUFFER_PROXY.getComparator(DBI_FLAGS))
            .withDbiFlags(DBI_FLAGS)
            .open());
    final DbiFactory iteratorComparator = new DbiFactory("iteratorComparator", env ->
        env.buildDbi()
            .withDbName(DB_4)
            .withIteratorComparator(BUFFER_PROXY.getComparator(DBI_FLAGS))
            .withDbiFlags(DBI_FLAGS)
            .open());
    return new Object[]{
        defaultComparator,
        nativeComparator,
        callbackComparator,
        iteratorComparator};
  }

  @Before
  public void before() throws IOException {
    final File path = tmp.newFile();
    final BufferProxy<ByteBuffer> bufferProxy = ByteBufferProxy.PROXY_OPTIMAL;
    env =
        create(bufferProxy)
            .setMapSize(KIBIBYTES.toBytes(256))
            .setMaxReaders(1)
            .setMaxDbs(3)
            .open(path, POSIX_MODE, MDB_NOSUBDIR);

    populateExpectedEntriesDeque();
  }

  @After
  public void after() {
    env.close();
  }

  @Test
  public void allBackwardTest() {
    verify(allBackward(), 8, 6, 4, 2);
  }

  @Test
  public void allTest() {
    verify(all(), 2, 4, 6, 8);
  }

  @Test
  public void atLeastBackwardTest() {
    verify(atLeastBackward(bbNative(5)), 4, 2);
    verify(atLeastBackward(bbNative(6)), 6, 4, 2);
    verify(atLeastBackward(bbNative(9)), 8, 6, 4, 2);
  }

  @Test
  public void atLeastTest() {
    verify(atLeast(bbNative(5)), 6, 8);
    verify(atLeast(bbNative(6)), 6, 8);
  }

  @Test
  public void atMostBackwardTest() {
    verify(atMostBackward(bbNative(5)), 8, 6);
    verify(atMostBackward(bbNative(6)), 8, 6);
  }

  @Test
  public void atMostTest() {
    verify(atMost(bbNative(5)), 2, 4);
    verify(atMost(bbNative(6)), 2, 4, 6);
  }

  private void populateExpectedEntriesDeque() {
    expectedEntriesDeque = new LinkedList<>();
    expectedEntriesDeque.addAll(INPUT_DATA);
  }

  private void populateDatabase(final Dbi<ByteBuffer> dbi) {
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      final Cursor<ByteBuffer> c = dbi.openCursor(txn);
      for (Map.Entry<Integer, Integer> entry : INPUT_DATA) {
        c.put(bbNative(entry.getKey()), bb(entry.getValue()));
      }
      txn.commit();
    }

    try (Txn<ByteBuffer> txn = env.txnRead();
         CursorIterable<ByteBuffer> c = dbi.iterate(txn)) {

      for (final KeyVal<ByteBuffer> kv : c) {
        System.out.print(getNativeInt(kv.key()) + " => " + kv.val().getInt());
        System.out.print(", ");
      }
      System.out.println();
    }
  }

  private int[] rangeInc(final int fromInc, final int toInc) {
    int idx = 0;
    if (fromInc <= toInc) {
      // Forwards
      final int[] arr = new int[toInc - fromInc + 1];
      for (int i = fromInc; i <= toInc; i++) {
        arr[idx++] = i;
      }
      return arr;
    } else {
      // Backwards
      final int[] arr = new int[fromInc - toInc + 1];
      for (int i = fromInc; i >= toInc; i--) {
        arr[idx++] = i;
      }
      return arr;
    }
  }

  @Test
  public void closedBackwardTest() {
    verify(closedBackward(bbNative(7), bbNative(3)), rangeInc(7, 3));
    verify(closedBackward(bbNative(6), bbNative(2)), rangeInc(6, 2));
    verify(closedBackward(bbNative(9), bbNative(3)), rangeInc(9, 3));
  }

  @Test
  public void closedOpenBackwardTest() {
    verify(closedOpenBackward(bbNative(8), bbNative(3)), rangeInc(8, 4));
    verify(closedOpenBackward(bbNative(7), bbNative(2)), rangeInc(7, 3));
    verify(closedOpenBackward(bbNative(9), bbNative(3)), rangeInc(9, 4));
  }

  @Test
  public void closedOpenTest() {
    verify(closedOpen(bbNative(3), bbNative(8)), rangeInc(3, 7));
    verify(closedOpen(bbNative(2), bbNative(6)), rangeInc(2, 5));
  }

  @Test
  public void closedTest() {
    verify(closed(bbNative(3), bbNative(7)), rangeInc(3, 7));
    verify(closed(bbNative(2), bbNative(6)), rangeInc(2, 6));
    verify(closed(bbNative(1), bbNative(7)), rangeInc(2, 7));
  }

  @Test
  public void greaterThanBackwardTest() {
    verify(greaterThanBackward(bbNative(6)), rangeInc(5, 2));
    verify(greaterThanBackward(bbNative(7)), rangeInc(6, 2));
    verify(greaterThanBackward(bbNative(9)), rangeInc(8, 2));
  }

  @Test
  public void greaterThanTest() {
    verify(greaterThan(bbNative(4)), rangeInc(5, 9));
    verify(greaterThan(bbNative(3)), rangeInc(4, 9));
  }

  @Test(expected = IllegalStateException.class)
  public void iterableOnlyReturnedOnce() {
    final Dbi<ByteBuffer> db = getDb();
    try (Txn<ByteBuffer> txn = env.txnRead();
         CursorIterable<ByteBuffer> c = db.iterate(txn)) {
      c.iterator(); // ok
      c.iterator(); // fails
    }
  }

  @Test
  public void iterate() {
    populateExpectedEntriesDeque();
    final Dbi<ByteBuffer> db = getDb();
    try (Txn<ByteBuffer> txn = env.txnRead();
         CursorIterable<ByteBuffer> c = db.iterate(txn)) {

      for (final KeyVal<ByteBuffer> kv : c) {
        final Map.Entry<Integer, Integer> entry = expectedEntriesDeque.pollFirst();
//        System.out.println(entry.getKey() + " => " + entry.getValue());
        assertThat(getNativeInt(kv.key()), is(entry.getKey()));
        assertThat(kv.val().getInt(), is(entry.getValue()));
      }
    }
  }

  @Test(expected = IllegalStateException.class)
  public void iteratorOnlyReturnedOnce() {
    final Dbi<ByteBuffer> db = getDb();
    try (Txn<ByteBuffer> txn = env.txnRead();
         CursorIterable<ByteBuffer> c = db.iterate(txn)) {
      c.iterator(); // ok
      c.iterator(); // fails
    }
  }

  @Test
  public void lessThanBackwardTest() {
    verify(lessThanBackward(bbNative(5)), 8, 6);
    verify(lessThanBackward(bbNative(2)), 8, 6, 4);
  }

  @Test
  public void lessThanTest() {
    verify(lessThan(bbNative(5)), 2, 4);
    verify(lessThan(bbNative(8)), 2, 4, 6);
  }

  @Test(expected = NoSuchElementException.class)
  public void nextThrowsNoSuchElementExceptionIfNoMoreElements() {
    populateExpectedEntriesDeque();
    final Dbi<ByteBuffer> db = getDb();
    try (Txn<ByteBuffer> txn = env.txnRead();
         CursorIterable<ByteBuffer> c = db.iterate(txn)) {
      final Iterator<KeyVal<ByteBuffer>> i = c.iterator();
      while (i.hasNext()) {
        final KeyVal<ByteBuffer> kv = i.next();
        assertThat(getNativeInt(kv.key()), is(expectedEntriesDeque.pollFirst()));
        assertThat(kv.val().getInt(), is(expectedEntriesDeque.pollFirst()));
      }
      assertThat(i.hasNext(), is(false));
      i.next();
    }
  }

  @Test
  public void openBackwardTest() {
    verify(openBackward(bbNative(7), bbNative(2)), 6, 4);
    verify(openBackward(bbNative(8), bbNative(1)), 6, 4, 2);
    verify(openBackward(bbNative(9), bbNative(4)), 8, 6);
  }

  @Test
  public void openClosedBackwardTest() {
    verify(openClosedBackward(bbNative(7), bbNative(2)), 6, 4, 2);
    verify(openClosedBackward(bbNative(8), bbNative(4)), 6, 4);
    verify(openClosedBackward(bbNative(9), bbNative(4)), 8, 6, 4);
  }

  @Test
  public void openClosedBackwardTestWithGuava() {
    final Comparator<byte[]> guava = UnsignedBytes.lexicographicalComparator();
    final Comparator<ByteBuffer> comparator =
        (bb1, bb2) -> {
          final byte[] array1 = new byte[bb1.remaining()];
          final byte[] array2 = new byte[bb2.remaining()];
          bb1.mark();
          bb2.mark();
          bb1.get(array1);
          bb2.get(array2);
          bb1.reset();
          bb2.reset();
          return guava.compare(array1, array2);
        };
    final Dbi<ByteBuffer> guavaDbi = env.openDbi(DB_1, comparator, MDB_CREATE);
    populateDatabase(guavaDbi);
    verify(openClosedBackward(bbNative(7), bbNative(2)), guavaDbi, 6, 4, 2);
    verify(openClosedBackward(bbNative(8), bbNative(4)), guavaDbi, 6, 4);
  }

  @Test
  public void openClosedTest() {
    verify(openClosed(bbNative(3), bbNative(8)), 4, 6, 8);
    verify(openClosed(bbNative(2), bbNative(6)), 4, 6);
  }

  @Test
  public void openTest() {
    verify(open(bbNative(3), bbNative(7)), 4, 6);
    verify(open(bbNative(2), bbNative(8)), 4, 6);
  }

  @Test
  public void removeOddElements() {
    final Dbi<ByteBuffer> db = getDb();
    verify(db, all(), 2, 4, 6, 8);
    int idx = -1;
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      try (CursorIterable<ByteBuffer> ci = db.iterate(txn)) {
        final Iterator<KeyVal<ByteBuffer>> c = ci.iterator();
        while (c.hasNext()) {
          c.next();
          idx++;
          if (idx % 2 == 0) {
            c.remove();
          }
        }
      }
      txn.commit();
    }
    verify(db, all(), 4, 8);
  }

  @Test(expected = Env.AlreadyClosedException.class)
  public void nextWithClosedEnvTest() {
    final Dbi<ByteBuffer> db = getDb();
    try (Txn<ByteBuffer> txn = env.txnRead()) {
      try (CursorIterable<ByteBuffer> ci = db.iterate(txn, KeyRange.all())) {
        final Iterator<KeyVal<ByteBuffer>> c = ci.iterator();

        env.close();
        c.next();
      }
    }
  }

  @Test(expected = Env.AlreadyClosedException.class)
  public void removeWithClosedEnvTest() {
    final Dbi<ByteBuffer> db = getDb();
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      try (CursorIterable<ByteBuffer> ci = db.iterate(txn, KeyRange.all())) {
        final Iterator<KeyVal<ByteBuffer>> c = ci.iterator();

        final KeyVal<ByteBuffer> keyVal = c.next();
        assertThat(keyVal, Matchers.notNullValue());

        env.close();
        c.remove();
      }
    }
  }

  @Test(expected = Env.AlreadyClosedException.class)
  public void hasNextWithClosedEnvTest() {
    final Dbi<ByteBuffer> db = getDb();
    try (Txn<ByteBuffer> txn = env.txnRead()) {
      try (CursorIterable<ByteBuffer> ci = db.iterate(txn, KeyRange.all())) {
        final Iterator<KeyVal<ByteBuffer>> c = ci.iterator();

        env.close();
        c.hasNext();
      }
    }
  }

  @Test(expected = Env.AlreadyClosedException.class)
  public void forEachRemainingWithClosedEnvTest() {
    final Dbi<ByteBuffer> db = getDb();
    try (Txn<ByteBuffer> txn = env.txnRead()) {
      try (CursorIterable<ByteBuffer> ci = db.iterate(txn, KeyRange.all())) {
        final Iterator<KeyVal<ByteBuffer>> c = ci.iterator();

        env.close();
        c.forEachRemaining(keyVal -> {
        });
      }
    }
  }

  private void verify(final KeyRange<ByteBuffer> range, final int... expectedKeys) {
    // Verify using all comparator types
    final Dbi<ByteBuffer> db = getDb();
    verify(range, db, expectedKeys);
  }

  private void verify(final Dbi<ByteBuffer> dbi,
                      final KeyRange<ByteBuffer> range,
                      final int... expectedKeys) {
    verify(range, dbi, expectedKeys);
  }

  private void verify(final KeyRange<ByteBuffer> range,
                      final Dbi<ByteBuffer> dbi,
                      final int... expectedKeys) {
    final boolean isForward = range.getType().isDirectionForward();

    final List<Integer> expectedValues = Arrays.stream(expectedKeys)
        .boxed()
        .flatMap(key -> {
          final int base = key * 10;
          return isForward
              ? Stream.of(base + 1, base + 2)
              : Stream.of(base + 2, base + 1);
        })
        .collect(Collectors.toList());

    final List<Integer> results = new ArrayList<>();
    System.out.println(rangeToString(range) + ", expected: " + expectedValues);

    try (Txn<ByteBuffer> txn = env.txnRead();
         CursorIterable<ByteBuffer> c = dbi.iterate(txn, range)) {
      for (final KeyVal<ByteBuffer> kv : c) {
        final int key = getNativeInt(kv.key());
        final int val = kv.val().getInt();
        System.out.println(key + " => " + val);
        results.add(val);
        assertThat(val, CoreMatchers.anyOf(
            CoreMatchers.is((key * 10) + 1),
            CoreMatchers.is((key * 10) + 2)));
      }
    }

    assertThat(results, hasSize(expectedValues.size()));
    for (int idx = 0; idx < results.size(); idx++) {
      assertThat(results.get(idx), is(expectedValues.get(idx)));
    }
  }

  private String rangeToString(final KeyRange<ByteBuffer> range) {
    final ByteBuffer start = range.getStart();
    final ByteBuffer stop = range.getStop();
    return range.getType() + " start: " + (start != null ? getNativeInt(start) : "")
        + " stop: " + (stop != null ? getNativeInt(stop) : "");
  }

  private Dbi<ByteBuffer> getDb() {
    final Dbi<ByteBuffer> dbi = dbiFactory.factory.apply(env);
    populateDatabase(dbi);
    return dbi;
  }


  // --------------------------------------------------------------------------------


  private static class DbiFactory {
    private final String name;
    private final Function<Env<ByteBuffer>, Dbi<ByteBuffer>> factory;

    private DbiFactory(String name, Function<Env<ByteBuffer>, Dbi<ByteBuffer>> factory) {
      this.name = name;
      this.factory = factory;
    }

    @Override
    public String toString() {
      return name;
    }
  }
}
