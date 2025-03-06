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
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
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
import static org.lmdbjava.PutFlags.MDB_NOOVERWRITE;
import static org.lmdbjava.TestUtils.DB_1;
import static org.lmdbjava.TestUtils.DB_2;
import static org.lmdbjava.TestUtils.DB_3;
import static org.lmdbjava.TestUtils.POSIX_MODE;
import static org.lmdbjava.TestUtils.bb;

import com.google.common.primitives.UnsignedBytes;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.lmdbjava.CursorIterable.KeyVal;

/** Test {@link CursorIterable}. */
public final class CursorIterableTest {

  @Rule public final TemporaryFolder tmp = new TemporaryFolder();
  private Dbi<ByteBuffer> dbJavaComparator;
  private Dbi<ByteBuffer> dbLmdbComparator;
  private Dbi<ByteBuffer> dbCallbackComparator;
  private List<Dbi<ByteBuffer>> dbs = new ArrayList<>();
  private Env<ByteBuffer> env;
  private Deque<Integer> list;

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
    verify(atLeastBackward(bb(5)), 4, 2);
    verify(atLeastBackward(bb(6)), 6, 4, 2);
    verify(atLeastBackward(bb(9)), 8, 6, 4, 2);
  }

  @Test
  public void atLeastTest() {
    verify(atLeast(bb(5)), 6, 8);
    verify(atLeast(bb(6)), 6, 8);
  }

  @Test
  public void atMostBackwardTest() {
    verify(atMostBackward(bb(5)), 8, 6);
    verify(atMostBackward(bb(6)), 8, 6);
  }

  @Test
  public void atMostTest() {
    verify(atMost(bb(5)), 2, 4);
    verify(atMost(bb(6)), 2, 4, 6);
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

    // Use a java comparator for start/stop keys only
    dbJavaComparator = env.openDbi(DB_1, bufferProxy.getUnsignedComparator(), MDB_CREATE);
    // Use LMDB comparator for start/stop keys
    dbLmdbComparator = env.openDbi(DB_2, MDB_CREATE);
    // Use a java comparator for start/stop keys and as a callback comparaotr
    dbCallbackComparator = env.openDbi(
            DB_3, bufferProxy.getUnsignedComparator(), true, MDB_CREATE);

    populateList();

    populateDatabase(dbJavaComparator);
    populateDatabase(dbLmdbComparator);
    populateDatabase(dbCallbackComparator);

    dbs.add(dbJavaComparator);
    dbs.add(dbLmdbComparator);
    dbs.add(dbCallbackComparator);
  }

  private void populateList() {
    list = new LinkedList<>();
    list.addAll(asList(2, 3, 4, 5, 6, 7, 8, 9));
  }

  private void populateDatabase(final Dbi<ByteBuffer> dbi) {
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      final Cursor<ByteBuffer> c = dbi.openCursor(txn);
      c.put(bb(2), bb(3), MDB_NOOVERWRITE);
      c.put(bb(4), bb(5));
      c.put(bb(6), bb(7));
      c.put(bb(8), bb(9));
      txn.commit();
    }
  }

  @Test
  public void closedBackwardTest() {
    verify(closedBackward(bb(7), bb(3)), 6, 4);
    verify(closedBackward(bb(6), bb(2)), 6, 4, 2);
    verify(closedBackward(bb(9), bb(3)), 8, 6, 4);
  }

  @Test
  public void closedOpenBackwardTest() {
    verify(closedOpenBackward(bb(8), bb(3)), 8, 6, 4);
    verify(closedOpenBackward(bb(7), bb(2)), 6, 4);
    verify(closedOpenBackward(bb(9), bb(3)), 8, 6, 4);
  }

  @Test
  public void closedOpenTest() {
    verify(closedOpen(bb(3), bb(8)), 4, 6);
    verify(closedOpen(bb(2), bb(6)), 2, 4);
  }

  @Test
  public void closedTest() {
    verify(closed(bb(3), bb(7)), 4, 6);
    verify(closed(bb(2), bb(6)), 2, 4, 6);
    verify(closed(bb(1), bb(7)), 2, 4, 6);
  }

  public void closedTest1() {
    verify(dbLmdbComparator, closed(bb(3), bb(7)), 4, 6);
  }

  public void closedTest2() {
    verify(dbJavaComparator, closed(bb(3), bb(7)), 4, 6);
  }

  @Test
  public void greaterThanBackwardTest() {
    verify(greaterThanBackward(bb(6)), 4, 2);
    verify(greaterThanBackward(bb(7)), 6, 4, 2);
    verify(greaterThanBackward(bb(9)), 8, 6, 4, 2);
  }

  @Test
  public void greaterThanTest() {
    verify(greaterThan(bb(4)), 6, 8);
    verify(greaterThan(bb(3)), 4, 6, 8);
  }

  @Test(expected = IllegalStateException.class)
  public void iterableOnlyReturnedOnce() {
    for (final Dbi<ByteBuffer> db : dbs) {
      try (Txn<ByteBuffer> txn = env.txnRead();
          CursorIterable<ByteBuffer> c = db.iterate(txn)) {
        c.iterator(); // ok
        c.iterator(); // fails
      }
    }
  }

  @Test
  public void iterate() {
    for (final Dbi<ByteBuffer> db : dbs) {
      populateList();
      try (Txn<ByteBuffer> txn = env.txnRead();
          CursorIterable<ByteBuffer> c = db.iterate(txn)) {

        int cnt = 0;
        for (final KeyVal<ByteBuffer> kv : c) {
          assertThat(kv.key().getInt(), is(list.pollFirst()));
          assertThat(kv.val().getInt(), is(list.pollFirst()));
        }
      }
    }
  }

  @Test(expected = IllegalStateException.class)
  public void iteratorOnlyReturnedOnce() {
    for (final Dbi<ByteBuffer> db : dbs) {
      try (Txn<ByteBuffer> txn = env.txnRead();
          CursorIterable<ByteBuffer> c = db.iterate(txn)) {
        c.iterator(); // ok
        c.iterator(); // fails
      }
    }
  }

  @Test
  public void lessThanBackwardTest() {
    verify(lessThanBackward(bb(5)), 8, 6);
    verify(lessThanBackward(bb(2)), 8, 6, 4);
  }

  @Test
  public void lessThanTest() {
    verify(lessThan(bb(5)), 2, 4);
    verify(lessThan(bb(8)), 2, 4, 6);
  }

  @Test(expected = NoSuchElementException.class)
  public void nextThrowsNoSuchElementExceptionIfNoMoreElements() {
    for (final Dbi<ByteBuffer> db : dbs) {
      populateList();
      try (Txn<ByteBuffer> txn = env.txnRead();
          CursorIterable<ByteBuffer> c = db.iterate(txn)) {
        final Iterator<KeyVal<ByteBuffer>> i = c.iterator();
        while (i.hasNext()) {
          final KeyVal<ByteBuffer> kv = i.next();
          assertThat(kv.key().getInt(), is(list.pollFirst()));
          assertThat(kv.val().getInt(), is(list.pollFirst()));
        }
        assertThat(i.hasNext(), is(false));
        i.next();
      }
    }
  }

  @Test
  public void openBackwardTest() {
    verify(openBackward(bb(7), bb(2)), 6, 4);
    verify(openBackward(bb(8), bb(1)), 6, 4, 2);
    verify(openBackward(bb(9), bb(4)), 8, 6);
  }

  @Test
  public void openClosedBackwardTest() {
    verify(openClosedBackward(bb(7), bb(2)), 6, 4, 2);
    verify(openClosedBackward(bb(8), bb(4)), 6, 4);
    verify(openClosedBackward(bb(9), bb(4)), 8, 6, 4);
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
    verify(openClosedBackward(bb(7), bb(2)), guavaDbi, 6, 4, 2);
    verify(openClosedBackward(bb(8), bb(4)), guavaDbi, 6, 4);
  }

  @Test
  public void openClosedTest() {
    verify(openClosed(bb(3), bb(8)), 4, 6, 8);
    verify(openClosed(bb(2), bb(6)), 4, 6);
  }

  @Test
  public void openTest() {
    verify(open(bb(3), bb(7)), 4, 6);
    verify(open(bb(2), bb(8)), 4, 6);
  }

  @Test
  public void removeOddElements() {
    for (final Dbi<ByteBuffer> db : dbs) {
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
  }

  @Test(expected = Env.AlreadyClosedException.class)
  public void nextWithClosedEnvTest() {
    for (final Dbi<ByteBuffer> db : dbs) {
      try (Txn<ByteBuffer> txn = env.txnRead()) {
        try (CursorIterable<ByteBuffer> ci = db.iterate(txn, KeyRange.all())) {
          final Iterator<KeyVal<ByteBuffer>> c = ci.iterator();

          env.close();
          c.next();
        }
      }
    }
  }

  @Test(expected = Env.AlreadyClosedException.class)
  public void removeWithClosedEnvTest() {
    for (final Dbi<ByteBuffer> db : dbs) {
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
  }

  @Test(expected = Env.AlreadyClosedException.class)
  public void hasNextWithClosedEnvTest() {
    for (final Dbi<ByteBuffer> db : dbs) {
      try (Txn<ByteBuffer> txn = env.txnRead()) {
        try (CursorIterable<ByteBuffer> ci = db.iterate(txn, KeyRange.all())) {
          final Iterator<KeyVal<ByteBuffer>> c = ci.iterator();

          env.close();
          c.hasNext();
        }
      }
    }
  }

  @Test(expected = Env.AlreadyClosedException.class)
  public void forEachRemainingWithClosedEnvTest() {
    for (final Dbi<ByteBuffer> db : dbs) {
      try (Txn<ByteBuffer> txn = env.txnRead()) {
        try (CursorIterable<ByteBuffer> ci = db.iterate(txn, KeyRange.all())) {
          final Iterator<KeyVal<ByteBuffer>> c = ci.iterator();

          env.close();
          c.forEachRemaining(keyVal -> {});
        }
      }
    }
  }

  @Test
  public void testSignedVsUnsigned() {
    final ByteBuffer val1 = bb(1);
    final ByteBuffer val2 = bb(2);
    final ByteBuffer val110 = bb(110);
    final ByteBuffer val111 = bb(111);
    final ByteBuffer val150 = bb(150);

    final BufferProxy<ByteBuffer> bufferProxy = ByteBufferProxy.PROXY_OPTIMAL;
    final Comparator<ByteBuffer> unsignedComparator = bufferProxy.getUnsignedComparator();
    final Comparator<ByteBuffer> signedComparator = bufferProxy.getSignedComparator();

    // Compare the same
    assertThat(
            unsignedComparator.compare(val1, val2),
            Matchers.is(signedComparator.compare(val1, val2)));

    // Compare differently
    assertThat(
            unsignedComparator.compare(val110, val150),
            Matchers.not(signedComparator.compare(val110, val150)));

    // Compare differently
    assertThat(
            unsignedComparator.compare(val111, val150),
            Matchers.not(signedComparator.compare(val111, val150)));

    // This will fail if the db is using a signed comparator for the start/stop keys
    for (final Dbi<ByteBuffer> db : dbs) {
      db.put(val110, val110);
      db.put(val150, val150);

      final ByteBuffer startKeyBuf = val111;
      KeyRange<ByteBuffer> keyRange = KeyRange.atLeastBackward(startKeyBuf);

      try (Txn<ByteBuffer> txn = env.txnRead();
           CursorIterable<ByteBuffer> c = db.iterate(txn, keyRange)) {
        for (final CursorIterable.KeyVal<ByteBuffer> kv : c) {
          final int key = kv.key().getInt();
          final int val = kv.val().getInt();
//          System.out.println("key: " + key + " val: " + val);
          assertThat(key, is(110));
          break;
        }
      }
    }
  }

  private void verify(final KeyRange<ByteBuffer> range, final int... expected) {
    // Verify using all comparator types
    for (final Dbi<ByteBuffer> db : dbs) {
      verify(range, db, expected);
    }
  }

  private void verify(
      final Dbi<ByteBuffer> dbi, final KeyRange<ByteBuffer> range, final int... expected) {
    verify(range, dbi, expected);
  }

  private void verify(
      final KeyRange<ByteBuffer> range, final Dbi<ByteBuffer> dbi, final int... expected) {

    final List<Integer> results = new ArrayList<>();

    try (Txn<ByteBuffer> txn = env.txnRead();
        CursorIterable<ByteBuffer> c = dbi.iterate(txn, range)) {
      for (final KeyVal<ByteBuffer> kv : c) {
        final int key = kv.key().getInt();
        final int val = kv.val().getInt();
        results.add(key);
        assertThat(val, is(key + 1));
      }
    }

    assertThat(results, hasSize(expected.length));
    for (int idx = 0; idx < results.size(); idx++) {
      assertThat(results.get(idx), is(expected[idx]));
    }
  }
}
