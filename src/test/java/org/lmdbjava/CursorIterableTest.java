/*-
 * #%L
 * LmdbJava
 * %%
 * Copyright (C) 2016 - 2023 The LmdbJava Open Source Project
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

package org.lmdbjava;

import com.google.common.primitives.UnsignedBytes;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.lmdbjava.CursorIterable.KeyVal;

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
import static org.lmdbjava.TestUtils.POSIX_MODE;
import static org.lmdbjava.TestUtils.bb;

/**
 * Test {@link CursorIterable}.
 */
public final class CursorIterableTest {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();
  private Dbi<ByteBuffer> db;
  private Env<ByteBuffer> env;
  private Deque<Integer> list;

  @After
  public void after() {
    env.close();
  }

  @Test
  public void allBackwardTest() {
    verify(allBackward(), 800, 600, 400, 200);
  }

  @Test
  public void allTest() {
    verify(all(), 200, 400, 600, 800);
  }

  @Test
  public void atLeastBackwardTest() {
    verify(atLeastBackward(bb(500)), 400, 200);
    verify(atLeastBackward(bb(600)), 600, 400, 200);
    verify(atLeastBackward(bb(900)), 800, 600, 400, 200);
  }

  @Test
  public void atLeastTest() {
    verify(atLeast(bb(500)), 600, 800);
    verify(atLeast(bb(600)), 600, 800);
  }

  @Test
  public void atMostBackwardTest() {
    verify(atMostBackward(bb(500)), 800, 600);
    verify(atMostBackward(bb(600)), 800, 600);
  }

  @Test
  public void atMostTest() {
    verify(atMost(bb(500)), 200, 400);
    verify(atMost(bb(600)), 200, 400, 600);
  }

  @Before
  public void before() throws IOException {
    final File path = tmp.newFile();
    env = create()
        .setMapSize(KIBIBYTES.toBytes(256))
        .setMaxReaders(1)
        .setMaxDbs(1)
        .open(path, POSIX_MODE, MDB_NOSUBDIR);
    db = env.openDbi(DB_1, MDB_CREATE);
    populateDatabase(db);
  }

  private void populateDatabase(final Dbi<ByteBuffer> dbi) {
    list = new LinkedList<>();
    list.addAll(asList(200, 300, 400, 500, 600, 700, 800, 900));
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      final Cursor<ByteBuffer> c = dbi.openCursor(txn);
      c.put(bb(200), bb(300), MDB_NOOVERWRITE);
      c.put(bb(400), bb(500));
      c.put(bb(600), bb(700));
      c.put(bb(800), bb(900));
      txn.commit();
    }
  }

  @Test
  public void closedBackwardTest() {
    verify(closedBackward(bb(700), bb(300)), 600, 400);
    verify(closedBackward(bb(600), bb(200)), 600, 400, 200);
    verify(closedBackward(bb(900), bb(300)), 800, 600, 400);
  }

  @Test
  public void closedOpenBackwardTest() {
    verify(closedOpenBackward(bb(800), bb(300)), 800, 600, 400);
    verify(closedOpenBackward(bb(700), bb(200)), 600, 400);
    verify(closedOpenBackward(bb(900), bb(300)), 800, 600, 400);
  }

  @Test
  public void closedOpenTest() {
    verify(closedOpen(bb(300), bb(800)), 400, 600);
    verify(closedOpen(bb(200), bb(600)), 200, 400);
  }

  @Test
  public void closedTest() {
    verify(closed(bb(300), bb(700)), 400, 600);
    verify(closed(bb(200), bb(600)), 200, 400, 600);
    verify(closed(bb(1), bb(700)), 200, 400, 600);
  }

  @Test
  public void greaterThanBackwardTest() {
    verify(greaterThanBackward(bb(600)), 400, 200);
    verify(greaterThanBackward(bb(700)), 600, 400, 200);
    verify(greaterThanBackward(bb(900)), 800, 600, 400, 200);
  }

  @Test
  public void greaterThanTest() {
    verify(greaterThan(bb(400)), 600, 800);
    verify(greaterThan(bb(300)), 400, 600, 800);
  }

  @Test(expected = IllegalStateException.class)
  public void iterableOnlyReturnedOnce() {
    try (Txn<ByteBuffer> txn = env.txnRead();
         CursorIterable<ByteBuffer> c = db.iterate(txn)) {
      c.iterator(); // ok
      c.iterator(); // fails
    }
  }

  @Test
  public void iterate() {
    try (Txn<ByteBuffer> txn = env.txnRead();
         CursorIterable<ByteBuffer> c = db.iterate(txn)) {
      for (final KeyVal<ByteBuffer> kv : c) {
        assertThat(kv.key().getInt(), is(list.pollFirst()));
        assertThat(kv.val().getInt(), is(list.pollFirst()));
      }
    }
  }

  @Test(expected = IllegalStateException.class)
  public void iteratorOnlyReturnedOnce() {
    try (Txn<ByteBuffer> txn = env.txnRead();
         CursorIterable<ByteBuffer> c = db.iterate(txn)) {
      c.iterator(); // ok
      c.iterator(); // fails
    }
  }

  @Test
  public void lessThanBackwardTest() {
    verify(lessThanBackward(bb(500)), 800, 600);
    verify(lessThanBackward(bb(200)), 800, 600, 400);
  }

  @Test
  public void lessThanTest() {
    verify(lessThan(bb(500)), 200, 400);
    verify(lessThan(bb(800)), 200, 400, 600);
  }

  @Test(expected = NoSuchElementException.class)
  public void nextThrowsNoSuchElementExceptionIfNoMoreElements() {
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

  @Test
  public void openBackwardTest() {
    verify(openBackward(bb(700), bb(200)), 600, 400);
    verify(openBackward(bb(800), bb(1)), 600, 400, 200);
    verify(openBackward(bb(900), bb(400)), 800, 600);
  }

  @Test
  public void openClosedBackwardTest() {
    verify(openClosedBackward(bb(700), bb(200)), 600, 400, 200);
    verify(openClosedBackward(bb(800), bb(400)), 600, 400);
    verify(openClosedBackward(bb(900), bb(400)), 800, 600, 400);
  }

  @Test
  public void openClosedBackwardTestWithGuava() {
    final Comparator<byte[]> guava = UnsignedBytes.lexicographicalComparator();
    final Comparator<ByteBuffer> comparator = (bb1, bb2) -> {
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
    verify(openClosedBackward(bb(700), bb(200)), guavaDbi, 600, 400, 200);
    verify(openClosedBackward(bb(800), bb(400)), guavaDbi, 600, 400);
  }

  @Test
  public void openClosedTest() {
    verify(openClosed(bb(300), bb(800)), 400, 600, 800);
    verify(openClosed(bb(200), bb(600)), 400, 600);
  }

  @Test
  public void openTest() {
    verify(open(bb(300), bb(700)), 400, 600);
    verify(open(bb(200), bb(800)), 400, 600);
  }

  @Test
  public void removeOddElements() {
    verify(all(), 200, 400, 600, 800);
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
    verify(all(), 400, 800);
  }

  @Test(expected = Env.AlreadyClosedException.class)
  public void nextWithClosedEnvTest() {
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
    try (Txn<ByteBuffer> txn = env.txnRead()) {
      try (CursorIterable<ByteBuffer> ci = db.iterate(txn, KeyRange.all())) {
        final Iterator<KeyVal<ByteBuffer>> c = ci.iterator();

        env.close();
        c.forEachRemaining(keyVal -> {

        });
      }
    }
  }

  private void verify(final KeyRange<ByteBuffer> range,
                      final int... expected) {
    verify(range, db, expected);
  }

  private void verify(final KeyRange<ByteBuffer> range,
                      final Dbi<ByteBuffer> dbi, final int... expected) {
    final List<Integer> results = new ArrayList<>();

    try (Txn<ByteBuffer> txn = env.txnRead();
         CursorIterable<ByteBuffer> c = dbi.iterate(txn, range)) {
      for (final KeyVal<ByteBuffer> kv : c) {
        final int key = kv.key().getInt();
        final int val = kv.val().getInt();
        results.add(key);
        assertThat(val, is(key + 100));
      }
    }

    assertThat(results, hasSize(expected.length));
    for (int idx = 0; idx < results.size(); idx++) {
      assertThat(results.get(idx), is(expected[idx]));
    }
  }

}
