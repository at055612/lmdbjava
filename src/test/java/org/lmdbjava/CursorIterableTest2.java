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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import static com.jakewharton.byteunits.BinaryByteUnit.KIBIBYTES;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.lmdbjava.ByteBufferProxy.PROXY_OPTIMAL;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.TestUtils.DB_1;
import static org.lmdbjava.TestUtils.POSIX_MODE;
import static org.lmdbjava.TestUtils.bb;

/**
 * Test {@link Cursor}.
 */
public final class CursorIterableTest2 {

  @Rule
  public final TemporaryFolder tmp = new TemporaryFolder();

  private Env<ByteBuffer> env;

  @After
  public void after() {
    env.close();
  }

  @Before
  public void before() throws IOException {
    try {
      final File path = tmp.newFile();
      env = create(PROXY_OPTIMAL)
          .setMapSize(KIBIBYTES.toBytes(1_024))
          .setMaxReaders(1)
          .setMaxDbs(1)
          .open(path, POSIX_MODE, MDB_NOSUBDIR);
    } catch (final IOException e) {
      throw new LmdbException("IO failure", e);
    }
  }

  @Test
  public void testAtLeastBackward1() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    db.put(bb(110), bb(110));
    db.put(bb(150), bb(150));

    final ByteBuffer startKeyBuf = bb(111);
    KeyRange<ByteBuffer> keyRange = KeyRange.atLeastBackward(startKeyBuf);

    try (Txn<ByteBuffer> txn = env.txnRead();
         CursorIterable<ByteBuffer> c = db.iterate(txn, keyRange)) {
      for (final CursorIterable.KeyVal<ByteBuffer> kv : c) {
        final int key = kv.key().getInt();
        final int val = kv.val().getInt();
        System.out.println("key: " + key + " val: " + val);
        assertThat(key, is(110));
        break;
      }
    }
  }

  @Test
  public void testAtLeastBackward2() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    db.put(bb(110), bb(110));
    db.put(bb(150), bb(150));

    final ByteBuffer startKeyBuf = bb(150);
    KeyRange<ByteBuffer> keyRange = KeyRange.atLeastBackward(startKeyBuf);

    try (Txn<ByteBuffer> txn = env.txnRead();
         CursorIterable<ByteBuffer> c = db.iterate(txn, keyRange)) {
      for (final CursorIterable.KeyVal<ByteBuffer> kv : c) {
        final int key = kv.key().getInt();
        final int val = kv.val().getInt();
        System.out.println("key: " + key + " val: " + val);
        assertThat(key, is(150));
        break;
      }
    }
  }

  @Test
  public void testAtLeastBackward3() {
    final Dbi<ByteBuffer> db = env.openDbi(DB_1, MDB_CREATE);
    db.put(bb(110), bb(110));
    db.put(bb(150), bb(150));

    final ByteBuffer startKeyBuf = bb(151);
    KeyRange<ByteBuffer> keyRange = KeyRange.atLeastBackward(startKeyBuf);

    try (Txn<ByteBuffer> txn = env.txnRead();
         CursorIterable<ByteBuffer> c = db.iterate(txn, keyRange)) {
      for (final CursorIterable.KeyVal<ByteBuffer> kv : c) {
        final int key = kv.key().getInt();
        final int val = kv.val().getInt();
        System.out.println("key: " + key + " val: " + val);
        assertThat(key, is(150));
        break;
      }
    }
  }
}
