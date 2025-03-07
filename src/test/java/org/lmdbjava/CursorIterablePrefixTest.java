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
import static java.nio.ByteBuffer.allocateDirect;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.EnvFlags.MDB_NOSUBDIR;
import static org.lmdbjava.PutFlags.MDB_NOOVERWRITE;
import static org.lmdbjava.TestUtils.DB_2;
import static org.lmdbjava.TestUtils.POSIX_MODE;
import static org.lmdbjava.TestUtils.bb;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.lmdbjava.CursorIterable.KeyVal;

/** Test {@link CursorIterable}. */
public final class CursorIterablePrefixTest {

  @Rule public final TemporaryFolder tmp = new TemporaryFolder();
  private Dbi<ByteBuffer> db;
  private List<Dbi<ByteBuffer>> dbs = new ArrayList<>();
  private Env<ByteBuffer> env;

  @After
  public void after() {
    env.close();
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

    db = env.openDbi(DB_2, MDB_CREATE);
    populateDatabase(db);
    dbs.add(db);
  }

  private void populateDatabase(final Dbi<ByteBuffer> dbi) {
    try (Txn<ByteBuffer> txn = env.txnWrite()) {
      final Cursor<ByteBuffer> c = dbi.openCursor(txn);
      int row = 1;
      for (int part1 = 200; part1 <= 800; part1 += 200) {
        final int startInc = part1 + 2;
        final int endInc = part1 + 8;

        // Put a key that is shorter than the prefix
        c.put(bbShort((short) (part1 / 10)), bb(row++), MDB_NOOVERWRITE);

        // Put a key that is prefix only
        c.put(compoundKey(part1, null), bb(row++), MDB_NOOVERWRITE);

        for (int part2 = startInc; part2 <= endInc; part2 += 2) {
          c.put(compoundKey(part1, part2), bb(row++), MDB_NOOVERWRITE);
        }
      }
      txn.commit();
    }
    //    dumpDbContents();
  }

  private void dumpDbContents() {
    try (Txn<ByteBuffer> txn = env.txnRead();
        CursorIterable<ByteBuffer> c = db.iterate(txn, KeyRange.all())) {
      for (final KeyVal<ByteBuffer> kv : c) {
        final String keyStr;
        if (kv.key().limit() == 2) {
          final short aShort = kv.key().getShort();
          keyStr = Short.toString(aShort);
        } else {
          final CompoundKey key = CompoundKey.of(kv.key());
          keyStr = key.toString();
        }
        final int val = kv.val().getInt();
        System.out.println(keyStr + " - " + val);
      }
    }
  }

  @Test
  public void testPrefix1() {
    verify(
        KeyRange.keyPrefix(bb(200)),
        CompoundKey.of(200, null),
        CompoundKey.of(200, 202),
        CompoundKey.of(200, 204),
        CompoundKey.of(200, 206),
        CompoundKey.of(200, 208));
  }

  @Test
  public void testPrefix2() {
    verify(
        KeyRange.keyPrefix(bb(400)),
        CompoundKey.of(400, null),
        CompoundKey.of(400, 402),
        CompoundKey.of(400, 404),
        CompoundKey.of(400, 406),
        CompoundKey.of(400, 408));
  }

  @Test
  public void testPrefix3() {
    // no prefix match, start key is before all data
    verify(KeyRange.keyPrefix(bb(100)));
  }

  @Test
  public void testPrefix4() {
    // no prefix match, start key is after all data
    verify(KeyRange.keyPrefix(bb(900)));
  }

  @Test
  public void testPrefix5() {
    // no prefix match, in the middle of the data
    verify(KeyRange.keyPrefix(bb(250)));
  }

  @Test
  public void testAtLeast() {
    verify(
        KeyRange.lessThan(compoundKey(400, 2)),
        CompoundKey.of(200, null),
        CompoundKey.of(200, 202),
        CompoundKey.of(200, 204),
        CompoundKey.of(200, 206),
        CompoundKey.of(200, 208),
        CompoundKey.of(400, null));
  }

  private void verify(final KeyRange<ByteBuffer> range, final CompoundKey... expected) {

    final List<CompoundKey> keys = new ArrayList<>();
    final List<Integer> values = new ArrayList<>();

    try (Txn<ByteBuffer> txn = env.txnRead();
        CursorIterable<ByteBuffer> c = db.iterate(txn, range)) {
      for (final KeyVal<ByteBuffer> kv : c) {
        try {
          final CompoundKey key = CompoundKey.of(kv.key());
          final int val = kv.val().getInt();
          System.out.println("Verify found: " + key + " - " + val);
          keys.add(key);
          values.add(val);
        } catch (Exception e) {
          throw new RuntimeException(
              "Found a key that is not a compoundKey, limit: " + kv.key().limit());
        }
      }
    }

    assertThat(keys, hasSize(expected.length));
    for (int idx = 0; idx < keys.size(); idx++) {
      assertThat(keys.get(idx), is(expected[idx]));
    }
  }

  static ByteBuffer compoundKey(final int part1, final Integer part2) {
    final ByteBuffer bb = allocateDirect(Integer.BYTES * 2);
    bb.putInt(part1);
    if (part2 != null) {
      bb.putInt(part2);
    }
    bb.flip();
    return bb;
  }

  static ByteBuffer bbShort(final short val) {
    final ByteBuffer bb = allocateDirect(Short.BYTES);
    bb.putShort(val);
    bb.flip();
    return bb;
  }

  // --------------------------------------------------------------------------------

  private static class CompoundKey {
    private final int part1;
    private final Integer part2;

    private CompoundKey(int part1, Integer part2) {
      this.part1 = part1;
      this.part2 = part2;
    }

    private static CompoundKey of(int part1, Integer part2) {
      return new CompoundKey(part1, part2);
    }

    private static CompoundKey of(final ByteBuffer bb) {
      final int part1 = bb.getInt();
      Integer part2 = null;
      if (bb.hasRemaining()) {
        part2 = bb.getInt();
      }
      final CompoundKey compoundKey = new CompoundKey(part1, part2);
      bb.rewind();
      return compoundKey;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      CompoundKey that = (CompoundKey) o;
      return part1 == that.part1 && Objects.equals(part2, that.part2);
    }

    @Override
    public int hashCode() {
      return Objects.hash(part1, part2);
    }

    @Override
    public String toString() {
      return part1 + "|" + part2;
    }
  }
}
