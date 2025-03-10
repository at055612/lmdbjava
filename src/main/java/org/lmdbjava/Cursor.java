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
import static org.lmdbjava.Dbi.KeyExistsException.MDB_KEYEXIST;
import static org.lmdbjava.Dbi.KeyNotFoundException.MDB_NOTFOUND;
import static org.lmdbjava.Env.SHOULD_CHECK;
import static org.lmdbjava.Library.LIB;
import static org.lmdbjava.MaskedFlag.isSet;
import static org.lmdbjava.MaskedFlag.mask;
import static org.lmdbjava.PutFlags.MDB_MULTIPLE;
import static org.lmdbjava.PutFlags.MDB_NODUPDATA;
import static org.lmdbjava.PutFlags.MDB_NOOVERWRITE;
import static org.lmdbjava.PutFlags.MDB_RESERVE;
import static org.lmdbjava.ResultCodeMapper.checkRc;
import static org.lmdbjava.SeekOp.MDB_FIRST;
import static org.lmdbjava.SeekOp.MDB_LAST;
import static org.lmdbjava.SeekOp.MDB_NEXT;
import static org.lmdbjava.SeekOp.MDB_PREV;

import jnr.ffi.Pointer;
import jnr.ffi.byref.NativeLongByReference;

/**
 * A cursor handle.
 *
 * @param <T> buffer type
 */
public final class Cursor<T> implements AutoCloseable {

  private boolean closed;
  private final KeyVal<T> kv;
  private final Pointer ptrCursor;
  private Txn<T> txn;
  private final Env<T> env;

  Cursor(final Pointer ptr, final Txn<T> txn, final Env<T> env) {
    requireNonNull(ptr);
    requireNonNull(txn);
    this.ptrCursor = ptr;
    this.txn = txn;
    this.kv = txn.newKeyVal();
    this.env = env;
  }

  /**
   * Close a cursor handle.
   *
   * <p>The cursor handle will be freed and must not be used again after this call. Its transaction
   * must still be live if it is a write-transaction.
   */
  @Override
  public void close() {
    if (closed) {
      return;
    }
    kv.close();
    if (SHOULD_CHECK) {
      env.checkNotClosed();
      if (!txn.isReadOnly()) {
        txn.checkReady();
      }
    }
    LIB.mdb_cursor_close(ptrCursor);
    closed = true;
  }

  /**
   * Return count of duplicates for current key.
   *
   * <p>This call is only valid on databases that support sorted duplicate data items {@link
   * DbiFlags#MDB_DUPSORT}.
   *
   * @return count of duplicates for current key
   */
  public long count() {
    if (SHOULD_CHECK) {
      env.checkNotClosed();
      checkNotClosed();
      txn.checkReady();
    }
    final NativeLongByReference longByReference = new NativeLongByReference();
    checkRc(LIB.mdb_cursor_count(ptrCursor, longByReference));
    return longByReference.longValue();
  }

  /**
   * Delete current key/data pair.
   *
   * <p>This function deletes the key/data pair to which the cursor refers.
   *
   * @param f flags (either null or {@link PutFlags#MDB_NODUPDATA}
   */
  public void delete(final PutFlags... f) {
    if (SHOULD_CHECK) {
      env.checkNotClosed();
      checkNotClosed();
      txn.checkReady();
      txn.checkWritesAllowed();
    }
    final int flags = mask(true, f);
    checkRc(LIB.mdb_cursor_del(ptrCursor, flags));
  }

  /**
   * Position at first key/data item.
   *
   * @return false if requested position not found
   */
  public boolean first() {
    return seek(MDB_FIRST);
  }

  /**
   * Reposition the key/value buffers based on the passed key and operation.
   *
   * @param key to search for
   * @param data to search for
   * @param op options for this operation
   * @return false if key not found
   */
  public boolean get(final T key, final T data, final SeekOp op) {
    if (SHOULD_CHECK) {
      requireNonNull(key);
      requireNonNull(op);
      env.checkNotClosed();
      checkNotClosed();
      txn.checkReady();
    }
    kv.keyIn(key);
    kv.valIn(data);

    final int rc = LIB.mdb_cursor_get(ptrCursor, kv.pointerKey(), kv.pointerVal(), op.getCode());

    if (rc == MDB_NOTFOUND) {
      return false;
    }

    checkRc(rc);
    kv.keyOut();
    kv.valOut();
    ReferenceUtil.reachabilityFence0(key);
    return true;
  }

  /**
   * Reposition the key/value buffers based on the passed key and operation.
   *
   * @param key to search for
   * @param op options for this operation
   * @return false if key not found
   */
  public boolean get(final T key, final GetOp op) {
    if (SHOULD_CHECK) {
      requireNonNull(key);
      requireNonNull(op);
      env.checkNotClosed();
      checkNotClosed();
      txn.checkReady();
    }
    kv.keyIn(key);

    final int rc = LIB.mdb_cursor_get(ptrCursor, kv.pointerKey(), kv.pointerVal(), op.getCode());

    if (rc == MDB_NOTFOUND) {
      return false;
    }

    checkRc(rc);
    kv.keyOut();
    kv.valOut();
    ReferenceUtil.reachabilityFence0(key);
    return true;
  }

  /**
   * Obtain the key.
   *
   * @return the key that the cursor is located at.
   */
  public T key() {
    return kv.key();
  }

  /**
   * Position at last key/data item.
   *
   * @return false if requested position not found
   */
  public boolean last() {
    return seek(MDB_LAST);
  }

  /**
   * Position at next data item.
   *
   * @return false if requested position not found
   */
  public boolean next() {
    return seek(MDB_NEXT);
  }

  /**
   * Position at previous data item.
   *
   * @return false if requested position not found
   */
  public boolean prev() {
    return seek(MDB_PREV);
  }

  /**
   * Store by cursor.
   *
   * <p>This function stores key/data pairs into the database.
   *
   * @param key key to store
   * @param val data to store
   * @param op options for this operation
   * @return true if the value was put, false if MDB_NOOVERWRITE or MDB_NODUPDATA were set and the
   *     key/value existed already.
   */
  public boolean put(final T key, final T val, final PutFlags... op) {
    if (SHOULD_CHECK) {
      requireNonNull(key);
      requireNonNull(val);
      env.checkNotClosed();
      checkNotClosed();
      txn.checkReady();
      txn.checkWritesAllowed();
    }
    kv.keyIn(key);
    kv.valIn(val);
    final int mask = mask(true, op);
    final int rc = LIB.mdb_cursor_put(ptrCursor, kv.pointerKey(), kv.pointerVal(), mask);
    if (rc == MDB_KEYEXIST) {
      if (isSet(mask, MDB_NOOVERWRITE)) {
        kv.valOut(); // marked as in,out in LMDB C docs
      } else if (!isSet(mask, MDB_NODUPDATA)) {
        checkRc(rc);
      }
      return false;
    }
    checkRc(rc);
    ReferenceUtil.reachabilityFence0(key);
    ReferenceUtil.reachabilityFence0(val);
    return true;
  }

  /**
   * Put multiple values into the database in one <code>MDB_MULTIPLE</code> operation.
   *
   * <p>The database must have been opened with {@link DbiFlags#MDB_DUPFIXED}. The buffer must
   * contain fixed-sized values to be inserted. The size of each element is calculated from the
   * buffer's size divided by the given element count. For example, to populate 10 X 4 byte integers
   * at once, present a buffer of 40 bytes and specify the element as 10.
   *
   * @param key key to store in the database (not null)
   * @param val value to store in the database (not null)
   * @param elements number of elements contained in the passed value buffer
   * @param op options for operation (must set <code>MDB_MULTIPLE</code>)
   */
  public void putMultiple(final T key, final T val, final int elements, final PutFlags... op) {
    if (SHOULD_CHECK) {
      requireNonNull(txn);
      requireNonNull(key);
      requireNonNull(val);
      env.checkNotClosed();
      txn.checkReady();
      txn.checkWritesAllowed();
    }
    final int mask = mask(true, op);
    if (SHOULD_CHECK && !isSet(mask, MDB_MULTIPLE)) {
      throw new IllegalArgumentException("Must set " + MDB_MULTIPLE + " flag");
    }
    txn.kv().keyIn(key);
    final Pointer dataPtr = txn.kv().valInMulti(val, elements);
    final int rc = LIB.mdb_cursor_put(ptrCursor, txn.kv().pointerKey(), dataPtr, mask);
    checkRc(rc);
    ReferenceUtil.reachabilityFence0(key);
    ReferenceUtil.reachabilityFence0(val);
  }

  /**
   * Renew a cursor handle.
   *
   * <p>A cursor is associated with a specific transaction and database. Cursors that are only used
   * in read-only transactions may be re-used, to avoid unnecessary malloc/free overhead. The cursor
   * may be associated with a new read-only transaction, and referencing the same database handle as
   * it was created with. This may be done whether the previous transaction is live or dead.
   *
   * @param newTxn transaction handle
   */
  public void renew(final Txn<T> newTxn) {
    if (SHOULD_CHECK) {
      requireNonNull(newTxn);
      env.checkNotClosed();
      checkNotClosed();
      this.txn.checkReadOnly(); // existing
      newTxn.checkReadOnly();
      newTxn.checkReady();
    }
    checkRc(LIB.mdb_cursor_renew(newTxn.pointer(), ptrCursor));
    this.txn = newTxn;
  }

  /**
   * Reserve space for data of the given size, but don't copy the given val. Instead, return a
   * pointer to the reserved space, which the caller can fill in later - before the next update
   * operation or the transaction ends. This saves an extra memcpy if the data is being generated
   * later. LMDB does nothing else with this memory, the caller is expected to modify all of the
   * space requested.
   *
   * <p>This flag must not be specified if the database was opened with MDB_DUPSORT
   *
   * @param key key to store in the database (not null)
   * @param size size of the value to be stored in the database (not null)
   * @param op options for this operation
   * @return a buffer that can be used to modify the value
   */
  public T reserve(final T key, final int size, final PutFlags... op) {
    if (SHOULD_CHECK) {
      requireNonNull(key);
      env.checkNotClosed();
      checkNotClosed();
      txn.checkReady();
      txn.checkWritesAllowed();
    }
    kv.keyIn(key);
    kv.valIn(size);
    final int flags = mask(true, op) | MDB_RESERVE.getMask();
    checkRc(LIB.mdb_cursor_put(ptrCursor, kv.pointerKey(), kv.pointerVal(), flags));
    kv.valOut();
    ReferenceUtil.reachabilityFence0(key);
    return val();
  }

  /**
   * Reposition the key/value buffers based on the passed operation.
   *
   * @param op options for this operation
   * @return false if requested position not found
   */
  public boolean seek(final SeekOp op) {
    if (SHOULD_CHECK) {
      requireNonNull(op);
      env.checkNotClosed();
      checkNotClosed();
      txn.checkReady();
    }

    final int rc = LIB.mdb_cursor_get(ptrCursor, kv.pointerKey(), kv.pointerVal(), op.getCode());

    if (rc == MDB_NOTFOUND) {
      return false;
    }

    checkRc(rc);
    kv.keyOut();
    kv.valOut();
    return true;
  }

  /**
   * Obtain the value.
   *
   * @return the value that the cursor is located at.
   */
  public T val() {
    return kv.val();
  }

  private void checkNotClosed() {
    if (closed) {
      throw new ClosedException();
    }
  }

  /** Cursor has already been closed. */
  public static final class ClosedException extends LmdbException {

    private static final long serialVersionUID = 1L;

    /** Creates a new instance. */
    public ClosedException() {
      super("Cursor has already been closed");
    }
  }

  /** Cursor stack too deep - internal error. */
  public static final class FullException extends LmdbNativeException {

    static final int MDB_CURSOR_FULL = -30_787;
    private static final long serialVersionUID = 1L;

    FullException() {
      super(MDB_CURSOR_FULL, "Cursor stack too deep - internal error");
    }
  }
}
