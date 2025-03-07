package org.lmdbjava;

public interface PrefixMatcher<T> {

  boolean prefixMatches(T buffer, T prefixBuffer);
}
