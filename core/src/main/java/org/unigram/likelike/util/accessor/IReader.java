package org.unigram.likelike.util.accessor;

import java.util.Map;

public interface IReader {
    Map<String, byte[]> read(Long key) throws Exception, InterruptedException;
}
