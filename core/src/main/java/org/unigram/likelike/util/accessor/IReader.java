package org.unigram.likelike.util.accessor;

import java.util.Map;

import org.apache.hadoop.mapreduce.Reducer.Context;

public interface IReader {

	Map<String, byte[]> read(Long key) 
	throws Exception, InterruptedException;	
	
}
