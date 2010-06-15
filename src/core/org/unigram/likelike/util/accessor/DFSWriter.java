package org.unigram.likelike.util.accessor;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class DFSWriter implements IWriter {

    @SuppressWarnings("unchecked")
    @Override
    public boolean write(Long key, Long value, Context context) 
    throws Exception, InterruptedException, IOException {
        context.write(new LongWritable(key), new LongWritable(value));
        return true;
    }
    
    public DFSWriter(Configuration conf) {
            // nothing to do
    }

}
