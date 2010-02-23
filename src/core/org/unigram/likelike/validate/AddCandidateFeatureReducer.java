package org.unigram.likelike.validate;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class AddCandidateFeatureReducer extends
        Reducer<LongWritable, Text, LongWritable, Text> {
    
    @Override
    public void reduce(final LongWritable key,
            final Iterable<Text> values,
            final Context context)
            throws IOException, InterruptedException {

        Text rtValue = null;
        List<Long> candidates = new LinkedList<Long>();
        for (Text v : values) {
            if (v.find(":") >= 0) { // feature                                                                                                               
                rtValue = new Text(key+"\t"+v);
                continue;
            }
            candidates.add(Long.parseLong(v.toString()));
        }

        /* output recommendations with target features */
        for (Long v : candidates) {
            context.write(new LongWritable(v), rtValue);
        }
    }
}
