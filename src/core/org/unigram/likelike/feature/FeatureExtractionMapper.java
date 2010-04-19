package org.unigram.likelike.feature;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 *
 */
public class FeatureExtractionMapper extends
        Mapper<LongWritable, Text, LongWritable, Text> {
    
    /**
     * map.
     * 
     * @param key key
     * @param value value
     * @param context -
     * @throws IOException -
     * @throws InterruptedException -
     */
    @Override
    public final void map(final LongWritable key,
            final Text value, final Context context) 
        throws IOException, InterruptedException {    
        String valueStr = value.toString();
        String[] valueArray = valueStr.split("\t");
        /* when target, related, related-features */
        if (valueArray.length == 3) { 
            context.write(
                    new LongWritable(
                            Long.parseLong(valueArray[0])), 
                    new Text(valueArray[2]));
        } else {
            System.out.println("Input shoud have three segments: " 
                    + valueStr);
        }
    }
}
