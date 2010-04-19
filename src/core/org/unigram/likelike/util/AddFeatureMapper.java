package org.unigram.likelike.util;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 */
public class AddFeatureMapper extends
        Mapper<LongWritable, Text, LongWritable, Text> {
  
    /**
     * Map method.
     * 
     * @param key -
     * @param value -
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
        
        if (valueArray.length == 2) {
            context.write(new LongWritable(
                        Long.parseLong(valueArray[0])),
                        new Text(valueArray[1]));
        } else {
            System.out.println(
                    "Input hould have two segments: " + valueStr);
        }
    }
}
