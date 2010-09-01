package org.unigram.likelike.feature;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

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
        if (valueArray.length == 3) {  // related example and features
            // TODO fix (inefficient!)
            context.write(
                    new LongWritable(
                            Long.parseLong(valueArray[0])), 
                    new Text(valueArray[1] + "\t" + valueArray[2])); 
        } else if (valueArray.length == 2) { // target features
            context.write(
                    new LongWritable(
                            Long.parseLong(valueArray[0])), 
                    new Text(valueArray[1]));
        } else {
            System.out.println("Input shoud have three segments: " 
                    + valueStr);
        }
    }
}
