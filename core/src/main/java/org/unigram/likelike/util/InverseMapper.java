package org.unigram.likelike.util;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 *
 */
public class InverseMapper extends Mapper<LongWritable, 
    Text, LongWritable, Text> {
    
    /**
     * Map method.
     * @param dummy -
     * @param value -
     * @param context -
     * @throws IOException -
     * @throws InterruptedException -
     */
    @Override
    public final void map(final LongWritable dummy,
            final Text value, final Context context) 
    throws InterruptedException, IOException {
        String valueStr = value.toString();        
        String[] valueArray = valueStr.split("\t");
        
        if (valueArray.length == 2) {
            context.write(
                    new LongWritable(Long.parseLong(valueArray[1])), 
                    new Text(valueArray[0]));
        } else {
            System.out.println("invalid input:" + value);
        }
    }
}
