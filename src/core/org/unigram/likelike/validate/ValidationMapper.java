package org.unigram.likelike.validate;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ValidationMapper extends Mapper
    <LongWritable, Text, LongWritable, Text> {
    
    @Override
    public final void map(final LongWritable key,
            final Text value, final Context context) 
        throws IOException, InterruptedException {    
        String valueStr = value.toString();
        String[] valueArray = valueStr.split("\t");
        
        if (valueArray.length == 2) { // feature information 
            context.write(new LongWritable(
                    Long.parseLong(valueArray[0])), 
                    new Text(valueArray[1]));
        } else if (valueArray.length == 3) { // recommend pair
            context.write(
                    new LongWritable(
                            Long.parseLong(valueArray[0])), 
                    new Text(valueArray[1] + "\t" + valueArray[2]));
        }
    }
}
