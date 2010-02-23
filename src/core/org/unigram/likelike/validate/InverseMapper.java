package org.unigram.likelike.validate;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class InverseMapper extends Mapper<LongWritable, 
    Text, LongWritable, Text> {

    @Override
    public final void map(final LongWritable dummy,
            final Text value, final Context context) 
    throws InterruptedException, IOException {
        String valueStr = value.toString();        
        String[] valueArray = valueStr.split("\t");
        
        //System.out.println("key: "+ key+"\tvalueStr: " + valueStr);
        
        if (valueArray.length == 2) {
            context.write(new LongWritable(Long.parseLong(valueArray[1]))
            , new Text(valueArray[0]));
        } else {
            System.out.println("invalid input:" + value);
        }
    }
}
