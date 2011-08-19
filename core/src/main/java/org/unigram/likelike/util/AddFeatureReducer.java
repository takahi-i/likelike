/**
 * Copyright 2009 Takahiko Ito
 * 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0 
 *        
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 */
package org.unigram.likelike.util;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 *
 */
public class AddFeatureReducer extends
        Reducer<LongWritable, Text, LongWritable, Text> {
    
    /**
     * Reduce method.
     * @param key -
     * @param values -
     * @param context -
     * @throws IOException -
     * @throws InterruptedException -
     */
    @Override
    public void reduce(final LongWritable key,
            final Iterable<Text> values,
            final Context context)
            throws IOException, InterruptedException {

        Text rtValue = null;
        List<Long> candidates = new LinkedList<Long>();
        for (Text v : values) {
            if (v.find(" ") >= 0) { // feature
                rtValue = new Text(key+"\t"+v);
                continue;
            }
            candidates.add(Long.parseLong(v.toString())); // example
        }

        /* output recommendations with target features */
        if (rtValue==null) {
            return;
        }
        
        for (Long v : candidates) {
            /* write with inverse key and value */ 
            /* caution: inverse key (target) and value (candidate) */
            context.write(new LongWritable(v), rtValue);  
        }
    }
}
