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
package org.unigram.likelike.lsh;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

import org.unigram.likelike.common.LikelikeConstants;

public class SelectClustersReducer extends
        Reducer<LongWritable, LongWritable,
        LongWritable, Text> {
    
    @Override
    public void reduce(final LongWritable key,
            final Iterable<LongWritable> values,
            final Context context)
            throws IOException, InterruptedException {
        
        StringBuffer idsStr = new StringBuffer();
        long clusterSize = 0;
        for (LongWritable id : values) {
            idsStr.append(id.toString() + ":");
            clusterSize += 1;
            if (clusterSize >= this.maximumClusterSize) {
                break;
            }
        }
        
        if (this.minimumClusterSize <= clusterSize) {
            context.write(key, new Text(idsStr.toString()));            
        }
    }
    
    @Override
    public final void setup(final Context context) {
        Configuration jc = context.getConfiguration();
        if (context == null || jc == null) {
            jc = new Configuration();
        }
        this.maximumClusterSize = jc.getLong(
                LikelikeConstants.MAX_CLUSTER_SIZE , 
                LikelikeConstants.DEFAULT_MAX_CLUSTER_SIZE);
        this.minimumClusterSize = jc.getLong(
                LikelikeConstants.MIN_CLUSTER_SIZE , 
                LikelikeConstants.DEFAULT_MIN_CLUSTER_SIZE);                
    }
    
    /** maximum number of examples in a cluster */
    private long maximumClusterSize;
    
    /** minimum number of examples in a cluster */    
    private long minimumClusterSize;
}
