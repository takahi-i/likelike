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
import java.util.Map;
import java.util.TreeMap;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.unigram.likelike.common.LikelikeConstants;
import org.unigram.likelike.common.SeedClusterId;
import org.unigram.likelike.lsh.function.CalcHashValue;
import org.unigram.likelike.lsh.function.MinWiseFunction;

import static org.mockito.Mockito.*;

public class TestSelectClustersMapper extends TestCase {

    private static final long HASH_SEED = 1L;

    public TestSelectClustersMapper(String name) {
        super(name);
    }

    @SuppressWarnings("unchecked")
    public void testMap() {

        CalcHashValue calcHash = new CalcHashValue();         

        SelectClustersMapper mapper = new SelectClustersMapper();        
        Mapper<LongWritable, Text, SeedClusterId, LongWritable>.Context mock_context
            = mock(Mapper.Context.class);
        mapper.setup(mock_context);
        
        /**/ 
        try {
            Text value = new Text("327\t1:3 2:3 43:3 21:1");
            mapper.map(null, value, mock_context);
        } catch (IOException e) {
            e.printStackTrace();
            TestCase.fail();
         } catch (InterruptedException e) {
             e.printStackTrace();
             TestCase.fail();
         } catch (Exception e) {
             e.printStackTrace();
             TestCase.fail();
         }
         
         TreeMap<Long,Long> hashedFeatureVector 
             = new TreeMap<Long,Long>();
         int[] features = {1, 2, 43, 21}; 
         
         for (int i = 0; i<features.length; i++ ) {
             hashedFeatureVector.put(calcHash.run(new Long(features[i]), this.HASH_SEED), 
                         new Long(1));
         }         
         
         try {
             verify(mock_context, times(1)).write(
                     new SeedClusterId(this.HASH_SEED, hashedFeatureVector.firstKey()),
                     new LongWritable(327));
         } catch (Exception e) {
             TestCase.fail();
         }  
        
    }


}
