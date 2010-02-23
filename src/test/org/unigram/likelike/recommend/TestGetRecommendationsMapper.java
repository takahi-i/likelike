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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.unigram.likelike.common.Candidate;
import org.unigram.likelike.lsh.GetRecommendationsMapper;

import junit.framework.TestCase;

import static org.mockito.Mockito.*;

public class TestGetRecommendationsMapper extends TestCase {

    public TestGetRecommendationsMapper(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();
    }

    public void testMap() {
        GetRecommendationsMapper mapper =
            new GetRecommendationsMapper();

        Mapper<LongWritable, Text, LongWritable, 
        Candidate>.Context mock_context
            = mock(Mapper.Context.class);        
        
        Text value = new Text("1:443:2:5:3:54:434:");
        LongWritable hashedClusterId 
            = new LongWritable(143248978L);
        LongWritable clusterSize 
            = new LongWritable(7L); 
        try {
            /*
             * key - hashed clusterId
             * value - example ids exist in the cluster with clusterId. 
             */
            mapper.map(hashedClusterId, value, mock_context);
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
        
         try {
             /* case: simple */
             verify(mock_context, times(1)).write(new LongWritable(54L),
             new Candidate(new LongWritable(443L), clusterSize));

             verify(mock_context, times(1)).write(new LongWritable(5L),
                     new Candidate(new LongWritable(54L), clusterSize));
             
             /* case: symmetric */
             verify(mock_context, times(1)).write(new LongWritable(443L),
                     new Candidate(new LongWritable(54L), clusterSize));
             
             /* case: self recommendaton */
             verify(mock_context, times(0)).write(new LongWritable(443L),
                     new Candidate(new LongWritable(443L), clusterSize));
             
             /* case: id not in the cluster */
             verify(mock_context, times(0)).write(new LongWritable(98L),
                     new Candidate(new LongWritable(443L), clusterSize));
             
         } catch (Exception e) {
             TestCase.fail();
         }         
        
    }
    
}
