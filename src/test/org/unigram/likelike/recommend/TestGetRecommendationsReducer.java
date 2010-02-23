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
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.mockito.InOrder;
import org.unigram.likelike.common.Candidate;
import org.unigram.likelike.lsh
.GetRecommendationsReducer;

import junit.framework.TestCase;

import static org.mockito.Mockito.*;

public class TestGetRecommendationsReducer extends TestCase {

    public TestGetRecommendationsReducer(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();
    }

    public void testReduce() {
        GetRecommendationsReducer reducer = new GetRecommendationsReducer();
        Reducer<LongWritable, Candidate, 
        LongWritable, LongWritable>.Context mock_context 
        = mock(Reducer.Context.class);        
     
        /* make input parameters */
        LongWritable key = new LongWritable(32L);
        List<Candidate> values = Arrays.asList (
            new Candidate(new LongWritable(1)  , new LongWritable(100)),
            new Candidate(new LongWritable(2)  , new LongWritable(1000)),
            new Candidate(new LongWritable(1)  , new LongWritable(20)),
            new Candidate(new LongWritable(1)  , new LongWritable(254)),
            new Candidate(new LongWritable(3)  , new LongWritable(2534)),
            new Candidate(new LongWritable(4)  , new LongWritable(253)),
            new Candidate(new LongWritable(5)  , new LongWritable(25)),
            new Candidate(new LongWritable(6)  , new LongWritable(25)),
            new Candidate(new LongWritable(7)  , new LongWritable(25)),
            new Candidate(new LongWritable(8)  , new LongWritable(25)),
            new Candidate(new LongWritable(9)  , new LongWritable(25)),
            new Candidate(new LongWritable(10)  , new LongWritable(25)),
            new Candidate(new LongWritable(11)  , new LongWritable(25434)),
            new Candidate(new LongWritable(12)  , new LongWritable(554545))
        );
        
        try {
            reducer.setup((Context) null);
            reducer.reduce(key, (Iterable<Candidate>) values, mock_context);
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
         
         /* validate the results */
         try {
             /* simple */
             verify(mock_context, times(1)).write(key, new LongWritable(1));
             verify(mock_context, times(1)).write(key, new LongWritable(4));
             verify(mock_context, times(1)).write(key, new LongWritable(10));
             
             /* id below the threshold (default 10) */
             verify(mock_context, times(0)).write(key, new LongWritable(11));
             verify(mock_context, times(0)).write(key, new LongWritable(12));
             
             /*id out of the ranking*/ 
             verify(mock_context, times(0)).write(key, new LongWritable(143892));
             
             /* oder */
             InOrder inOrder = inOrder(mock_context);
             inOrder.verify(mock_context).write(key, new LongWritable(1));
             //inOrder.verify(mock_context).write(key, new LongWritable(4));
             //inOrder.verify(mock_context).write(key, new LongWritable(3));
             
         } catch (Exception e) {
             TestCase.fail();
         }
        
    }
}
