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
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import static org.mockito.Mockito.*;

import junit.framework.TestCase;

public class TestAddFeatureReducer extends TestCase {

    public TestAddFeatureReducer(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();
    }

    public void testReduceSimeple() {
        AddFeatureReducer reducer = new AddFeatureReducer();
        Reducer<LongWritable, Text, LongWritable, Text>.Context mock_context = mock(Reducer.Context.class);               

        /* create input */
        LongWritable key = new LongWritable(42L);
        List<Text> values = Arrays.asList (
                new Text("45"),
                new Text("345"),
                new Text("69434"),
                new Text("43:1 32039:1 3904:1 493:1 9032:1"),                
                new Text("9445"),
                new Text("2346")
                );
        
        try {
            reducer.reduce(key, (Iterable<Text>) values, mock_context);
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
         Text value = new Text(key+"\t"+new String("43:1 32039:1 3904:1 493:1 9032:1"));
         try {
             verify(mock_context, times(1)).write(new LongWritable(45), value);
             verify(mock_context, times(1)).write(new LongWritable(345), value);
             verify(mock_context, times(1)).write(new LongWritable(69434), value);
             verify(mock_context, times(1)).write(new LongWritable(9445), value);
             verify(mock_context, times(1)).write(new LongWritable(2346), value);
         } catch (Exception e) {
             TestCase.fail();
         }

    }

    public void testReduceNoFeature() {
        AddFeatureReducer reducer = new AddFeatureReducer();
        Reducer<LongWritable, Text, LongWritable, Text>.Context mock_context = mock(Reducer.Context.class);               
        
        /* create input */
        LongWritable key = new LongWritable(42L);
        List<Text> values = Arrays.asList (
                new Text("45"),
                new Text("345"),
                new Text("69434"),
                new Text("9445"),
                new Text("2346")
        );
            
        try {
            reducer.reduce(key, (Iterable<Text>) values, mock_context);
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
             
        /* validate the results (should not have output) */
        Text value = null;
        try {
            verify(mock_context, times(0)).write(new LongWritable(45), value);
            verify(mock_context, times(0)).write(new LongWritable(345), value);
            verify(mock_context, times(0)).write(new LongWritable(69434), value);
            verify(mock_context, times(0)).write(new LongWritable(9445), value);
            verify(mock_context, times(0)).write(new LongWritable(2346), value);
        } catch (Exception e) {
            TestCase.fail();
        }         
    }
}
