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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.MultiMap;
import org.apache.commons.collections.MultiHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.OutputLogFilter;

import org.unigram.likelike.lsh.DFSLSHRecommendations;

import junit.framework.TestCase;

public class TestDFSLSHRecommendations extends TestCase {

    public TestDFSLSHRecommendations(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();
    }

    
    public boolean runWithCheck(int depth, int iterate) {
        String inputPath = "src/test/build/resources/testSmallInput.txt";
        String outputPath = "src/test/build/outputLsh"; 
        /* run lsh */
        String[] args = {"-input",  inputPath, 
                         "-output", outputPath,
                         "-depth",  Integer.toString(depth),
                         "-iterate", Integer.toString(iterate) 
        };

        Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
        
        DFSLSHRecommendations job = new DFSLSHRecommendations();
        
        try {
            job.run(args, conf);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        
        /* check output */
        try {
            assertTrue(this.check(conf, new Path(outputPath)));
        } catch (IOException e) {
            fail("Got IOException");
            e.printStackTrace();
        }
        
        return true;
    }
    
    public void testRun() {
        assertTrue(this.runWithCheck(1, 1));
        assertTrue(this.runWithCheck(1, 5));
        assertTrue(this.runWithCheck(1, 10));
        
        //assertTrue(this.runWithCheck(2, 1));
        //assertTrue(this.runWithCheck(2, 5));
        //assertTrue(this.runWithCheck(2, 10));
        
        //assertTrue(this.runWithCheck(3, 1));
        //assertTrue(this.runWithCheck(3, 5));
        //assertTrue(this.runWithCheck(3, 10));
        
        /*TODO add tests for pareters such as minCluster */ 
        return; 
    }

    private boolean check(Configuration conf, 
            Path outputPath) 
    throws IOException {
        FileSystem fs = FileSystem.getLocal(conf);
        Path[] outputFiles = FileUtil.stat2Paths(
            fs.listStatus(outputPath, new OutputLogFilter()));

        if (outputFiles != null) {
            TestCase.assertEquals(outputFiles.length, 1);
        } else {
            TestCase.fail();
        }

        BufferedReader reader = this.asBufferedReader(
                fs.open(outputFiles[0]));        
        
        String line;
        MultiHashMap resultMap = new MultiHashMap();
        while ((line = reader.readLine()) != null) {
            String[] lineArray = line.split("\t");
            resultMap.put(Long.parseLong(lineArray[0]), // target 
                    Long.parseLong(lineArray[1]));      // recommended
            
        }
        
        /* basic test cases */
        Set keys = resultMap.keySet();
        Collection coll = (Collection) resultMap.get(new Long(0));
        assertTrue(coll.size() >= 2 && coll.size() <= 4);
        coll = (Collection) resultMap.get(new Long(1));
        assertTrue(coll.size() >= 2 && coll.size() <= 4);
        coll = (Collection) resultMap.get(new Long(2));
        assertTrue(coll.size() >= 2 && coll.size() <= 4);
        coll = (Collection) resultMap.get(new Long(3));
        assertTrue(coll.size() >= 1 && coll.size() <= 3);
        
        /* examples with no recommendation */
        assertFalse(resultMap.containsKey(new Long(7)));
        assertFalse(resultMap.containsKey(new Long(8)));
        
        return true;
    }
    
    private BufferedReader asBufferedReader(final InputStream in)
    throws IOException {
        return new BufferedReader(new InputStreamReader(in));
    }    
    
}
