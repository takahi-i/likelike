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

import junit.framework.TestCase;
import org.apache.commons.collections.MultiHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.OutputLogFilter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;

public class TestLSHRecommendations extends TestCase {

   
    public TestLSHRecommendations(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();        
    }
    
    protected void tearDown() throws IOException {
    }    
    
    
    public boolean dfsRunWithCheck(int depth, int iterate) {
        // settings 
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");

        // run
        this.run(depth, iterate, "dfs", conf);

        /* check output */
        try {
            assertTrue(this.dfsCheck(conf, new Path(this.outputPath)));
        } catch (IOException e) {
            fail("Got IOException");
            e.printStackTrace();
        }
        return true;
    }
    
    public boolean run(int depth, int iterate, String writer, Configuration conf) {
        /* run lsh */
        String[] args = {"-input",  this.inputPath, 
                         "-output", this.outputPath,
                         "-depth",  Integer.toString(depth),
                         "-iterate", Integer.toString(iterate),
                         "-storage", writer
                         
        };
        
        LSHRecommendations job = new LSHRecommendations();
        
        try {
            job.run(args, conf);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
    
    public void testRun() {
        assertTrue(this.dfsRunWithCheck(1, 1));
        assertTrue(this.dfsRunWithCheck(1, 5));
        assertTrue(this.dfsRunWithCheck(1, 10));
    }
    
    private void check(MultiHashMap resultMap) {
        /* basic test cases */
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
    }

    private boolean dfsCheck(Configuration conf, 
            Path outputPath) 
    throws IOException {
        FileSystem fs = FileSystem.getLocal(conf);
        Path[] outputFiles = FileUtil.stat2Paths(
            fs.listStatus(outputPath, new OutputLogFilter()));

        BufferedReader reader = this.asBufferedReader(
                fs.open(outputFiles[1]));
        
        String line;
        MultiHashMap resultMap = new MultiHashMap();
        while ((line = reader.readLine()) != null) {
            String[] lineArray = line.split("\t");
            resultMap.put(Long.parseLong(lineArray[0]), // target 
                    Long.parseLong(lineArray[1]));      // recommended
            
        }
        this.check(resultMap);
        return true;
    }
    
    private BufferedReader asBufferedReader(final InputStream in)
    throws IOException {
        return new BufferedReader(new InputStreamReader(in));
    }
    
    private String inputPath  = "testSmallInput.txt";

    private String outputPath = "outputLSH";
}
