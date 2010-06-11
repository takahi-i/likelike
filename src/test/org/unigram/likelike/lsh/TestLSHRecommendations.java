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
import java.util.List;
import java.util.Map;
import java.util.Set;

import me.prettyprint.cassandra.service.CassandraClient;
import me.prettyprint.cassandra.service.CassandraClientPool;
import me.prettyprint.cassandra.service.CassandraClientPoolFactory;
import me.prettyprint.cassandra.service.Keyspace;
import me.prettyprint.cassandra.testutils.EmbeddedServerHelper;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.commons.collections.MultiMap;
import org.apache.commons.collections.MultiHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.apache.thrift.transport.TTransportException;

import org.unigram.likelike.common.LikelikeConstants;
import org.unigram.likelike.lsh.LSHRecommendations;

import junit.framework.TestCase;

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
        this.run(depth, iterate, LikelikeConstants.DEFAULT_LIKELIKE_OUTPUT_WRITER, conf);

        /* check output */
        try {
            assertTrue(this.check(conf, new Path(this.outputPath)));
        } catch (IOException e) {
            fail("Got IOException");
            e.printStackTrace();
        }
        return true;
    }
    
    public boolean cassandraRunWithCheck(int depth, int iterate) {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
        
        // run
        if (this.run(depth, iterate, 
        		"org.unigram.likelike.writer.CassandraWriter", conf) == false) {
            return false;
        }
        
        assertTrue(this.checkCassandra(conf));
        return true;
    }

    public boolean run(int depth, int iterate, String writer, Configuration conf) {
        /* run lsh */
        String[] args = {"-input",  this.inputPath, 
                         "-output", this.outputPath,
                         "-depth",  Integer.toString(depth),
                         "-iterate", Integer.toString(iterate) 
        };
        conf.set(LikelikeConstants.LIKELIKE_OUTPUT_WRITER, writer);
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
        
        
        try {
            embedded = new EmbeddedServerHelper();
            embedded.setup();
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        try {
            this.pools = CassandraClientPoolFactory.INSTANCE.get();
            this.client = pools.borrowClient("localhost", 9170);
            this.keyspace = client.getKeyspace("Likelike");
        } catch (Exception e){
            e.printStackTrace();
        }         
        
        assertTrue(this.cassandraRunWithCheck(1, 1));
        assertTrue(this.cassandraRunWithCheck(1, 5));
        assertTrue(this.cassandraRunWithCheck(1, 10));
        
        embedded.teardown();
        
    }

    private boolean checkCassandra(Configuration conf) {
        ColumnParent clp = new ColumnParent("RelatedPairs");
        SliceRange sr = new SliceRange(new byte[0], 
                new byte[0], false, 150);
        SlicePredicate sp = new SlicePredicate();
        sp.setSlice_range(sr);
        
        Long keys[] = {0L, 1L, 2L, 3L, 7L, 8L};
        MultiHashMap resultMap = new MultiHashMap();
        for (int i =0; i<keys.length; i++) {
            Long key = keys[i];
            try {
            	List<Column> cols  = keyspace.getSlice(key.toString(), clp, sp);
                System.out.println("key:" + key.toString() + "\tcols.size() = " + cols.size());
                
                Iterator itrHoge = cols.iterator();
                while(itrHoge.hasNext()){
                    Column c = (Column) itrHoge.next();
                    System.out.println("\tc.name: " + new String(c.name));
                    resultMap.put(key, // target  
                            Long.parseLong(new String(c.name)));                    
                   }
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            } 
        }
        
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
        
        return true;        
    }
    
    
    private boolean check(Configuration conf, 
            Path outputPath) 
    throws IOException {
        FileSystem fs = FileSystem.getLocal(conf);
        Path[] outputFiles = FileUtil.stat2Paths(
            fs.listStatus(outputPath, new OutputLogFilter()));

        //if (outputFiles != null) {
        //    TestCase.assertEquals(outputFiles.length, 1);
        //} else {
        //    TestCase.fail();
        //}

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
    
    private String inputPath  = "build/test/resources/testSmallInput.txt";

    private String outputPath = "build/test/resources/outputLSH";

    private static EmbeddedServerHelper embedded;

    private CassandraClient client;
    
    private Keyspace keyspace;
    
    private CassandraClientPool pools;    
    
}
