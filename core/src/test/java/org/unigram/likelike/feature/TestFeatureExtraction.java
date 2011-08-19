package org.unigram.likelike.feature;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.apache.commons.collections.MultiHashMap;
import org.unigram.likelike.common.LikelikeConstants;
import org.unigram.likelike.util.accessor.IWriter;

import junit.framework.TestCase;

public class TestFeatureExtraction extends TestCase {

    public TestFeatureExtraction(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();
    }
    
    public void testRun() {
    	// test with hadoop dfs
    	this.dfsRunWithCheck();
    }
    

    public void dfsRunWithCheck() {
        // settings 
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");

        // run
        this.run("dfs", conf);       
    
        /* extract result*/
        MultiHashMap resultMap = null;
        try {
            resultMap = this.getResultMap(conf, 
                    new Path(this.outputPath));
        } catch (IOException e){ 
                e.printStackTrace();
                return;
        }
        /* check result */
        assertTrue(this.checkResults(resultMap));
    }
    
    public boolean run(String writer, Configuration conf) {

         /* run feature extraction */
         String[] args = {
                 "-input",  this.recommendPath, 
                 "-feature", this.featurePath,
                 "-output", this.outputPath,
                 "-storage", writer
         };

         conf.set(LikelikeConstants.LIKELIKE_OUTPUT_WRITER, writer);         
         FeatureExtraction job = new FeatureExtraction();
     
         try {
             job.run(args, conf);
         } catch (Exception e) {
             e.printStackTrace();
             return false;
         }    	
         return true;
    }
    
    public boolean checkResults(MultiHashMap resultMap) {
        Set keys = resultMap.keySet();
        assertTrue(keys.size() == 2);

        Collection coll = (Collection) resultMap.get(new Long(0));
        if (coll == null || coll.size() != 3) { return false; }
        assertTrue(coll.contains(new Long(5)));
        assertTrue(coll.contains(new Long(6)));
        assertTrue(coll.contains(new Long(7)));
        assertFalse(coll.contains(new Long(1)));
        assertFalse(coll.contains(new Long(2)));
        
        coll = (Collection) resultMap.get(new Long(2));
        if (coll == null || coll.size() != 2) { return false; }          
        assertTrue(coll.contains(new Long(10)));
        assertTrue(coll.contains(new Long(11)));
        assertFalse(coll.contains(new Long(1)));
        assertFalse(coll.contains(new Long(2)));        
        
        coll = (Collection) resultMap.get(new Long(5));
        if (coll != null) { return false; }

        return true;
    }
    
    protected MultiHashMap getResultMap(
            Configuration conf, 
            Path outputPath) throws IOException {
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
                resultMap.put(Long.parseLong(lineArray[0]),
                        Long.parseLong(lineArray[1]));
        }
        return resultMap;
    }    
    
    private BufferedReader asBufferedReader(final InputStream in)
    throws IOException {
        return new BufferedReader(new InputStreamReader(in));
    }
    
    private String recommendPath  = "testSmallRecommend.txt";

    private String featurePath = "testSmallInput.txt";
    
    private String outputPath = "outputFeatureExtraction";    

}
