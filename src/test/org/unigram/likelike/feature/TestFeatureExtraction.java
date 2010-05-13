package org.unigram.likelike.feature;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.apache.commons.collections.MultiHashMap;

import junit.framework.TestCase;

public class TestFeatureExtraction extends TestCase {

    public TestFeatureExtraction(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();
    }
    
    public void testRun() {
        String recommendPath 
        = "build/test/resources/testSmallRecommend.txt";
        String featurePath   
        = "build/test/resources/testSmallInput.txt";
        String outputPath    
        = "build/test/outputFeatureExtraction";

    
        /* run feature extraction */
        String[] args = {
                "-input",   recommendPath, 
                "-feature", featurePath,
                "-output", outputPath,
        };

        Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
    
        FeatureExtraction job = new FeatureExtraction();
    
        try {
            job.run(args, conf);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
    
        /* extract result*/
        MultiHashMap resultMap = null;
        try {
            resultMap = this.getResultMap(conf, 
                    new Path(outputPath));
        } catch (IOException e){ 
                e.printStackTrace();
                return;
        }
        
        /* check result */
        assertTrue(this.checkResults(resultMap));
        
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
            String[] featureArray = lineArray[1].split(" ");
            for (int i=0; i < featureArray.length; i++) {
                resultMap.put(Long.parseLong(lineArray[0]),
                        Long.parseLong(featureArray[i]));
            }
            
        }
        return resultMap;
    }    
    
    private BufferedReader asBufferedReader(final InputStream in)
    throws IOException {
        return new BufferedReader(new InputStreamReader(in));
    }     
}
