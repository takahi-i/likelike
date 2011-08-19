package org.unigram.likelike.validate;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.MultiHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.OutputLogFilter;

import org.unigram.likelike.validate.Validation;

import junit.framework.TestCase;

abstract class RunWithCheck {
    
    public RunWithCheck() {}        
    
    public MultiHashMap run(double threshold) {
        String recommendPath 
            = "testSmallRecommend.txt";
        String featurePath   
            = "testSmallInput.txt";
        String outputPath    
            = "build/test/outputValidation";
        String thresholdStr 
            = Double.toString(threshold);

        /* run validation */
        String[] args = {
                        "-input",  recommendPath, 
                        "-feature",  featurePath,
                        "-output", outputPath,
                        "-threshold", thresholdStr
        };

        Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
        
        Validation job = new Validation();
        
        try {
            job.run(args, conf);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        
        /* extract result*/
        MultiHashMap resultMap = null;
        try {
            return this.getResultMap(conf, 
                    new Path(outputPath));
        } catch (IOException e){ 
            e.printStackTrace();
            return null;
        }
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
    
    /** 
     * to be implemented
     * 
     * @param result
     * @return
     */
    public abstract boolean check(MultiHashMap result);

    private BufferedReader asBufferedReader(final InputStream in)
    throws IOException {
        return new BufferedReader(new InputStreamReader(in));
    }        
    
}

class RunWithCheck00 extends RunWithCheck {

    public boolean check(MultiHashMap resultMap) {
        Set keys = resultMap.keySet();
        Collection coll = (Collection) resultMap.get(new Long(0));
        if (coll == null || coll.size() != 2) { return false; }
        coll = (Collection) resultMap.get(new Long(5));
        if (coll == null || coll.size() != 1) { return false; }        
        coll = (Collection) resultMap.get(new Long(2));
        if (coll == null || coll.size() != 1) { return false; }
        return true;
    }
    
    public boolean run() {
        MultiHashMap resultMap = super.run(0.0);
        if (resultMap == null) {
            return false;
        }
        return this.check(resultMap);
    }
}

class RunWithCheck05 extends RunWithCheck {

    public boolean check(MultiHashMap resultMap) {
        Set keys = resultMap.keySet();
        Collection coll = (Collection) resultMap.get(new Long(0));
        if (coll == null || coll.size() != 1) { return false; }
        coll = (Collection) resultMap.get(new Long(5));
        if (coll == null || coll.size() != 1) { return false; }          
        return true;
    }
    
    public boolean run() {
        MultiHashMap resultMap = super.run(0.5);
        if (resultMap == null) {
            return false;
        }
        return this.check(resultMap);
    }
}


public class TestValidation extends TestCase {

    public TestValidation(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();
    }
    
    public void testRun() {
        RunWithCheck00 run00 = new RunWithCheck00();
        assertTrue(run00.run());
        RunWithCheck05 run05 = new RunWithCheck05();
        assertTrue(run05.run());
        return; 
    }        

}
