package org.unigram.likelike.util.casssandra;

import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.unigram.likelike.util.accessor.cassandra.AccessRecommendedFeatures;

public class AccessRecommendedItemsSample {
	
    public static void main(final String[] argv) {
    	Long target = null;    	
    	
        for (int i = 0; i < argv.length; ++i) {
            if ("-target".equals(argv[i])) {
            	target = Long.parseLong(argv[++i]);
            }
        }
        
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
    	
    	AccessRecommendedFeatures accessor 
    		= new AccessRecommendedFeatures(conf);
    	
    	Map results = null;
    	try {
    		results = accessor.read(target);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
        
        Iterator sItr = results.keySet().iterator();
        while(sItr.hasNext()){
            String v = (String) sItr.next();        	
            System.out.println(v + " ");        	
        }
        
        return;
    }
	
}
