package org.unigram.likelike.feature;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.unigram.likelike.common.LikelikeConstants;
import org.unigram.likelike.util.accessor.IWriter;

/**
 *
 */
public class FeatureExtractionReducer extends
        Reducer<LongWritable, Text, LongWritable, LongWritable> {
    
    /** max number of output features. */
    private int maxOutputSize = 10;

    /** writer */
    private IWriter writer;
    
    /* TODO refactoring */
    /**
     * reduce. 
     * @param target -
     * @param values -
     * @param context -
     * @throws IOException -
     * @throws InterruptedException -
     */    
    @Override
    public void reduce(final LongWritable target,
            final Iterable<Text> values,
            final Context context)
            throws IOException, InterruptedException {    

        Map<Long, Long> featureCount = new HashMap<Long, Long>();  
        Map<Long, Long> targetFeatures = new HashMap<Long, Long>();
        
        for (Text value : values) {
            String valueStr = value.toString();
            
            String[] valueArray = valueStr.split("\t");
            if (valueArray.length == 2) {  // related example and the features 
                String featureStr = valueArray[1];
                Map<Long, Long> currentFeature 
                    = this.getFeature(featureStr);
                
                Set<Long> demensions = currentFeature.keySet();
                Iterator<Long> iterator = demensions.iterator();
                while (iterator.hasNext()) {
                    Long demension = (Long) iterator.next();
                    if (featureCount.containsKey(demension)) {
                        Long c = featureCount.get(demension);
                        c += 1;
                        featureCount.put(demension, c);
                    } else {
                        featureCount.put(demension, new Long(1));
                    }
                }
            } else if (valueArray.length == 1) { // target features
                targetFeatures = this.getFeature(valueStr);
            }
        }

        /* sort by value and then output */
        ArrayList<Map.Entry> array 
            = new ArrayList<Map.Entry>(featureCount.entrySet());
        Collections.sort(array, new Comparator<Object>(){
            public int compare(final Object o1, final Object o2){
                Map.Entry e1 =(Map.Entry)o1;
                Map.Entry e2 =(Map.Entry)o2;
                Long e1Value = (Long) e1.getValue();
                Long e2Value = (Long) e2.getValue();
                return (e2Value.compareTo(e1Value));
            }
        });
        
        StringBuffer rtString = new StringBuffer();            
        Iterator it = array.iterator();
        int i = 0;
        while(it.hasNext()) {
            if (i >= this.maxOutputSize) { // TODO to be parameterized
                break;
            }
            Map.Entry obj = (Map.Entry) it.next();
            Long feature = (Long) obj.getKey();
            if (!targetFeatures.containsKey(feature)) {
            	//System.out.println("target: " + target + "\tfeature: " + feature);
            	try {
					this.writer.write((Long) target.get(), feature, context);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            }
        }
    }
    
    /**
     * getFeature.
     * @param featureStr feature string.
     * @return feature map.
     */
    private Map<Long, Long> getFeature(
            final String featureStr) {
        Map<Long, Long> rtMap = new HashMap<Long, Long>();
        String[] featureArray = featureStr.split(" ");
        for (int i=0; i<featureArray.length; i++) {
            rtMap.put(Long.parseLong(featureArray[i]), 
                    new Long(1));
        }
        return rtMap;
    }
    
    /**
     * setup.
     * 
     * @param context contains Configuration object to get settings
     */
    @Override
    public final void setup(final Context context) {
        Configuration jc = null;
        
        if (context == null) {
            jc = new Configuration();
        } else {
            jc = context.getConfiguration();
        }
        
      
        // create writer
        String writerClassName = 
                LikelikeConstants.DEFAULT_LIKELIKE_OUTPUT_WRITER;
        
        jc.set(LikelikeConstants.CASSANDRA_COLUMNFAMILY_NAME, 
        		LikelikeConstants.LIKELIKE_CASSANDRA_FEATURE_EXTRACTION_COLUMNFAMILY_NAME);
        
        try {
            writerClassName = 
                    jc.get(LikelikeConstants.LIKELIKE_OUTPUT_WRITER,
                    LikelikeConstants.DEFAULT_LIKELIKE_OUTPUT_WRITER);
            Class<? extends IWriter> extractorClass = Class.forName(
                    writerClassName).asSubclass(IWriter.class);
            Constructor<? extends IWriter> constructor = extractorClass
                    .getConstructor(Configuration.class);
            this.writer = constructor.newInstance(jc);
        } catch (NoSuchMethodException nsme) {
            throw new RuntimeException(nsme);
        } catch (ClassNotFoundException cnfe) {
            throw new RuntimeException(cnfe);
        } catch (InstantiationException ie) {
            throw new RuntimeException(ie);
        } catch (IllegalAccessException iae) {
            throw new RuntimeException(iae);
        } catch (InvocationTargetException ite) {
            throw new RuntimeException(ite.getCause());
        }        
    }

}
