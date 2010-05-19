package org.unigram.likelike.lsh;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import me.prettyprint.cassandra.service.CassandraClient;
import me.prettyprint.cassandra.service.CassandraClientPool;
import me.prettyprint.cassandra.service.CassandraClientPoolFactory;
import me.prettyprint.cassandra.service.Keyspace;
import me.prettyprint.cassandra.service.PoolExhaustedException;
import static me.prettyprint.cassandra.utils.StringUtils.bytes;

import org.apache.cassandra.service.ColumnPath;
import org.apache.cassandra.service.InvalidRequestException;
import org.apache.cassandra.service.NotFoundException;
import org.apache.cassandra.service.TimedOutException;
import org.apache.cassandra.service.UnavailableException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.thrift.TException;

import org.unigram.likelike.common.Candidate;
import org.unigram.likelike.common.LikelikeConstants;
import org.unigram.likelike.writer.CassandraWriter;
import org.unigram.likelike.writer.IWriter;

public class GetRecommendationsCassandraReducer extends
    Reducer<LongWritable, Candidate, LongWritable, LongWritable> {

    private GetRecommendationsCassandraReducer() {
    }
    
    /**
     * reduce. 
     * @param key target
     * @param values candidates
     * @param context -
     * @throws IOException - 
     * @throws InterruptedException -
     */
    public void reduce(final LongWritable key,
            final Iterable<Candidate> values,
            final Context context)
            throws IOException, InterruptedException {
        
        HashMap<Long, Double> candidates 
            = new HashMap<Long, Double>();
        for (Candidate cand : values) {
            Long tid = cand.getId().get();
            if (candidates.containsKey(tid)) {
               Double weight = candidates.get(tid);
               weight += 1.0;
               candidates.put(tid, weight);
            } else {
                candidates.put(tid, 
                        new Double(1.0));
            }
            
            if (candidates.size() > 50000) { // TODO should be parameterized
                break;
            }
        }
        
        /* sort by value and then output */
        ArrayList<Map.Entry> array 
            = new ArrayList<Map.Entry>(candidates.entrySet());
        Collections.sort(array, new Comparator<Object>(){
            public int compare(final Object o1, final Object o2){
                Map.Entry e1 =(Map.Entry)o1;
                Map.Entry e2 =(Map.Entry)o2;
                Double e1Value = (Double) e1.getValue();
                Double e2Value = (Double) e2.getValue();
                return (e2Value.compareTo(e1Value));
            }
        });

        Iterator it = array.iterator();

        int i = 0;
        while(it.hasNext()) {
            if (i >= this.maxOutputSize) {
                return;
            }
            Map.Entry obj = (Map.Entry) it.next();
            try {
                this.writer.write(key.get(), (Long) obj.getKey(), context);
                
            } catch (InvalidRequestException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (UnavailableException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (TException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (TimedOutException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
            i += 1; 
        }
    }
    
    /** maximum number of output per example. */
    private long maxOutputSize;
  
    /** writer */
    private IWriter writer;
   
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
        this.maxOutputSize = jc.getLong(
                LikelikeConstants.MAX_OUTPUT_SIZE , 
                LikelikeConstants.DEFAULT_MAX_OUTPUT_SIZE);
        try {
            this.writer = new CassandraWriter(jc);
        } catch (PoolExhaustedException e) {
            e.printStackTrace();
        } catch (NotFoundException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
 
    }    
    
}