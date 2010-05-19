package org.unigram.likelike.writer;

import static me.prettyprint.cassandra.utils.StringUtils.bytes;
import me.prettyprint.cassandra.service.CassandraClient;
import me.prettyprint.cassandra.service.CassandraClientPool;
import me.prettyprint.cassandra.service.CassandraClientPoolFactory;
import me.prettyprint.cassandra.service.Keyspace;
import me.prettyprint.cassandra.service.PoolExhaustedException;

import org.apache.cassandra.service.ColumnPath;
import org.apache.cassandra.service.InvalidRequestException;
import org.apache.cassandra.service.NotFoundException;
import org.apache.cassandra.service.TimedOutException;
import org.apache.cassandra.service.UnavailableException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.thrift.TException;

public class CassandraWriter implements IWriter {
    
    public CassandraWriter(Configuration conf) 
    throws PoolExhaustedException, NotFoundException, Exception{
        super();
        this.pool = CassandraClientPoolFactory.INSTANCE.get();
        this.client = pool.borrowClient("localhost", 9170); // TODO parameterize
        this.ks = client.getKeyspace("Likelike");         
    }

    public boolean write(Long key, Long value, Context context) 
    throws UnavailableException, InvalidRequestException, TException, TimedOutException {
        ColumnPath cp =  new ColumnPath("RelatedPairs", null, 
                value.toString().getBytes());                
        this.ks.insert(key.toString(), cp, new Long(1).toString().getBytes());       
        return false;
    }
    
    private CassandraWriter() {}
        
    /** clinent pool. */
    private CassandraClientPool pool;
    
    /** client. */
    private CassandraClient client;
    
    /** keyspace. */
    private Keyspace ks;

}
