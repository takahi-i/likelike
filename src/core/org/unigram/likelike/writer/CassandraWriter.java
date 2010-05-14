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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.thrift.TException;

public class CassandraWriter implements IWriter {

    public boolean write(Long key, Long value, Context context) 
    throws UnavailableException, InvalidRequestException, TException, TimedOutException {
        ColumnPath cp =  new ColumnPath("RelatedPairs", null, 
                null);                
        this.ks.insert(key.toString(), cp,
                bytes(value.toString()));        
        return false;
    }
    
    private CassandraWriter() {}
    
    private CassandraWriter(Context context) 
    throws PoolExhaustedException, NotFoundException, TException, Exception {
        this.pool = CassandraClientPoolFactory.INSTANCE.get();
        this.client = pool.borrowClient("localhost", 9170); // TODO parameterize
        this.ks = client.getKeyspace("Likelike"); 
    }    

    private CassandraClientPool pool;
    private CassandraClient client;
    private Keyspace ks;

    
}
