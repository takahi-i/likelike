package org.unigram.likelike.writer;

import me.prettyprint.cassandra.service.CassandraClient;
import me.prettyprint.cassandra.service.CassandraClientPool;
import me.prettyprint.cassandra.service.CassandraClientPoolFactory;
import me.prettyprint.cassandra.service.Keyspace;
import me.prettyprint.cassandra.service.PoolExhaustedException;

import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer.Context;

import org.apache.thrift.TException;

public class CassandraWriter implements IWriter {
    
    public CassandraWriter(Configuration conf) 
    throws PoolExhaustedException, NotFoundException, Exception{
        super();

        this.pool = CassandraClientPoolFactory.INSTANCE.get();
        String cassandraHost = conf.get(CASSANDRQA_SERVER_NAME, 
                    DEFAULT_CASSANDRQA_SERVER_NAME);
        int cassandraPort = conf.getInt(CASSANDRQA_PORT, 
                DEFAULT_CASSANDRQA_PORT);
        this.client = pool.borrowClient(cassandraHost, cassandraPort);
        String keySpaceName = conf.get(CASSANDRQA_KEYSPACE_NAME, 
                DEFAULT_CASSANDRQA_KEYSPACE_NAME);
        this.keySpace = client.getKeyspace(keySpaceName);
        
        String columnFamily = conf.get(CASSANDRQA_COLUMNFAMILY_NAME, 
                DEFAULT_CASSANDRQA_COLUMNFAMILY_NAME);
        this.columnFamily = columnFamily;
    }

    public boolean write(Long key, Long value, Context context) 
    throws UnavailableException, InvalidRequestException, TException, 
    	TimedOutException {
        ColumnPath cp =  new ColumnPath(this.columnFamily);
        cp.setColumn(value.toString().getBytes());
        System.out.println("key: " + key + "\tvalue: " + value);
        this.keySpace.insert(key.toString(), cp, new Long(1).toString().getBytes());       
        return true;
    }
    
    /** symbol: cassandra server name. */
    public static final String CASSANDRQA_SERVER_NAME
    = "likelike.cassandra.server.name";
    
    /** default: output type. */
    public static final String DEFAULT_CASSANDRQA_SERVER_NAME
    = "localhost";
    
    /** symbol: cassandra port. */
    public static final String CASSANDRQA_PORT
    = "likelike.cassandra.server.port";
    
    /** default: output type. */
    public static final int DEFAULT_CASSANDRQA_PORT
    = 9170;
    
    /** symbol: cassandra keyspace name. */
    public static final String CASSANDRQA_KEYSPACE_NAME
    = "likelike.cassandra.keyspace.name";
    
    /** default: output type. */
    public static final String DEFAULT_CASSANDRQA_KEYSPACE_NAME
    = "Likelike";
    
    /** symbol: cassandra columnfamily name. */
    public static final String CASSANDRQA_COLUMNFAMILY_NAME
    = "likelike.cassandra.columnfamily.name";    
    
    /** default: output type. */
    public static final String DEFAULT_CASSANDRQA_COLUMNFAMILY_NAME
    = "RelatedPairs";
        
    private CassandraWriter() {}
    
    /** clinent pool. */
    private CassandraClientPool pool;
    
    /** client. */
    private CassandraClient client;
    
    /** keyspace. */
    private Keyspace keySpace;
    
    /** columnfamily name to be written. */
    private String columnFamily;
}
