package org.unigram.likelike.util.accessor;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.service.CassandraClient;
import me.prettyprint.cassandra.service.CassandraClientPool;
import me.prettyprint.cassandra.service.CassandraClientPoolFactory;
import me.prettyprint.cassandra.service.Keyspace;
import me.prettyprint.cassandra.service.PoolExhaustedException;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.commons.collections.MultiHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer.Context;

import org.apache.thrift.TException;

public class CassandraWriter implements IWriter, IReader {
    
    public CassandraWriter(Configuration conf) 
    throws PoolExhaustedException, NotFoundException, Exception{
        super();
        this.pool = CassandraClientPoolFactory.INSTANCE.get();
        String cassandraHost = conf.get(CASSANDRA_SERVER_NAME, 
        		DEFAULT_CASSANDRA_SERVER_NAME);
        int cassandraPort = conf.getInt(CASSANDRA_PORT, 
                DEFAULT_CASSANDRA_PORT);
        this.client = pool.borrowClient(cassandraHost, cassandraPort);
        String keySpaceName = conf.get(CASSANDRA_KEYSPACE_NAME, 
                DEFAULT_CASSANDRA_KEYSPACE_NAME);
        this.keySpace = client.getKeyspace(keySpaceName);
        String columnFamily = conf.get(CASSANDRA_COLUMNFAMILY_NAME, 
                DEFAULT_CASSANDRA_COLUMNFAMILY_NAME);
        this.columnFamily = columnFamily;
    }

    public boolean write(Long key, Long value, Context context) 
    throws UnavailableException, InvalidRequestException, TException, 
    	TimedOutException {
        ColumnPath cp =  new ColumnPath(this.columnFamily);
        cp.setColumn(value.toString().getBytes());
        this.keySpace.insert(key.toString(), 
        		cp, new Long(1).toString().getBytes());       
        return true;
    }
    
    public Map<String, byte[]> read(Long key) {
    	ColumnParent clp = new ColumnParent(this.columnFamily);
    	SliceRange sr = new SliceRange(new byte[0], 
                new byte[0], false, 150);
        SlicePredicate sp = new SlicePredicate();
        sp.setSlice_range(sr);

        HashMap<String, byte[]> resultMap 
        	= new HashMap<String, byte[]>();        
        try {
        	List<Column> cols  
        		= this.keySpace.getSlice(key.toString(), clp, sp);
            Iterator<Column> itrHoge = cols.iterator();
            while(itrHoge.hasNext()){
                Column c = (Column) itrHoge.next();
                resultMap.put(new String(c.name), c.value);      
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } 
		return resultMap;
    }
    
    /** symbol: cassandra server name. */
    public static final String CASSANDRA_SERVER_NAME
    = "likelike.cassandra.server.name";
    
    /** default: output type. */
    public static final String DEFAULT_CASSANDRA_SERVER_NAME
    = "localhost";
    
    /** symbol: cassandra port. */
    public static final String CASSANDRA_PORT
    = "likelike.cassandra.server.port";
    
    /** default: output type. */
    public static final int DEFAULT_CASSANDRA_PORT
    = 9170;
    
    /** symbol: cassandra keyspace name. */
    public static final String CASSANDRA_KEYSPACE_NAME
    = "likelike.cassandra.keyspace.name";
    
    /** default: output type. */
    public static final String DEFAULT_CASSANDRA_KEYSPACE_NAME
    = "Likelike";
    
    /** symbol: cassandra columnfamily name. */
    public static final String CASSANDRA_COLUMNFAMILY_NAME
    = "likelike.cassandra.columnfamily.name";    
    
    /** default: output type. */
    public static final String DEFAULT_CASSANDRA_COLUMNFAMILY_NAME
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
