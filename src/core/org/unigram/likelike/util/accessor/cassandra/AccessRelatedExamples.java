package org.unigram.likelike.util.accessor.cassandra;

import java.util.Map;

import me.prettyprint.cassandra.service.PoolExhaustedException;

import org.apache.cassandra.thrift.NotFoundException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.unigram.likelike.common.LikelikeConstants;
import org.unigram.likelike.util.accessor.CassandraWriter;
import org.unigram.likelike.util.accessor.IReader;
import org.unigram.likelike.util.accessor.IWriter;

public class AccessRelatedExamples implements IWriter, IReader {

	public AccessRelatedExamples(Configuration conf) {
		super();
        conf.set(LikelikeConstants.CASSANDRA_COLUMNFAMILY_NAME, 
        		LikelikeConstants.LIKELIKE_CASSANDRA_LSH_COLUMNFAMILY_NAME);		
		try {
			this.writer = new CassandraWriter(conf);
		} catch (PoolExhaustedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public boolean write(Long key, Long value, Context context)
			throws Exception, InterruptedException {
		return this.writer.write(key, value, context);
	}

	@Override
	public Map<String, byte[]> read(Long key) throws Exception,
			InterruptedException {
		return this.writer.read(key);
	}

	private CassandraWriter writer;
	
}
