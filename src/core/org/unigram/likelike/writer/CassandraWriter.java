package org.unigram.likelike.writer;

import org.apache.hadoop.mapreduce.Reducer.Context;

public class CassandraWriter implements IWriter {

    @Override
    public boolean write(Long key, Long value, Context context) {
        // TODO Auto-generated method stub
        return false;
    }

}
