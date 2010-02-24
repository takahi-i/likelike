package org.unigram.likelike.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class RelatedUsersWritable implements Writable {

    /** contains IDs of related users. */
    private List<LongWritable> relatedUsers;
    
    public RelatedUsersWritable() {
    }

    public RelatedUsersWritable(List<LongWritable> users) {
        this.relatedUsers = users;
    }

    public List<LongWritable> getRelatedUsers() {
        return relatedUsers;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.relatedUsers= new ArrayList<LongWritable>();
        try {
            do {
                long userID = in.readLong();
                this.relatedUsers.add(new LongWritable(userID));
            } while (true);
        } catch (EOFException e) {
            // do nothing
        } catch (ArrayIndexOutOfBoundsException e) {
            // do nothing
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        for (LongWritable item : this.relatedUsers) {
            out.writeLong(item.get());
        }
    }
    
    @Override
    public String toString() {
        StringBuilder rtStr = new StringBuilder();
        for (LongWritable user : this.relatedUsers) {
            rtStr.append(user);
            rtStr.append(' ');      
        }
        return rtStr.toString();
    }

}
