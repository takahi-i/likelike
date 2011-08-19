/**
 * Copyright 2009 Takahiko Ito
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.unigram.likelike.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

/**
 *
 */
public class RelatedUsersWritable implements Writable {

    /** contains IDs of related users. */
    private List<LongWritable> relatedUsers;
    
    /**
     * Constructor.
     */
    public RelatedUsersWritable() {}

    /**
     * Constructor. 
     * @param users related users
     */
    public RelatedUsersWritable(final List<LongWritable> users) {
        this.relatedUsers = users;
    }
    
    /**
     * Constructor. 
     * @param id related user id
     */
    public RelatedUsersWritable(final Long id) {
        List<LongWritable> eid = 
            new ArrayList<LongWritable>();
        eid.add(new LongWritable(id));
        this.relatedUsers = eid;
    }
    
    /**
     * Get related users.
     * @return set of related users
     */
    public List<LongWritable> getRelatedUsers() {
        return this.relatedUsers;
    }

    /**
     * Create RelatedUsersWritable from input stream.
     * 
     * @param in input stream
     * @throws IOException -
     */
    @Override
    public void readFields(final DataInput in) throws IOException {
        try {
            int listSize = in.readInt();
            this.relatedUsers= new ArrayList<LongWritable>(listSize);
            for(int i=0; i<listSize; i++) {
                long userID = in.readLong();
                this.relatedUsers.add(new LongWritable(userID));
            } 
        } catch (EOFException e) {
            e.printStackTrace();
        } catch (ArrayIndexOutOfBoundsException e) {
            e.printStackTrace();
        }
    }

    /**
     * write.
     * @param out output stream
     * @throws IOException -
     */
    @Override
    public void write(final DataOutput out) throws IOException {
        out.writeInt(this.relatedUsers.size());
        for (LongWritable item : this.relatedUsers) {
            out.writeLong(item.get());
        }
    }
    
    /**
     * Create String.
     * 
     * @return string reprsents for the related users. 
     */
    @Override
    public String toString() {
        StringBuilder rtStr = new StringBuilder();
        for (LongWritable user : this.relatedUsers) {
            rtStr.append(user);
            rtStr.append(' ');      
        }
        return rtStr.toString();
    }
    
    /**
     * equals.
     * 
     * @param o checked whether o is identical to this or not  
     * @return true when o is identical to this, otherwise return true
     */
    @Override
    public boolean equals(final Object o) {
        if (o instanceof RelatedUsersWritable) {
            RelatedUsersWritable that = (RelatedUsersWritable) o;
            return (this.relatedUsers.equals(that.relatedUsers));
        }
        return false;
    }
    
    /**
     * hashCode.
     * @return hash value
     */
    public int hashCode() {
        return this.relatedUsers.hashCode();
    }

}
