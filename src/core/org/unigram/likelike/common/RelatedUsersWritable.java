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

    /**
     * write.
     * @param out output stream
     * @throws IOException -
     */
    @Override
    public void write(final DataOutput out) throws IOException {
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

}
