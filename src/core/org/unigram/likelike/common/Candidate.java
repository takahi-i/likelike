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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 */
public class Candidate implements
    WritableComparable <Candidate> {
    /** id of example. */
    private LongWritable id;

    /** size of the cluster. */
    private LongWritable size;

    /**
     * Default Constructor.
     */
    public Candidate() {
        this.id = new LongWritable();
        this.size = new LongWritable(0);
    }

    /**
     * Constructor.
     * @param id user id
     * @param inputQueryStr query string
     * @param inputsize time
     */
    public Candidate(final LongWritable id,
            final LongWritable clusterSize) {
        this.id = id;
        this.size = clusterSize;
    }

    /**
     * Get userId who submit the query string.
     * @return id
     */
    public final LongWritable getId() {
        return id;
    }

    /**
     * Get the time when user submitted query.
     * @return size 
     */
    public final LongWritable getSize() {
        return size;
    }

    /**
     * Write the query information for serialization.
     * @param out - DataOutput
     * @throws IOException -
     */
    public final void write(
            final DataOutput out)
        throws IOException {
        id.write(out);
        size.write(out);
    }

    /**
     * Read the serialized the query information.
     * @param in - DataInput
     * @throws IOException -
     */
    public final void readFields(final DataInput in)
        throws IOException {
        id.readFields(in);
        size.readFields(in);
    }
    
    /**
     * Compare query into another one.
     * 
     * @param other query to be compared
     * @return 1 when this is bigger than other
     *          0 when this is the same as other
     *         -1 when this is the smaller than other
     */
    public final int compareTo(final Candidate other) {
        // compare userId
        if (this.id.compareTo(other.id) > 0) {
            return 1;
        }
        if (this.id.compareTo(other.id) < 0) {
            return -1;
        }
        // compare size
        if (this.size.compareTo(other.size) > 0) {
            return 1;
        }
        if (this.size.compareTo(other.size) < 0) {
            return -1;
        }
        return 0;
    }

    /**
     * Validate assigned Object(should Query instance) is the same
     * as this.
     * @param obj a object to be validated.
     * @return true when this and obj is identical contents
     *          false when this and obj is different
     */
    public final boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof Candidate)) {
            return false;
        }

        if ((this.size.get() == ((Candidate) obj).size.get())
            && (this.id.equals(((Candidate) obj).id))) {
            return true;
        } 
        return false;
    }

    /**
     * hashCode.
     * @return hash value
     */
    public final int hashCode() {
        int result = 0;
        result += this.id.hashCode();
        result += this.size.hashCode();
        return result;
    }

}
