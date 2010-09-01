/**
 * Copyright 2009 Takahiko Ito
 * 
 * 
 * Licenced under the Apache License, Version 2.0 (the "License"); 
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
package org.unigram.likelike.lsh.function;

import java.util.TreeMap;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;

import org.unigram.likelike.common.LikelikeConstants;

/**
 * MinWiseFunction.
 */
public class MinWiseFunction 
    implements IHashFunction {
    
    /**
     * Return cluster id on the hash value for input feature vector.
     * 
     * @param featureVector feature vector
     * @param seed hash seed
     * @return cluster id
     */
    @Override
    public LongWritable returnClusterId(
            final Set<Long> featureVector, final long seed) {
        
        TreeMap<Long, Long> hashedFeatureVector 
            = this.createHashedVector(featureVector, seed);
        
        long clusterId = 0;
        for (int i = 0; i < this.depth; i++) {
            if (hashedFeatureVector.size() <= 0) {
                return new LongWritable(clusterId);
            }
            Long minimum = hashedFeatureVector.firstKey();
            clusterId += (minimum + (i * 13));
            hashedFeatureVector.remove(minimum);
        }
        return new LongWritable(clusterId);
    }
   
    /**
     * create hashed feature vector.
     * 
     * @param featureVector input
     * @param seed hash seed
     * @return hashed feature vector
     */
    TreeMap<Long, Long> createHashedVector(
            final Set<Long> featureVector, final long seed) {
        TreeMap<Long, Long> hashedFeatureVector 
        = new TreeMap<Long, Long>(); // key: hashed feature-id, value: dummy
    
        for (Long key : featureVector) {
            hashedFeatureVector.put(this.calcHash.run(key, seed), 
                    key);
        }
        return hashedFeatureVector;
    }
    
    /**
     * Constructor.
     * 
     * @param conf get parameters
     */
    public MinWiseFunction(final Configuration conf) {
        this.calcHash = new CalcHashValue();
        this.depth = conf.getInt(LikelikeConstants.FEATURE_DEPTH,
                LikelikeConstants.DEFAULT_FEATURE_DEPTH);
    }
    
    /** for safe. */
    private MinWiseFunction() {
        /* setting dummies */ 
        this.depth = 0;
        this.calcHash = null;
    };

    /** Depth of cluster. */
    private final int depth;
    
    /** Calculator of hashed value. */
    private final CalcHashValue calcHash;
    
}
