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
package org.unigram.likelike.lsh.function;

import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;

import org.unigram.likelike.common.LikelikeConstants;

/**
 * 
 */
public class MinWiseFunction implements IHashFunction {

    /**
     * 
     */
    @Override
    public LongWritable returnClusterId(Map<Long,Long> featureVector) {
        long clusterId = 0;
        
        TreeMap<Long,Long> hashedFeatureVector 
            = new TreeMap<Long,Long>(); // key: hashed, value: id 
        for (Long key : featureVector.keySet()) {
            hashedFeatureVector.put(this.calcHash.run(key), 
                        new Long(featureVector.get(key)));
        }
        for (int i = 0; i < this.depth; i ++ ) {
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
     * Constructor.
     * 
     * @param conf get parameters.
     */
    public MinWiseFunction(Configuration conf) {
        this.calcHash = new CalcHashValue(conf.getLong(MINWISE_HASH_SEED, 
                DEFAULT_MINWISE_HASH_SEED));
        this.depth = conf.getInt(LikelikeConstants.FEATURE_DEPTH,
                LikelikeConstants.DEFAULT_FEATURE_DEPTH);
    }
    
    /** symbol: hash seed. */
    public static final String MINWISE_HASH_SEED
        = "likelike.minwise.hash.seed";
    
    /** default: hash seed. */
    public static final long DEFAULT_MINWISE_HASH_SEED    
        = 1L;    
    
    /** for safe. */
    private MinWiseFunction() {
        /* setting dummies */ 
        this.depth = 0;
        this.calcHash = null;
    };
    
    
    /** Depth of cluster. */    
    private final int depth;
    
    /** Calculator of hashed value */
    private final CalcHashValue calcHash;
    
}
