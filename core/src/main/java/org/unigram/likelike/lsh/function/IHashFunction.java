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

import java.util.Set;

import org.apache.hadoop.io.LongWritable;

/**
 * Interface hash function for LSH. 
 */
public interface IHashFunction {
    /**
     * Compute hashing function.
     *  
     * @param featureVector feature vector
     * @param seed hash seed
     * @return hashed value
     */
    LongWritable returnClusterId(Set<Long> featureVector, 
            long seed);
}
