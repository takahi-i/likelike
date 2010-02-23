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

public class CalcHashValue {
    
    public CalcHashValue(Long hashSeed) {
        System.out.println("hashSeed:" + hashSeed);
        this.hashSeed = hashSeed;
    }

    /**
     * Create hashed value from the given 
     * parameter and seed.
     *  
     * NOTE: applied 64 bit hash function by Thomas Wang.
     * 
     * @param value
     * @return
     */
    public Long run(long value) {  
        Long key = (value+hashSeed);
        key = (~key) + (key << 21);
        key = key ^ (key >>> 24);
        key = (key + (key << 3)) + (key << 8); 
        key = key ^ (key >>> 14);
        key = (key + (key << 2)) + (key << 4); 
        key = key ^ (key >>> 28);
        key = key + (key << 31);
        return key;
    }   
    
    /** Seed value of hash. */
    private final Long hashSeed;
}
