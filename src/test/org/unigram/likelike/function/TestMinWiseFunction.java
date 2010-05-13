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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;

import javax.swing.text.html.MinimalHTMLWriter;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.unigram.likelike.common.LikelikeConstants;
import org.unigram.likelike.lsh.SelectClustersMapper;
import org.unigram.likelike.lsh.function.IHashFunction;
import org.unigram.likelike.lsh.function.MinWiseFunction;

public class TestMinWiseFunction extends TestCase {

    public TestMinWiseFunction(String name) {
        super(name);
    }
    
    private Set<Long> createFeatureSet(Long[] keys) {
        Set<Long> rtMap = new HashSet<Long>();
        for(int i=0;i<keys.length;i++) {
            rtMap.add(keys[i]);
        }
        return rtMap;
    }
    
    private MinWiseFunction createFunction(int depth, long seed) {
        Configuration conf = new Configuration();        
        conf.setLong(SelectClustersMapper.MINWISE_HASH_SEEDS, seed);
        conf.setInt(LikelikeConstants.FEATURE_DEPTH, depth);        
        return new MinWiseFunction(conf);
    }
    
    public void testReturnHashDepth() {
        long seed = 1438L;
        IHashFunction function = this.createFunction(2, seed);
        
        Long[] keys1   = {10L, 438L, 43L, 438L, 3489L};
        Set<Long> set1 = this.createFeatureSet(keys1);
        LongWritable hashValue1 = function.returnClusterId(set1, seed);
        
        Long[] keys2   = {10L, 438L, 43L, 438L, 3489L};
        Set<Long> set2 = this.createFeatureSet(keys2);        
        LongWritable hashValue2 = function.returnClusterId(set2, seed);
        
        Long[] keys3   = {11L, 439L};
        Set<Long> set3 = this.createFeatureSet(keys1);  
        LongWritable hashValue3 = function.returnClusterId(set3, seed);        
        
        assertEquals(hashValue1, hashValue2);
        assertFalse((hashValue1 == hashValue3));
        
        function = this.createFunction(1, seed);
        long seed2 = 1438L; 
        LongWritable hashValue1_dash = function.returnClusterId(set1, seed2);

        assertFalse(hashValue1_dash == hashValue1);
        
    }

    public void testReturnHashValue() {
        final int depth = 1;
        long seed = 1438L;
        IHashFunction function = this.createFunction(depth, seed);

        
        Long[] keys1   = {10L, 438L, 43L, 438L, 3489L};
        Set<Long> set1 = this.createFeatureSet(keys1);
        LongWritable hashValue1 = function.returnClusterId(set1, seed);
        
        Long[] keys2   = {10L, 438L, 43L, 438L, 3489L};
        Set<Long> set2 = this.createFeatureSet(keys2);        
        LongWritable hashValue2 = function.returnClusterId(set2, seed);
        
        Long[] keys3   = {11L, 439L};
        Set<Long> set3 = this.createFeatureSet(keys1);        
        LongWritable hashValue3 = function.returnClusterId(set3, seed);        
        
        assertEquals(hashValue1, hashValue2);
        assertFalse((hashValue1 == hashValue3));

        function = this.createFunction(depth, seed);
        long seed2 = 243L;
        LongWritable hashValue1_dash = function.returnClusterId(set1, seed);
        LongWritable hashValue2_dash = function.returnClusterId(set2, seed);
        
        assertFalse(hashValue1 
                == hashValue1_dash);

        assertFalse(hashValue2 
                == hashValue2_dash);        
    }
    
    public void testCreateHashedVector() {
        Long[] features   = {10L, 438L, 43L, 438L, 3489L, 4355L, 37478L, 
                    323727L, 371L, 9302L, 30111L, 9702L, 63567623L, 1L, 573L};
        Set<Long> featureSet = this.createFeatureSet(features);
        
        Random rand = new Random();
        int collision = 0;
        int tryNum = 1000;
        for (int i = 0; i < tryNum; i++) {
            Long seed1 = rand.nextLong();
            MinWiseFunction function1 = this.createFunction(1, seed1);
            Long seed2 = rand.nextLong();
            MinWiseFunction function2 = this.createFunction(1, seed2);
        
            TreeMap<Long, Long> hashedFeatureVector1 
                = function1.createHashedVector(featureSet, seed1);
            TreeMap<Long, Long> hashedFeatureVector2 
                = function1.createHashedVector(featureSet, seed2);
            
            Entry<Long, Long> first1 = hashedFeatureVector1.firstEntry();
            Long minimumFeature1 = first1.getValue();
            
            Entry<Long, Long> first2 = hashedFeatureVector2.firstEntry();
            Long minimumFeature2 = first2.getValue();

            if (minimumFeature1.equals(minimumFeature2)) {
                collision += 1;
            }
        }        
        
        
        float collisionRate = (float) collision / (float) tryNum;
        System.out.println("collisionRate: " + collisionRate);
        assertTrue( collisionRate < 0.2);
        
    }
}
