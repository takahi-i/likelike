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
import java.util.Map;

import javax.swing.text.html.MinimalHTMLWriter;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.unigram.likelike.common.LikelikeConstants;
import org.unigram.likelike.lsh.function.IHashFunction;
import org.unigram.likelike.lsh.function.MinWiseFunction;

public class TestMinWiseFunction extends TestCase {

    public TestMinWiseFunction(String name) {
        super(name);
    }
    
    private Map<Long, Long> initMap(Long[] keys, Long[] values) {
        Map<Long, Long> rtMap = new HashMap<Long, Long>();
        for(int i=0;i<keys.length;i++) {
            rtMap.put(keys[i], values[i]);
        }
        return rtMap;
    }
    
    private IHashFunction createFunction(long hashSeed, int depth) {
        Configuration conf = new Configuration();        
        conf.setLong(MinWiseFunction.MINWISE_HASH_SEED, 1438L);
        conf.setInt(LikelikeConstants.FEATURE_DEPTH, depth);        
        return (IHashFunction) new MinWiseFunction(conf);
    }    
    
    public void testReturnHashDepth() {
        IHashFunction function = this.createFunction(1438L, 2);

        Long[] keys1   = {10L, 438L, 43L, 438L, 3489L};
        Long[] values1 = {1L,  1L,   1L,  1L,   1L};
        Map<Long, Long> map1 = this.initMap(keys1, values1);
        LongWritable hashValue1 = function.returnClusterId(map1);
        
        Long[] keys2   = {10L, 438L, 43L, 438L, 3489L};
        Long[] values2 = {1L,  1L,   1L,  1L,   1L};
        Map<Long, Long> map2 = this.initMap(keys2, values2);        
        LongWritable hashValue2 = function.returnClusterId(map2);
        
        Long[] keys3   = {11L, 439L};
        Long[] values3 = {1L,  1L };
        Map<Long, Long> map3 = this.initMap(keys1, values1);        
        LongWritable hashValue3 = function.returnClusterId(map2);        
        
        assertEquals(hashValue1,  hashValue2);
        assertFalse((hashValue1 == hashValue3));
        
        function = this.createFunction(1438L, 1);
        LongWritable hashValue1_dash = function.returnClusterId(map1);

        assertFalse(hashValue1_dash == hashValue1);
        
    }

    public void testReturnHashValue() {
        IHashFunction function = this.createFunction(1438L,1);

        Long[] keys1   = {10L, 438L, 43L, 438L, 3489L};
        Long[] values1 = {1L,  1L,   1L,  1L,   1L};
        Map<Long, Long> map1 = this.initMap(keys1, values1);
        LongWritable hashValue1 = function.returnClusterId(map1);
        
        Long[] keys2   = {10L, 438L, 43L, 438L, 3489L};
        Long[] values2 = {1L,  1L,   1L,  1L,   1L};
        Map<Long, Long> map2 = this.initMap(keys2, values2);        
        LongWritable hashValue2 = function.returnClusterId(map2);
        
        Long[] keys3   = {11L, 439L};
        Long[] values3 = {1L,  1L };
        Map<Long, Long> map3 = this.initMap(keys1, values1);        
        LongWritable hashValue3 = function.returnClusterId(map2);        
        
        assertEquals(hashValue1,  hashValue2);
        assertFalse((hashValue1 == hashValue3));

        function = this.createFunction(243L,1);        
        LongWritable hashValue1_dash = function.returnClusterId(map1);
        LongWritable hashValue2_dash = function.returnClusterId(map2);
        
        assertFalse(hashValue1 
                == hashValue1_dash);

        assertFalse(hashValue2 
                == hashValue2_dash);        
    }

}
