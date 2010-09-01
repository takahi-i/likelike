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
import java.util.Set;
import java.util.TreeMap;

import javax.swing.plaf.basic.BasicTreeUI.TreeHomeAction;

import org.apache.hadoop.io.LongWritable;
import org.unigram.likelike.lsh.function.CalcHashValue;

import junit.framework.TestCase;

public class TestCalcHashValue extends TestCase {

    public TestCalcHashValue(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();
    }

    public void testRun() {
        CalcHashValue calcHash = new CalcHashValue(); 
        
        /* test collision */
        Map<Long, Long>resultMap = new HashMap<Long, Long>();
        for (int i = 0; i<100000; i++) {
            Long hashedValue = calcHash.run((long) i, 3349L);
            if (resultMap.containsKey(hashedValue)) {
                fail("Collision keys!");
                Long count = resultMap.get(hashedValue);
                resultMap.put(hashedValue, count+1);
            } else {
                resultMap.put(hashedValue, 1L);
            }
        }
        
        /* compare the rankings with different seeds */
        Set<Long>resultMapA = this.extractTopRanked(233L, 10000, 10); 
        Set<Long>resultMapB =  this.extractTopRanked(3L, 10000, 10);
        
        int collistionCount = 0;
        for (Long id : resultMapA) {
            if (resultMapB.contains(id)) {
                collistionCount += 1;
            }
        }
        assertTrue(collistionCount < 1);
    }

    
    private Set<Long> extractTopRanked(long hashSeed, 
            int size, int threshold) {
        CalcHashValue calcHash = new CalcHashValue();
        TreeMap<Long,Long> hashedValues = new TreeMap<Long,Long>();

        for (int i=0; i<size; i++) {
            hashedValues.put(
                    calcHash.run((long) i, hashSeed), 
                    (long) i);
        }
        
        Set<Long> rtSet = new HashSet<Long>();
        for (int i=0; i<threshold; i++) {
            Long hashedValue = hashedValues.lastKey();
            Long id = hashedValues.get(hashedValue);
            rtSet.add(id);
            hashedValues.remove(hashedValue);
        }
        return rtSet;
    }
    
}
