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
package org.unigram.likelike.lsh
;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.unigram.likelike.common.Candidate;
import org.unigram.likelike.common.RelatedUsersWritable;

public class GetRecommendationsMapper extends
        Mapper<LongWritable, RelatedUsersWritable, LongWritable, Candidate> {
    
    @Override
    public final void map(final LongWritable key,
            final RelatedUsersWritable value, final Context context) 
    throws IOException, InterruptedException {
        List<LongWritable> relatedUsers = value.getRelatedUsers();
        
        //System.out.println("relatedUsers.size():" + relatedUsers.size());
        
        for (int i = 0; i < relatedUsers.size(); i++) {
            LongWritable targetId 
            = new LongWritable(relatedUsers.get(i).get());
            //System.out.println("targetId: " + targetId);            
            for (int j = 0; j < relatedUsers.size(); j++) {
                if (i == j) {
                    continue;
                }
                LongWritable candidateId 
                    = new LongWritable(relatedUsers.get(j).get());
            
                context.write(targetId, new Candidate(candidateId, 
                        new LongWritable(relatedUsers.size())));
            }
        }
    }
}
