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
package org.unigram.likelike.lsh;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.unigram.likelike.common.Candidate;
import org.unigram.likelike.common.RelatedUsersWritable;
import org.unigram.likelike.common.SeedClusterId;

/**
 * Mapper. 
 */
public class GetRecommendationsMapper extends
        Mapper<SeedClusterId, RelatedUsersWritable, LongWritable, Candidate> {
    
    /**
     * Map method.
     * 
     * @param key dummy
     * @param value related users
     * @param context for writing
     * @throws IOException -
     * @throws InterruptedException -
     */
    @Override
    public final void map(final SeedClusterId key,
            final RelatedUsersWritable value, final Context context) 
    throws IOException, InterruptedException  {
        List<LongWritable> relatedUsers = value.getRelatedUsers();
        for (int targetId = 0; targetId < relatedUsers.size(); targetId++) {
            this.writeCandidates(targetId, relatedUsers, context);
        }
    }

    /**
     * write candidates.
     * 
     * @param targetIndex target id
     * @param relatedUsers related users
     * @param context -
     * @throws IOException -
     * @throws InterruptedException -
     */
    private void writeCandidates(final int targetIndex,
            final List<LongWritable> relatedUsers, final Context context) 
        throws IOException, InterruptedException {
        LongWritable targetId 
            = new LongWritable(relatedUsers.get(targetIndex).get());        
        for (int candidateIndex = 0; 
            candidateIndex < relatedUsers.size(); candidateIndex++) {
            if (targetIndex == candidateIndex) {
                continue;
            }
            LongWritable candidateId 
                = new LongWritable(relatedUsers.get(candidateIndex).get());
            context.write(targetId, new Candidate(candidateId, 
                    new LongWritable(relatedUsers.size())));
        }
    }
}
