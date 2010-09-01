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
package org.unigram.likelike;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.unigram.likelike.common.LikelikeConstants;

/**
 * 
 */
public class ShowVersion extends Configured implements Tool {

    /**
     * Show version.
     * @param arg0 dummy
     * @return 0 (dummy)
     * @throws Exception -
     */
    public int run(String[] arg0) throws Exception {
        System.out.println("Likelike version: " + LikelikeConstants.VERSION);
        return 0;
    }
    
    
    /**
     * Main method.
     *
     * @param args dummy
     * @throws Exception -
     */
    public static void main(final String[] args)
    throws Exception {
        int exitCode = ToolRunner.run(new ShowVersion(), args);
        System.exit(exitCode);
    }
}
