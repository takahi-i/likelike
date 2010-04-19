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

import org.apache.hadoop.util.ProgramDriver;

import org.unigram.likelike.feature.FeatureExtraction;
import org.unigram.likelike.lsh.LSHRecommendations;
import org.unigram.likelike.validate.Validation;

public final class LikelikeDriver {

    /**
     * Call specific job to create one of related query dictionaries.
     *
     * @param argv arguments array
     */
    public static void main(final String[] argv) {
        int exitCode = -1;
        ProgramDriver pgd = new ProgramDriver();
        try {
            pgd.addClass("lsh", LSHRecommendations.class,
                    "create recommendations.");
            pgd.addClass("validate", Validation.class,
                    "validate the result recommended pairs " +
                    "with cosine similarity.");        
            pgd.addClass("featureExtraction", 
                    FeatureExtraction.class, "extract features");                    
            pgd.addClass("help", ShowHelp.class,  "Show usage.");
            pgd.addClass("version", ShowVersion.class,  "Show version.");            
            pgd.driver(argv);
            exitCode = 0;
        } catch (Throwable e) {
            e.printStackTrace();
        }
        System.exit(exitCode);
    }

    /**
     *  for safe.
     */
    private LikelikeDriver() {}
}
