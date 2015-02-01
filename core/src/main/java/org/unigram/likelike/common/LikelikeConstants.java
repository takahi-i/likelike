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
package org.unigram.likelike.common;

/**
 * Contains constans used in Likelike.
 *
 */
public final class LikelikeConstants {

    /** symbol: number of reducers. */
    public static final String NUMBER_OF_REDUCES
        = "likelike.reduces";

    /** default: number of reducers. */
    public static final int DEFAULT_NUMBER_OF_REDUCES = 1;

    /** symbol: depth. */
    public static final String FEATURE_DEPTH 
        = "likelike.feature.depth";    
    
    /** default: feature. */
    public static final int DEFAULT_FEATURE_DEPTH = 1;    
    
    /** symbol: hash function. */
    public static final String HASH_FUNCTION 
        = "likelike.hash.function";    
    
    /** default: default hash function. */
    public static final String DEFAULT_HASH_FUNCTION 
        = "org.unigram.likelike.lsh.function.MinWiseFunction";
    
    /** symbol: maximum cluster size. */
    public static final String MAX_CLUSTER_SIZE
        = "likelike.max.cluster.size";
    
    /** symbol: maximum number of recommendation per example. */
    public static final String MAX_OUTPUT_SIZE
        = "likelike.max.output.size";    
    
    /** default: maximum cluster size. */
    public static final long DEFAULT_MAX_CLUSTER_SIZE  = 300L; 
    
    /** default: maximum number of output for one example. */
    public static final long DEFAULT_MAX_OUTPUT_SIZE  = 10L;
    
    /** symbol: minimum cluster size. */
    public static final String MIN_CLUSTER_SIZE 
        = "likelike.min.cluster.size";
    
    /** default: minimum cluster size. */
    public static final long DEFAULT_MIN_CLUSTER_SIZE  = 1L;    
    
    /** symbol: logger. */
    public static final String LIKELIKE_LOGGER 
        = "likelike.logger";    

    /** symbol: counter group. */    
    public static final String COUNTER_GROUP
        = "org.apache.hadoop.mapred.Task$Counter";

    /** symbol: number of input records. */
    public static final String LIKELIKE_INPUT_RECORDS
        = "likelike.input.record";
    
    /** symbol: output type. */
    public static final String LIKELIKE_OUTPUT_WRITER
    = "likelike.output.writer";
    
    /** default: output type. */
    public static final String DEFAULT_LIKELIKE_OUTPUT_WRITER
    = "org.unigram.likelike.util.accessor.DFSWriter";
    
    /** symbol: cassandra columnfamily name. */
    public static final String CASSANDRA_COLUMNFAMILY_NAME
    = "likelike.cassandra.columnfamily.name";
    
    /** default: output table name. */
    public static final String DEFAULT_CASSANDRA_COLUMNFAMILY_NAME
    = "RelatedPairs";
    
    public static final String LIKELIKE_CASSANDRA_LSH_COLUMNFAMILY_NAME
    = "RelatedPairs";
    
    public static final String LIKELIKE_CASSANDRA_FEATURE_EXTRACTION_COLUMNFAMILY_NAME
    = "RecommendationItems";

    /** version. */
    public static final float VERSION = 0.3F;

    /** for safe. */
    private LikelikeConstants() {}
}
