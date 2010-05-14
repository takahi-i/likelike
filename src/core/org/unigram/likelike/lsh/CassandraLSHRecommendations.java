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

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.unigram.likelike.common.Candidate;
import org.unigram.likelike.common.FsUtil;
import org.unigram.likelike.common.LikelikeConstants;
import org.unigram.likelike.common.LikelikeLogger;
import org.unigram.likelike.common.RelatedUsersWritable;
import org.unigram.likelike.common.SeedClusterId;

/**
 * Extract recommendations for input examples. 
 */
public class CassandraLSHRecommendations extends
    LSHRecommendations { 

    /**
     * Get items to be recommended.
     * 
     * @param inputDir input 
     * @param outputFile output
     * @param conf configuration
     * @param fs file system
     * @return true when succeeded otherwise false
     * @throws IOException -
     * @throws InterruptedException -
     * @throws ClassNotFoundException -
     */
    @Override
    protected boolean getRecommendations(final String inputDir,
            final String outputFile, final Configuration conf, 
            final FileSystem fs) 
    throws IOException, InterruptedException, 
    ClassNotFoundException {
        this.logger.logInfo("Extracting recommendation to " + inputDir);
        Path inputPath = new Path(inputDir);
        Path outputPath = new Path(outputFile);
        FsUtil.checkPath(outputPath, FileSystem.get(conf));

        Job job = new Job(conf);
        job.setJarByClass(CassandraLSHRecommendations.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setMapperClass(GetRecommendationsMapper.class);
        job.setReducerClass(GetRecommendationsCassandraReducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Candidate.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setNumReduceTasks(conf.getInt(LikelikeConstants.NUMBER_OF_REDUCES,
                LikelikeConstants.DEFAULT_NUMBER_OF_REDUCES));

        return job.waitForCompletion(true);        
    }

    /**
     * Main method.
     *
     * @param args argument strings which contain input and output files
     * @throws Exception -
     */
    public static void main(final String[] args)
    throws Exception {
        int exitCode = ToolRunner.run(
                new CassandraLSHRecommendations(), args);
        System.exit(exitCode);
    }
    
}
