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
public abstract class LSHRecommendations extends
    Configured implements Tool {
    
    /** logger. */
    protected final LikelikeLogger logger 
        = LikelikeLogger.getLogger();

    /** random generator. */
    private final Random rand = new Random();
    
    /**
     * Run from ToolRunner.
     *  
     * @param args contains arguments
     * @return 0 when succeeded.
     * @throws IOException -
     * @throws InterruptedException -
     * @throws ClassNotFoundException -
     * @throws Exception -
     */
    public final int run(final String[] args) 
    throws IOException,
        InterruptedException, 
        ClassNotFoundException, Exception {
        this.setDefaultConfiguration();
        Configuration conf = getConf();
        return this.run(args, conf);
    }
    
    /**
     * Run.
     * @param args arguments
     * @param conf configuration
     * @return 0 when succeeded.
     * @throws Exception -
     */
    public int run(final String[] args, final Configuration conf) 
    throws Exception {

        String inputFile = "";
        String outputPrefix = "";
        String clusterDir = "";
        int iterate = 1;
        int depth   = 0;
        int rowSize = 0;

        for (int i = 0; i < args.length; ++i) {
            if ("-input".equals(args[i])) {
                inputFile = args[++i];
                clusterDir = inputFile + ".clusters";
            } else if ("-output".equals(args[i])) {
                outputPrefix = args[++i];
            } else if ("-depth".equals(args[i])) {
                conf.setInt(LikelikeConstants.FEATURE_DEPTH, 
                        Integer.parseInt(args[++i]));
            } else if ("-iterate".equals(args[i])) {
                iterate = Integer.parseInt(args[++i]);
            }  else if ("-maxCluster".equals(args[i])) {
                conf.setLong(LikelikeConstants.MAX_CLUSTER_SIZE, 
                        Long.parseLong(args[++i]));
            }  else if ("-minCluster".equals(args[i])) {
                conf.setLong(LikelikeConstants.MIN_CLUSTER_SIZE, 
                        Long.parseLong(args[++i]));
            } else if ("-maxRecommend".equals(args[i])) {
                conf.setLong(LikelikeConstants.MAX_OUTPUT_SIZE, 
                        Long.parseLong(args[++i]));
            } else if ("-help".equals(args[i])) {
                this.showParameters();
                return 0;
            } 
        }
        
        this.setHashKeys(iterate, inputFile, conf);
        this.extractClusters(inputFile, clusterDir, conf);
        this.getRecommendations(clusterDir, 
                outputPrefix, conf, FileSystem.get(conf));

        FsUtil.clean(FileSystem.get(conf), clusterDir);        
        return 0;
    }
  
    /**
     * Set hash keys into a string.
     * @param iterate the number of hash keys 
     * @param inputFile input 
     * @param conf configuration
     * @return string contains hash keys 
     * @throws IOException -
     */
    private String setHashKeys(final int iterate, 
            final String inputFile, final Configuration conf) 
    throws IOException {

        StringBuffer keysStrBuffer = new StringBuffer(); 
        for (int i =0; i < iterate; i++) {
            Long hashKey = this.rand.nextLong();
            keysStrBuffer.append(hashKey.toString() + ":");
        }
        conf.set(SelectClustersMapper.MINWISE_HASH_SEEDS, 
                keysStrBuffer.toString());
        
        String keysStr = keysStrBuffer.toString();
        this.saveKeys(keysStr, inputFile, conf);        
        return keysStr;
    }

    /**
     * Add the configuration information from the result of 
     * extract candidates to conf.
     * 
     * @param counters contains counter
     * @param conf configuration
     */
    protected void setResultConf(final Counters counters, 
            final Configuration conf) {
        conf.setLong(LikelikeConstants.LIKELIKE_INPUT_RECORDS, 
                counters.findCounter(
                        LikelikeConstants.COUNTER_GROUP, 
                   "MAP_INPUT_RECORDS").getValue());    
        this.logger.logInfo("The number of record is " 
                + conf.getLong(
                        LikelikeConstants.LIKELIKE_INPUT_RECORDS, -1));
    }

    /**
     * Save keys.
     * @param keys hash keys
     * @param inputFile input file
     * @param conf configuration
     * @throws IOException -
     */
    private void saveKeys(final String keys, 
            final String inputFile, final Configuration conf) 
    throws IOException {
        /* save to local fs */
        String tempKeyFile = new String("keys.tmp");
        try {
            FileOutputStream fos = new FileOutputStream(tempKeyFile);
            OutputStreamWriter osw = new OutputStreamWriter(fos , "UTF-8");
            BufferedWriter bw = new BufferedWriter(osw);
            bw.write(keys+"\n");
            bw.close();
            osw.close();
            fos.close();
        } catch (Exception e) {
          e.printStackTrace();
        }

        /* put local file to hdfs */
        FileSystem fs = FileSystem.get(conf);
        Path localKeyFilePath = new Path(tempKeyFile);
        Path hdfsKeyFilePath = new Path(inputFile + ".keys");
        fs.copyFromLocalFile(localKeyFilePath, hdfsKeyFilePath);
        
        /* remove local file*/
        fs.delete(localKeyFilePath, true);
        
        return;
    }

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
    protected abstract boolean getRecommendations(final String inputDir,
            final String outputFile, final Configuration conf, 
            final FileSystem fs) throws IOException, InterruptedException, 
            ClassNotFoundException; 

    /**
     * Extract clusters.
     * @param inputFile input 
     * @param clusterFile cluster files
     * @param conf configuration
     * @return 0 when succeeded
     * @throws IOException -
     * @throws InterruptedException -
     * @throws ClassNotFoundException -
     */
    private boolean extractClusters(final String inputFile, 
            final String clusterFile,
            final Configuration conf) throws IOException, 
            InterruptedException, ClassNotFoundException {

        Path inputPath = new Path(inputFile);
        Path outputPath = new Path(clusterFile);
        FsUtil.checkPath(outputPath, FileSystem.get(conf));

        Job job = new Job(conf);
        job.setJarByClass(LSHRecommendations.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setMapperClass(SelectClustersMapper.class);
        job.setCombinerClass(SelectClustersReducer.class);
        job.setReducerClass(SelectClustersReducer.class);
        job.setMapOutputKeyClass(SeedClusterId.class);
        job.setMapOutputValueClass(RelatedUsersWritable.class);
        job.setOutputKeyClass(SeedClusterId.class);
        job.setOutputValueClass(RelatedUsersWritable.class);
        job.setOutputFormatClass(
                SequenceFileOutputFormat.class);
        job.setNumReduceTasks(
                conf.getInt(LikelikeConstants.NUMBER_OF_REDUCES,
                LikelikeConstants.DEFAULT_NUMBER_OF_REDUCES));

        boolean result =  job.waitForCompletion(true);
        this.setResultConf(job.getCounters(), conf);        
        return result;
    }


    /**
     * Add configuration from xml files.
     */
    private void setDefaultConfiguration()  {
        Configuration.addDefaultResource("conf/likelike-default.xml");
        Configuration.addDefaultResource("conf/likelike-site.xml");
    }

    /**
     * Show parameters for FreqentNGramExtraction.
     */
    protected void showParameters() {
        System.out.println("Extract related (or similar) examples.");
        System.out.println("");             
        System.out.println("Paramters:");
        System.out.println("    -input    INPUT           " 
                + "use INPUT as input resource");
        System.out.println("    -output   OUTPUT          " 
                + "use OUTPUT as outupt prefix");
        System.out.println("    [-depth   DEPTH]          " 
                + "use DEPTH as size of concatinations (default 1)");        
        System.out.println("    [-iterate  ITERATE]       " 
                + "use ITERATE as the number of hash keys (default 1)");        
        System.out.println("    [-maxRecommend  SIZE]     " 
                + "use SIZE as the maximum number of recommendation "
                + "for one example");        
        System.out.println("    [-help]                   "
                + "show this message");
    }    
}
