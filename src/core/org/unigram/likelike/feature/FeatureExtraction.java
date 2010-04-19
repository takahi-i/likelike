
package org.unigram.likelike.feature;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.unigram.likelike.common.FsUtil;
import org.unigram.likelike.common.LikelikeConstants;
import org.unigram.likelike.util.IdentityReducer;
import org.unigram.likelike.util.InverseMapper;
import org.unigram.likelike.util.AddFeatureMapper;
import org.unigram.likelike.util.AddFeatureReducer;

/**
 *
 */
public class FeatureExtraction  extends Configured 
    implements Tool {

    /**
     * run.
     * @param args arguments
     * @return 0 when succeeded
     * @throws Exception -
     */
    @Override
    public int run(final String[] args) throws Exception {
        Configuration conf = getConf();
        return this.run(args, conf);   
    }

    /**
     * run.
     * @param args arguments
     * @param conf configuration
     * @return 0 when succeeded
     * @throws IOException -
     * @throws InterruptedException -
     * @throws ClassNotFoundException -
     */
    public int run(final String[] args, final Configuration conf) 
        throws IOException, InterruptedException, ClassNotFoundException {
        
        String recommendDir = "";
        String inversedDir = "";
        String addedFeatureDir = "";
        String featureDir = "";
        String outputDir = "";
        String tmpOutputDir = "";

        this.fs = FileSystem.get(conf);

        for (int i = 0; i < args.length; ++i) {
            if ("-recommend".equals(args[i])) {
                recommendDir = args[++i];
                inversedDir = recommendDir + ".inv";
                addedFeatureDir = recommendDir + ".feature";
            } else if ("-output".equals(args[i])) {
                outputDir = args[++i];
                tmpOutputDir = outputDir + ".tmp";
            } else if ("-feature".equals(args[i])) {
                featureDir = args[++i];
            } else if ("-help".equals(args[i])) {
                this.showParameters();
                return 0;
            } 
        }
        
        this.inverse(recommendDir, inversedDir, conf);
        this.addFeatures(inversedDir, addedFeatureDir, featureDir, conf);
        this.relatedFeatureExtraction(addedFeatureDir, 
                outputDir, featureDir, conf);
        FsUtil.clean(this.fs, addedFeatureDir, inversedDir);
        
        return 0;
    }        

    /**
     * Feature extraction for related ones.
     * 
     * @param addedFeatureDir input dir
     * @param outputDir output dir
     * @param featureDir feature dir 
     * @param conf configuration
     * @return 0 when succeeded
     * @throws IOException -
     * @throws InterruptedException -
     * @throws ClassNotFoundException -
     */
    private boolean relatedFeatureExtraction(final String addedFeatureDir,
            final String outputDir, final String featureDir, 
            final Configuration conf) 
           throws  IOException, InterruptedException, ClassNotFoundException {
        Path addedfeaturePath = new Path(addedFeatureDir);
        Path outputPath = new Path(outputDir);
        //Path featurePath = new Path(featureDir);
        FsUtil.checkPath(outputPath, this.fs);
        
        Job job = new Job(conf);
        job.setJarByClass(FeatureExtraction.class);
        FileInputFormat.addInputPath(job, addedfeaturePath);
        //FileInputFormat.addInputPath(job, featurePath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setMapperClass(FeatureExtractionMapper.class); 
        job.setReducerClass(FeatureExtractionReducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(
                conf.getInt(LikelikeConstants.NUMBER_OF_REDUCES,
                LikelikeConstants.DEFAULT_NUMBER_OF_REDUCES));

        return job.waitForCompletion(true);            
    }

    /**
     * inverse.
     * @param inputDir input dir
     * @param outputDir output dir
     * @param conf configuration
     * @return 0 when succeeded 
     * @throws IOException -
     * @throws InterruptedException -
     * @throws ClassNotFoundException -
     */
    private boolean inverse(final String inputDir, final String outputDir,
            final Configuration conf) 
    throws IOException, InterruptedException, ClassNotFoundException {
        Path inputPath = new Path(inputDir);
        Path outputPath = new Path(outputDir);
        FsUtil.checkPath(outputPath, this.fs);
        
        Job job = new Job(conf);
        job.setJarByClass(FeatureExtraction.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setMapperClass(InverseMapper.class); 
        job.setReducerClass(IdentityReducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(
                conf.getInt(LikelikeConstants.NUMBER_OF_REDUCES,
                LikelikeConstants.DEFAULT_NUMBER_OF_REDUCES));

        return job.waitForCompletion(true);
    }    

    /**
     * add features.
     * @param recommendDir inpu dir
     * @param outputFile output dir
     * @param featureDir feature dir
     * @param conf configure
     * @return 0 when succeeded
     * @throws IOException -
     * @throws InterruptedException -
     * @throws ClassNotFoundException -
     */
    private boolean addFeatures(final String recommendDir, 
            final String outputFile, final String featureDir,
            final Configuration conf) throws 
            IOException, InterruptedException, ClassNotFoundException {
        Path recommendPath = new Path(recommendDir);
        Path featurePath = new Path(featureDir);
        Path outputPath = new Path(outputFile);
        FsUtil.checkPath(outputPath, this.fs);

        Job job = new Job(conf);
        job.setJarByClass(FeatureExtraction.class);
        FileInputFormat.addInputPath(job, recommendPath);
        FileInputFormat.addInputPath(job, featurePath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setMapperClass(AddFeatureMapper.class); 
        job.setReducerClass(AddFeatureReducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setNumReduceTasks(
                conf.getInt(LikelikeConstants.NUMBER_OF_REDUCES,
                LikelikeConstants.DEFAULT_NUMBER_OF_REDUCES));

        return job.waitForCompletion(true);          
    }    
    
    /**
     * Show parameters for FreqentNGramExtraction.
     */
    private void showParameters() {
        System.out.println("Paramters:");
        System.out.println("    -input INPUT                " 
                + "use INPUT as input resource");
        System.out.println("    -output OUTPUT              " 
                + "use OUTPUT as outupt prefix");
        System.out.println("    [-help]                     "
                + "show usage");
    }
    
    /**
     * Main method.
     *
     * @param args argument strings which contain input and output files.
     * @throws Exception -
     */
    public static void main(final String[] args)
    throws Exception {
        int exitCode = ToolRunner.run(
                new FeatureExtraction(), args);
        System.exit(exitCode);
    }
    
    /** file system.  */
    private FileSystem fs = null;
    
}
