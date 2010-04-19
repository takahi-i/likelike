package org.unigram.likelike;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.unigram.likelike.common.LikelikeConstants;

/**
 * Show help message.
 */
public class ShowHelp extends Configured implements Tool {

    /**
     * Show usage.
     * @param args arguments
     * @return 0
     * @throws Exception -
     */
    public int run(String[] args) throws Exception {
        System.out.print("Likelike: an LSH implementation on MapReduce\n");
        System.out.print("\n");        
        System.out.print("Copyright(C) 2010 Takahiko Ito\n");
        System.out.print("\n");
        System.out.print("Usage:\n");
        System.out.print("     hadoop jar likelike-" 
                + LikelikeConstants.VERSION 
                +"-examples.jar"
                + "org.unigram.likelike.LikelikeDriver "
                + "PROGRAM_NAME [OPTIONS]\n");
        System.out.print("\n");
        System.out.print("In order to get detailed OPTIONS:\n");
        System.out.print("     hadoop jar likelike-0.1-examples.jar"
                + "org.unigram.likelike.LikelikeDriver "
                + "PROGRAM_NAME -help\n");
        return 0;
    }
    
    /**
     * Constructor.
     */
    public ShowHelp() {
        // nothing to do.
    }
    
    /**
     * Main method.
     *
     * @param args argument strings which contain input and output files.
     * @throws Exception -
     */
    public static void main(final String[] args)
    throws Exception {
        int exitCode = ToolRunner.run(new ShowHelp(), args);
        System.exit(exitCode);
    }
}
