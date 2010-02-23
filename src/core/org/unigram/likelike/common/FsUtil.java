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

import java.io.IOException;
import java.util.logging.Level;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * File utility class. 
 */
public final class FsUtil {
    
    /** logger. */
    private static LikelikeLogger logger =LikelikeLogger.getLogger();

    /**
     * for safe.
     */
    private FsUtil() {
        //dummy
    }

   /**
    * Check a file or directory exist. if so delete them.
    * @param dir path to be checked
    * @param fs filesystem containing the path
    * @return true when the check succeeded
    * @throws IOException occurs deleting file
    */
   public static boolean checkPath(final Path dir,
           final FileSystem fs)
   throws IOException {
        if (fs.exists(dir)) {
            logger.log(Level.INFO, "Overiding: " + dir.toString());
            return fs.delete(dir, true);
        } else {
            return true;
        }
    }
      
   /**
    * Check a file or directory exist. if so delete them.
    *
    * @param dir dir path to be checked
    * @param conf containing the filesystem of path
    * @return true when the check succeeded
    * @throws IOException when opening error such as there is no directory. 
    */
   public static boolean checkPath(final Path dir, 
           final Configuration conf)
   throws IOException {
       return checkPath(dir, FileSystem.get(conf));
    }
   
   /**
    * Delete files.  
    * 
    * @param fs filesytem containing files with fileNames
    * @param fileNames file names to be removed
    * @throws IOException
    */
   public static void clean(final FileSystem fs, 
       final String... fileNames) throws IOException {
       
       for (int i = 0; i < fileNames.length; i++) {
           Path path = new Path(fileNames[i]);
           if (fs.exists(path)) {
               logger.log(Level.INFO, 
                       "Removing: " + path.toString());                          
               fs.delete(path, true);
           }
       }
       return;
   }   

}
