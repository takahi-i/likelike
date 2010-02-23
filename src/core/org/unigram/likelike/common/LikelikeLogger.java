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

import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

final public class LikelikeLogger extends Logger {

    /** logger (Singleton). */
    private static LikelikeLogger defaultLogger =
        LikelikeLogger.getLogger("likelike");

    /**
     * Constructor.
     * @param name logger instance name
     */
    private LikelikeLogger(final String name) {
        super(name, null);
    }

    /**
     * Constructor.
     * @param name logger instance name
     * @param resourceBundleName resource bundle name
     */
    private LikelikeLogger(final String name,
           final  String resourceBundleName) {
        super(name, resourceBundleName);
    }

    /**
     * Return LikelikeLogger object.
     * @param name name of logger instance
     * @return Logger which have assigned name
     */
    public static LikelikeLogger getLogger(final String name) {
        LogManager logManager = LogManager.getLogManager();
        Logger jdkLogger = logManager.getLogger(name);
        LikelikeLogger likelikeLogger;
        
        if (jdkLogger instanceof LikelikeLogger) {
            likelikeLogger = (LikelikeLogger) jdkLogger;
        } else if (jdkLogger == null) {
            likelikeLogger = new LikelikeLogger(name, (String) null);
            logManager.addLogger(likelikeLogger);
            likelikeLogger = (LikelikeLogger) logManager.getLogger(name);
        } else { // logger is not LikelikeLogger
            likelikeLogger = new LikelikeLogger(name, (String) null);
        }
        
        return likelikeLogger;
    }

    /**
     * Return logger objects reference.
     * @return LikelikeLogger
     */
    public static LikelikeLogger getLogger() {
        return defaultLogger;
    }
    
    /**
     * Flush msg as Severe Information. 
     * @param msg message to be flushed by logger 
     */
    public void logSevere(String msg) {
        this.log(Level.SEVERE, msg);
    }
    
    /**
     * Flush msg as log information.
     * @param msg message to be flushed by logger 
     */
    public void logInfo(String msg) {
        this.log(Level.INFO, msg);
    }    
    
    /**
     * Flush msg as warning.
     * @param msg message to be flushed by logger 
     */
    public void logWarning(String msg) {
        this.log(Level.WARNING, msg);
    }  
    
    /**
     * Flush msg for configuration information.
     * @param msg message to be flushed by logger 
     */
    public void logConfig(String msg) {
        this.log(Level.CONFIG, msg);
    }      
    
    /**
     * Flush msg to tell the state is fine.
     * @param msg message to be flushed by logger 
     */
    public void logFine(String msg) {
        this.log(Level.FINE, msg);
    }      
    
}
