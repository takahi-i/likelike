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
 * LikelikeException.
 */
public class LikelikeException extends Exception {

    /**
     * Default constructor.
     */
    public LikelikeException() {
    }

    /**
     * Constructor.
     * @param cause the exception information to be added
     */
    public LikelikeException(final Throwable cause) {
        super(cause);
    }

    /**
     * Constructor.
     * @param details detailed information of exception
     * @param cause the exception information to be added
     */
    public LikelikeException(final String details,
            final Throwable cause) {
        super(details, cause);
    }

    /**
     * Constructor.
     * @param details detailed information of exception 
     */
    public LikelikeException(final String details) {
        super(details);
    }
    
}
