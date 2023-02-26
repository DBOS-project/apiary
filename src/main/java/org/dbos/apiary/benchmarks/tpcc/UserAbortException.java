/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

package org.dbos.apiary.benchmarks.tpcc;

/**
 * Thrown from a Procedure to indicate to the Worker
 * that the procedure should be aborted and rolled back.
 */
public class UserAbortException extends RuntimeException {
    private static final long serialVersionUID = -1L;

    /**
     * Default Constructor
     * @param msg
     * @param ex
     */
    public UserAbortException(String msg, Throwable ex) {
        super(msg, ex);
    }

    /**
     * Constructs a new UserAbortException
     * with the specified detail message.
     */
    public UserAbortException(String msg) {
        this(msg, null);
    }
} // END CLASS