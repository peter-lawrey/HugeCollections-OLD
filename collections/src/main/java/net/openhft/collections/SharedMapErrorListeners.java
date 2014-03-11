/*
 * Copyright 2013 Peter Lawrey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.collections;

import java.util.logging.Level;
import java.util.logging.Logger;

public enum SharedMapErrorListeners implements SharedMapErrorListener {
    LOGGING {
        @Override
        public void onLockTimeout(long threadId) throws IllegalStateException {
            Logger logger = Logger.getLogger(getClass().getName());
            if (threadId > 1L << 32)
                logger.severe("Grabbing lock held by processId: " + (threadId >>> 33) + ", threadId: " + (threadId & 0xFFFFFFFFL));
            else
                logger.severe("Grabbing lock held by threadId: " + threadId);
        }

        @Override
        public void errorOnUnlock(IllegalMonitorStateException e) {
            Logger.getLogger(getClass().getName()).log(Level.SEVERE, "Failed to unlock as expected", e);
        }
    }, ERROR {
        @Override
        public void onLockTimeout(long threadId) throws IllegalStateException {
            throw new IllegalStateException("Unable to acquire lock held by threadId: " + threadId);
        }

        @Override
        public void errorOnUnlock(IllegalMonitorStateException e) {
            throw e;
        }
    }
}
