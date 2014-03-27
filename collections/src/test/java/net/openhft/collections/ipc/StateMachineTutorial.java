/*
 * Copyright 2014 Peter Lawrey
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
package net.openhft.collections.ipc;

import net.openhft.collections.SharedHashMap;
import net.openhft.collections.SharedHashMapBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 *
 */
public class StateMachineTutorial {
    private static final Logger LOGGER = LoggerFactory.getLogger(StateMachineTutorial.class);

        // *************************************************************************
    //
    // *************************************************************************

    public static void main(String[] args) throws Exception{
        SharedHashMap<Integer, StateMachineData> map = null;

        try {
            map = new SharedHashMapBuilder()
                .entries(8)
                .create(
                    new File(System.getProperty("java.io.tmpdir"),"hft-state-machine"),
                    Integer.class,
                    StateMachineData.class);

            if(args.length > 0) {
                if("0".equalsIgnoreCase(args[0])) {
                    StateMachineData smd =
                        map.acquireUsing(0, new StateMachineData());

                    StateMachineState st = smd.getState();
                    if(st == StateMachineState.STATE_0) {
                        //fire the first state change
                        smd.setStateData(0);
                        smd.setState(StateMachineState.STATE_0, StateMachineState.STATE_1);
                    }
                } else if("1".equalsIgnoreCase(args[0])) {
                    StateMachineProcessor.runProcessor(
                        map.acquireUsing(0,new StateMachineData()),
                        StateMachineState.STATE_1,
                        StateMachineState.STATE_1_WORKING,
                        StateMachineState.STATE_2);
                } else if("2".equalsIgnoreCase(args[0])) {
                    StateMachineProcessor.runProcessor(
                        map.acquireUsing(0,new StateMachineData()),
                        StateMachineState.STATE_2,
                        StateMachineState.STATE_2_WORKING,
                        StateMachineState.STATE_3);
                } else if("3".equalsIgnoreCase(args[0])) {
                    StateMachineProcessor.runProcessor(
                        map.acquireUsing(0,new StateMachineData()),
                        StateMachineState.STATE_3,
                        StateMachineState.STATE_3_WORKING,
                        StateMachineState.STATE_1);
                }
            }
        } finally {
            if(map != null) {
                map.close();
            }
        }
    }
}
