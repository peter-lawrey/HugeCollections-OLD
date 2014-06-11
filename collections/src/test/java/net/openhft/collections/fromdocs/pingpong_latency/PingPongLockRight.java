/*
 * Copyright 2014 Higher Frequency Trading
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

package net.openhft.collections.fromdocs.pingpong_latency;

import net.openhft.collections.SharedHashMap;
import net.openhft.collections.fromdocs.BondVOInterface;

import java.io.IOException;

import static net.openhft.collections.fromdocs.pingpong_latency.PingPongLockLeft.playPingPong;

public class PingPongLockRight {
    public static void main(String... ignored) throws IOException, InterruptedException {
        SharedHashMap<String, BondVOInterface> shm = PingPongCASLeft.acquireSHM();

        playPingPong(shm, 5, 4, false, "PingPongLocRIGHT");
    }
}