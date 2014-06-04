/*
 * Copyright 2014 Higher Frequency Trading
 * <p/>
 * http://www.higherfrequencytrading.com
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;


public class UdpReplicatorBuilder implements Cloneable {

    private static final Logger LOG = LoggerFactory.getLogger(UdpReplicatorBuilder.class.getName());

    private String broadcastAddress;
    private int port;
    private long throttle;

    /**
     * @param port     udp port
     * @param throttle bits per seconds
     * @throws UnknownHostException
     */
    public UdpReplicatorBuilder(int port, long throttle) throws UnknownHostException {
        this.port = port;
        broadcastAddress = "255.255.255.255";
        this.throttle = throttle;
    }


    public String broadcastAddress() {
        return broadcastAddress;
    }

    public UdpReplicatorBuilder broadcastAddress(String broadcastAddress) {
        this.broadcastAddress = broadcastAddress;
        return this;
    }

    public int port() {
        return port;
    }

    public UdpReplicatorBuilder port(int port) {
        this.port = port;
        return this;
    }

    @Override
    public String toString() {
        return "UdpReplication{" +
                "broadcastAddress='" + broadcastAddress + '\'' +
                ", port=" + port + '}';
    }

    public long throttle() {
        return this.throttle;
    }


    /**
     * @param throttle bits per seconds
     * @return this
     */
    public UdpReplicatorBuilder throttle(long throttle) {
        this.throttle = throttle;
        return this;
    }

    @Override
    public UdpReplicatorBuilder clone() {
        try {
            final UdpReplicatorBuilder result = (UdpReplicatorBuilder) super.clone();
            result.broadcastAddress(this.broadcastAddress());
            result.port(this.port());
            result.broadcastAddress(this.broadcastAddress());
            result.throttle(this.throttle());
            return result;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }


    enum Unit {

        BITS {
            public long toBits(long v) {
                return v;
            }

            public long toMegaBits(long v) {
                return v / _MB;
            }

            public long toGigaBits(long v) {
                return v / _GB;
            }
        },

        MEGA_BITS {
            public long toBits(long v) {
                return v * _MB;
            }

            public long toMegaBits(long v) {
                return v;
            }

            public long toGigaBits(long v) {
                return (v * _MB) / _GB;
            }
        },

        GIGA_BITS {
            public long toBits(long v) {
                return v * _GB;
            }

            public long toMegaBits(long v) {
                return (v * _GB) / _MB;
            }

            public long toGigaBits(long v) {
                return v;
            }
        };


        public long toBits(long duration) {
            throw new AbstractMethodError();
        }

        long toMegaBits(long value) {
            throw new AbstractMethodError();
        }


        public long toGigaBits(long v) {
            throw new AbstractMethodError();
        }

        static long _MB = 52428800;
        static long _GB = 1073741824;

    }
}