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

import java.net.UnknownHostException;


public class UdpReplicatorBuilder implements Cloneable {

    private String broadcastAddress;
    private int port;
    private long throttle;

    /**
     * @param port             udp port
     * @param broadcastAddress the UDP broadcast address Directed broadcast,
     *                         <p/>
     *                         <p/>
     *                         for example a broadcast address of 192.168.0.255  has an IP range of
     *                         192.168.0.0 - 192.168.0.254
     *                         <p/>
     *                         see  http://www.subnet-calculator.com/subnet.php?net_class=C for more details
     * @throws UnknownHostException
     */
    public UdpReplicatorBuilder(int port, String broadcastAddress) throws UnknownHostException {
        this.port = port;
        this.broadcastAddress = broadcastAddress;
    }


    public String broadcastAddress() {
        return broadcastAddress;
    }

    /**
     * @param broadcastAddress the UDP broadcast address Directed broadcast,
     *                         <p/>
     *                         <p/>
     *                         for example a broadcast address of 192.168.0.255  has an IP range of
     *                         192.168.0.0 - 192.168.0.254
     *                         <p/>
     *                         see  http://www.subnet-calculator.com/subnet.php?net_class=C for more details
     */
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

    /**
     * @return throttle bits per seconds
     */
    public long throttle() {
        return this.throttle;
    }


    /**
     * @param throttleInBitsPerSecond bits per seconds
     * @return this
     */
    public UdpReplicatorBuilder throttle(long throttleInBitsPerSecond) {
        this.throttle = throttleInBitsPerSecond;
        return this;
    }

    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    @Override
    public UdpReplicatorBuilder clone() {
        try {
            return (UdpReplicatorBuilder) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }

}