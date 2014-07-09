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

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;

import static net.openhft.collections.AbstractChannelReplicator.ChannelReplicatorBuilder;


public class UdpReplicatorBuilder implements ChannelReplicatorBuilder, Cloneable {

    private InetAddress address;
    private int port;
    private long throttle;

    private NetworkInterface interf;

    /**
     * @param port    udp port
     * @param address the UDP broadcast or multicast address Directed broadcast, <p/> <p/> for example a
     *                broadcast address of 192.168.0.255  has an IP range of 192.168.0.0 - 192.168.0.254 <p/>
     *                see  http://www.subnet-calculator.com/subnet.php?net_class=C for more details
     * @throws UnknownHostException
     */
    public UdpReplicatorBuilder(int port, InetAddress address) throws UnknownHostException {
        this.port = port;
        this.address = address;
    }


    public InetAddress address() {
        return address;
    }

    /**
     * @param address the UDP broadcast or multicast address, for example, for broadcast an address of
     *                192.168.0.255 has an IP range of 192.168.0.0 - 192.168.0.254 <p/> see
     *                http://www.subnet-calculator.com/subnet.php?net_class=C for more details
     */
    public UdpReplicatorBuilder address(InetAddress address) {
        this.address = address;
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
                "address='" + address + '\'' +
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


    public UdpReplicatorBuilder networkInterface(NetworkInterface interf) throws SocketException {
        if (interf == null) {
            StringBuilder builder = new StringBuilder();

            final Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            while (networkInterfaces.hasMoreElements()) {
                builder.append(networkInterfaces.nextElement().getName());

                if (networkInterfaces.hasMoreElements())
                    builder.append(", ");
            }

            throw new IllegalArgumentException("networkInterface can not be set to null. Please use one of " +
                    "the following: " + builder + "");
        }

        this.interf = interf;
        return this;
    }

    public NetworkInterface networkInterface() {
        return interf;
    }
}