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

package net.openhft.collections.blackboard;

import net.openhft.collections.SharedHashMap;
import net.openhft.collections.SharedHashMapBuilder;

import java.io.File;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Anshul Shelley
 */

public class SHMTestIterator1 
{
	public static void main(String[] args) throws Exception 
	{
		AtomicLong alValue = new AtomicLong();
		AtomicLong alKey = new AtomicLong();
		int runs = 3000000;
        SharedHashMapBuilder builder = new SharedHashMapBuilder().entries(runs);
        String shmPath = System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + "SHMTestIterator1";
        SharedHashMap<String, Long> shm = builder.create(new File(shmPath), String.class, Long.class);
        /*shm.put("k1", alValue.incrementAndGet());
        shm.put("k2", alValue.incrementAndGet());
        shm.put("k3", alValue.incrementAndGet());
        shm.put("k4", alValue.incrementAndGet());
        shm.put("k5", alValue.incrementAndGet());*/
        //shm.keySet();
                
        
        for (int i = 0; i < runs; i++) { 
        	shm.put("k"+alKey.incrementAndGet(), alValue.incrementAndGet());
        }
               
        long start = System.nanoTime();        
        for (Map.Entry<String, Long> entry : shm.entrySet())
        {   
        	entry.getKey();
        	entry.getValue();
            
        }
        long time = System.nanoTime() - start;
        System.out.println("Average iteration time was "+time / runs / 1e3+"us, for " + runs/1e6 +"m entries");

     
    }

  
}