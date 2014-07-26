package net.openhft.collections.blackboard;

import java.io.File;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import net.openhft.collections.SharedHashMap;
import net.openhft.collections.SharedHashMapBuilder;
import net.openhft.collections.blackboard.SHMTest5.SHMTest5Data;

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
