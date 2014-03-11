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

package net.openhft.collections.fromdocs;

import net.openhft.collections.SharedHashMap;
import net.openhft.collections.SharedHashMapBuilder;
import net.openhft.lang.model.DataValueClasses;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import static org.junit.Assert.assertEquals;

/**
 * These code fragments will appear in an article on OpenHFT.  These tests to ensure that the examples compile and behave as expected.
 */
public class OpenJDKAndHashMapExamplesTest {
    private static final SimpleDateFormat YYYYMMDD = new SimpleDateFormat("yyyyMMdd");

    @Test
    public void bondExample() throws IOException {
        SharedHashMap<String, BondVOInterface> shm = new SharedHashMapBuilder()
                .generatedValueType(true)
                .entrySize(320)
                .create(
                        new File("/dev/shm/myBondPortfolioSHM"),
                        String.class,
                        BondVOInterface.class
                );

        BondVOInterface bondVO = DataValueClasses.newDirectReference(BondVOInterface.class);
        shm.acquireUsing("369604103", bondVO);
        bondVO.setIssueDate(parseYYYYMMDD("20130915"));
        bondVO.setMaturityDate(parseYYYYMMDD("20140915"));
        bondVO.setCoupon(5.0 / 100); // 5.0%

        BondVOInterface.MarketPx mpx930 = bondVO.getMarketPxIntraDayHistoryAt(0);
        mpx930.setAskPx(109.2);
        mpx930.setBidPx(106.9);

        BondVOInterface.MarketPx mpx1030 = bondVO.getMarketPxIntraDayHistoryAt(1);
        mpx1030.setAskPx(109.7);
        mpx1030.setBidPx(107.6);


        SharedHashMap<String, BondVOInterface> shmB = new SharedHashMapBuilder()
                .generatedValueType(true)
                .entrySize(320)
                .create(
                        new File("/dev/shm/myBondPortfolioSHM"),
                        String.class,
                        BondVOInterface.class
                );

        BondVOInterface bondVOB = shmB.get("369604103");
        assertEquals(5.0 / 100, bondVOB.getCoupon(), 0.0);

        BondVOInterface.MarketPx mpx930B = bondVOB.getMarketPxIntraDayHistoryAt(0);
        assertEquals(109.2, mpx930B.getAskPx(), 0.0);
        assertEquals(106.9, mpx930B.getBidPx(), 0.0);

        BondVOInterface.MarketPx mpx1030B = bondVOB.getMarketPxIntraDayHistoryAt(1);
        assertEquals(109.7, mpx1030B.getAskPx(), 0.0);
        assertEquals(107.6, mpx1030B.getBidPx(), 0.0);

        // cleanup.
        shm.close();
        shmB.close();
        new File("/dev/shm/myBondPortfolioSHM").delete();

    }

    private static long parseYYYYMMDD(String s) {
        try {
            return YYYYMMDD.parse(s).getTime();
        } catch (ParseException e) {
            throw new AssertionError(e);
        }
    }


}
