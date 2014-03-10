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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

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


        ((Closeable) shm).close();

    }

    private static long parseYYYYMMDD(String s) {
        try {
            return YYYYMMDD.parse(s).getTime();
        } catch (ParseException e) {
            throw new AssertionError(e);
        }
    }


}
