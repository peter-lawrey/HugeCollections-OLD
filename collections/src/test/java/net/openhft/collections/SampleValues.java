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

package net.openhft.collections;/*
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

import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshallable;
import org.jetbrains.annotations.NotNull;

/**
 * Sample entry of 10 x 10 character strings.
 */
public class SampleValues implements BytesMarshallable {
    String aa = "aaaaaaaaaa";
    String bb = "bbbbbbbbbb";
    String cc = "cccccccccc";
    String dd = "ddddddddddd";
    String ee = "eeeeeeeeee";
    String ff = "ffffffffff";
    String gg = "gggggggggg";
    String hh = "hhhhhhhhhh";
    String ii = "iiiiiiiiii";
    String jj = "jjjjjjjjjj";
    String kk = "kkkkkkkkkk";

    @Override
    public void readMarshallable(@NotNull Bytes in) throws IllegalStateException {
        aa = in.readUTFΔ();
        bb = in.readUTFΔ();
        cc = in.readUTFΔ();
        dd = in.readUTFΔ();
        ee = in.readUTFΔ();
        ff = in.readUTFΔ();
        gg = in.readUTFΔ();
        hh = in.readUTFΔ();
        ii = in.readUTFΔ();
        jj = in.readUTFΔ();
        kk = in.readUTFΔ();
    }

    @Override
    public void writeMarshallable(@NotNull Bytes out) {
        out.writeUTFΔ(aa);
        out.writeUTFΔ(bb);
        out.writeUTFΔ(cc);
        out.writeUTFΔ(dd);
        out.writeUTFΔ(ee);
        out.writeUTFΔ(ff);
        out.writeUTFΔ(gg);
        out.writeUTFΔ(hh);
        out.writeUTFΔ(ii);
        out.writeUTFΔ(jj);
        out.writeUTFΔ(kk);
    }
}
