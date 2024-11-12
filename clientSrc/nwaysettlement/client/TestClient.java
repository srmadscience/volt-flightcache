/* This file is part of VoltDB.
 * Copyright (C) 2022 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package nwaysettlement.client;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Random;

import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.NullCallback;
import org.voltdb.client.ProcCallException;
import org.voltdb.voltutil.stats.SafeHistogramCache;
import org.voltdb.voltutil.stats.StatsHistogram;

public class TestClient {

    public static void main(String[] args) {
        try {

            msg(Arrays.toString(args));
            SafeHistogramCache statsCache = SafeHistogramCache.getInstance();

            Client c = connectVoltDB(args[0]);
            ComplainOnErrorCallback coec = new ComplainOnErrorCallback();

            final int AIRPORT_COUNT = 10;
            final int FLIGHTS_PER_DAY = 10;
            final long TODAY = System.currentTimeMillis();
            final long MS_PER_DAY = 1000 * 60 * 60 * 24;
            long flightId = 1;

            for (long d = TODAY; d < TODAY + (MS_PER_DAY * 7); d += MS_PER_DAY) {
                for (int flightsPerDay = 0; flightsPerDay < FLIGHTS_PER_DAY; flightsPerDay++) {
                    
                    for (int i = 0; i < AIRPORT_COUNT; i++) {
                        for (int j = 0; j < AIRPORT_COUNT; j++) {

                            if (i != j) {
                                c.callProcedure(coec,"CompoundUpsertFlight", "A" + i, "A" + j, new Date(d), flightId++);
                            }

                        }
                    }
                }
            }
            
            
            msg("Waiting 10 seconds");
            Thread.sleep(10000);
            
            final long queryCount = 1000000;
            
            Random r = new Random(42);
            
            for (int i=0; i < queryCount; i++) {
                
                String depCity = "A" + r.nextInt(AIRPORT_COUNT);
                String arrCity = "A" + r.nextInt(AIRPORT_COUNT);
                ClientResponse cr = c.callProcedure("CompoundListFlight",depCity,arrCity, new Date(TODAY));
                
                
                if (i % 10000 == 0) {
                    msg("From " + depCity + " to " + arrCity +":");
                    msg(cr.getResults()[0].toFormattedString());
                }
               
                
                
            }

          

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

  

    public static Client connectVoltDB(String commaDelimitedHostnames) throws Exception {
        Client client = null;
        ClientConfig config = null;

        try {
            msg("Logging into VoltDB");

            config = new ClientConfig(); // "admin", "idontknow");
            config.setTopologyChangeAware(true);

            client = ClientFactory.createClient(config);

            String[] hostnameArray = commaDelimitedHostnames.split(",");

            for (String element : hostnameArray) {
                msg("Connect to " + element + "...");
                try {
                    client.createConnection(element);
                } catch (Exception e) {
                    msg(e.getMessage());
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("VoltDB connection failed.." + e.getMessage(), e);
        }

        return client;

    }

    /**
     * Print a formatted message.
     *
     * @param message
     */
    public static void msg(String message) {

        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date now = new Date();
        String strDate = sdfDate.format(now);
        System.out.println(strDate + ":" + message);

    }
}
