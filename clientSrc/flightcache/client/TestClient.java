/* This file is part of VoltDB.
 * Copyright (C) 2024 Volt Active Data Inc.
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

package flightcache.client;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Random;

import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.voltutil.stats.SafeHistogramCache;

public class TestClient {

    private static final int MESSAGE_INTERVAL_MILLISECONDS = 5000;
    public static final String LIST_FLIGHT = "List Flight";
    public static final String LOAD_FLIGHT = "Load Flight";
    public static final String BUY_FLIGHT = "Buy Flight";
    public static final int MICROSECOND_HISTOGRAM_SIZE = 30000;
    public static final String RETURNED_FLIGHTS_LIST_SIZE = "Returned Flights List Size";
    public static final int MAX_FLIGHTS_PER_CITY_PAIR = 200;

    public static void main(String[] args) {
        try {

            SafeHistogramCache statsCache = SafeHistogramCache.getInstance();
            Random r = new Random(42);

            msg(Arrays.toString(args));

            if (args.length != 5) {
                msg("Usage: hostnames filename transactions_per_millisecond duration_in_seconds chance_of_purchase");
                msg("e.g. : 10.13.1.21 /Users/dwrolfe/Desktop/EclipseWorkspace/volt-flightcache/inputdata/flights.txt  3  300 100");
                System.exit(1);
            }

            String hostnames = args[0];
            String filename = args[1];
            int tpMs = Integer.parseInt(args[2]);
            int durationSeconds = Integer.parseInt(args[3]);
            int chanceOfPurchase = Integer.parseInt(args[4]);

            Client c = connectVoltDB(hostnames);
            Client callbackClient = connectVoltDB(hostnames);

            loadFlights(filename, c, tpMs);

            VoltTable cityPairs = getCityPairs(c);
            msg(cityPairs.getRowCount() + " city pairs found");

            long currentMs = System.currentTimeMillis();
            long lastMessageMs = 0;

            int tpThisMs = 0;
            long tranCount = 0;

            final long endtimeMs = System.currentTimeMillis() + (durationSeconds * 1000);

            msg("Generating requests...");

            while (endtimeMs > System.currentTimeMillis()) {
                tranCount++;

                int pairId = r.nextInt(cityPairs.getRowCount());
                cityPairs.resetRowPosition();
                cityPairs.advanceRow();
                cityPairs.advanceToRow(pairId);

                boolean buySomething = false;

                if (r.nextInt(chanceOfPurchase) == 0) {
                    buySomething = true;
                }

                ListFlightCallback lfcb = new ListFlightCallback(callbackClient, buySomething, "John Smith #"+ r.nextInt(1000));

                c.callProcedure(lfcb, "CompoundListFlight", cityPairs.getString("dep_city"),
                        cityPairs.getString("arr_city"), cityPairs.getTimestampAsTimestamp("min_flight_date"));

                if (++tpThisMs >= tpMs) {

                    while (currentMs == System.currentTimeMillis()) {
                        Thread.sleep(0, 50000);

                    }

                    currentMs = System.currentTimeMillis();
                    tpThisMs = 0;
                }

                if (lastMessageMs + MESSAGE_INTERVAL_MILLISECONDS < System.currentTimeMillis()) {
                    msg("On transaction " + tranCount + " " + cityPairs.getString("dep_city") + " to "
                            + cityPairs.getString("arr_city") + " on "
                            + cityPairs.getTimestampAsTimestamp("min_flight_date"));
                    lastMessageMs = System.currentTimeMillis();
                }

            }

            final float actualTps = tranCount / durationSeconds;

            msg("TPS Requested/Actual=" + (tpMs * 1000) + "/" + actualTps);

            msg(statsCache.toStringShort());

            int maxUsedSize = statsCache.getSize(RETURNED_FLIGHTS_LIST_SIZE).getMaxUsedSize();
            msg("Busiest city pair is "
                    + statsCache.getSize(RETURNED_FLIGHTS_LIST_SIZE).getTheHistogramComment(maxUsedSize));

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static VoltTable getCityPairs(Client c) throws NoConnectionsException, IOException, ProcCallException {
        ClientResponse cr = c.callProcedure("@AdHoc", "SELECT * FROM city_pairs ORDER BY dep_city, arr_city;");
        return cr.getResults()[0];

    }

    private static void loadFlights(String flightFileName, Client c, int tpMs)
            throws IOException, ParseException, InterruptedException, ProcCallException {

        final String DateFormat = "yyyy-MM-dd HHmm";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(DateFormat);

        ClientResponse flightCountResponse = c.callProcedure("@AdHoc", "SELECT COUNT(*) how_many FROM flights;");

        flightCountResponse.getResults()[0].advanceRow();
        final long existingFlightCount = flightCountResponse.getResults()[0].getLong("HOW_MANY");

        if (existingFlightCount > 0) {
            msg(existingFlightCount + " flights found. No need to load flights...");
        } else {

            msg("No flights found. Loading flights from " + flightFileName);

            long currentMs = System.currentTimeMillis();
            long lastMessageMs = 0;
            int tpThisMs = 0;
            int rowCounter = 0;
            BufferedReader br = new BufferedReader(new FileReader(flightFileName));

            String line;
            while ((line = br.readLine()) != null) {

                rowCounter++;

                if (++tpThisMs >= tpMs) {

                    while (currentMs == System.currentTimeMillis()) {
                        Thread.sleep(0, 50000);

                    }

                    currentMs = System.currentTimeMillis();
                    tpThisMs = 0;
                }

                String[] lineElements = line.split(",");
                String fromCity, toCity, flightId, tailNumber;
                Date departureTime;

                if (lineElements.length == 6) {

                    tailNumber = lineElements[1].replace("\"", "").trim();
                    flightId = lineElements[2].replace("\"", "").trim();
                    fromCity = lineElements[3].replace("\"", "").trim();
                    toCity = lineElements[4].replace("\"", "").trim();
                    departureTime = simpleDateFormat.parse(
                            lineElements[0].replace("\"", "").trim() + " " + lineElements[5].replace("\"", "").trim());
                    LoadFlightCallback lfcb = new LoadFlightCallback();
                    c.callProcedure(lfcb, "CompoundUpsertFlight", fromCity, toCity, departureTime, flightId,
                            tailNumber);
                }

                if (lastMessageMs + MESSAGE_INTERVAL_MILLISECONDS < System.currentTimeMillis()) {
                    msg("On row " + rowCounter + ":" + line);
                    lastMessageMs = System.currentTimeMillis();

                }

            }

            br.close();
            c.drain();
            msg("Loaded " + rowCounter + " flights");
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
