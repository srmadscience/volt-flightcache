package flightcache.client;

import org.voltdb.client.Client;

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

import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.types.TimestampType;
import org.voltdb.voltutil.stats.SafeHistogramCache;

public class ListFlightCallback implements ProcedureCallback {

    SafeHistogramCache statsCache = SafeHistogramCache.getInstance();

    Client voltClient;

    final long startTime = statsCache.getMicrosecondStartTime();
    boolean buySomething;
    String passengerName;

    public ListFlightCallback(Client voltClient, boolean buySomething, String passengerName) {
        super();
        this.voltClient = voltClient;
        this.buySomething = buySomething;
        this.passengerName = passengerName;
    }

    @Override
    public void clientCallback(ClientResponse arg0) throws Exception {
        statsCache.reportLatencyMicros(TestClient.LIST_FLIGHT, startTime, "", TestClient.MICROSECOND_HISTOGRAM_SIZE);

        if (arg0.getStatus() != ClientResponse.SUCCESS) {
            TestClient.msg("Error Code " + arg0.getStatusString());
        } else {

            String flightId;
            String flightClass;
            TimestampType flightDate;
            double flightPrice;

            arg0.getResults()[0].advanceRow();

            if (buySomething && arg0.getResults()[0].getRowCount() > 0) {

                BuyFlightCallback bfcb = new BuyFlightCallback();

                flightId = arg0.getResults()[0].getString("flight_id");
                flightClass = arg0.getResults()[0].getString("ticket_class");
                flightDate = arg0.getResults()[0].getTimestampAsTimestamp("flight_date");
                flightPrice = arg0.getResults()[0].getDouble("base_price");

                voltClient.callProcedure(bfcb, "flight_sale.INSERT", flightId, flightDate, flightClass, flightPrice,
                        passengerName);

            }

            statsCache.reportSize(TestClient.RETURNED_FLIGHTS_LIST_SIZE, arg0.getResults()[0].getRowCount(),
                    arg0.getResults()[0].getString("dep_city") + " -> " + arg0.getResults()[0].getString("arr_city"),
                    TestClient.MAX_FLIGHTS_PER_CITY_PAIR);
        }

    }

}
