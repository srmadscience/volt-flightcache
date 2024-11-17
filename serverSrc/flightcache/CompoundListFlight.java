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
package flightcache;

import java.util.Date;

import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltCompoundProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.ClientResponse;

public class CompoundListFlight extends VoltCompoundProcedure {

    public static final byte PARAMETER_NULL_CODE = 8;
    public static final String PARAMETER_NULL_MESSAGE = "Parameter can not be null";

    static VoltLogger LOG = new VoltLogger("CompoundListFlight");

    String depCity, arrCity, flightId;
    Date flightDate;

    public long run(String depCity, String arrCity, Date flightDate) {

        this.depCity = depCity;
        this.arrCity = arrCity;
        this.flightDate = flightDate;

        if (depCity == null || arrCity == null || flightDate == null) {
            LOG.error(PARAMETER_NULL_CODE);
            this.setAppStatusCode(PARAMETER_NULL_CODE);
            this.setAppStatusString(PARAMETER_NULL_MESSAGE);
            abortProcedure(PARAMETER_NULL_MESSAGE);
        }

        // Build stages
        newStageList(this::getFlights).then(this::getFlightDetails).then(this::finish).build();
        return 0L;
    }

    private void getFlights(ClientResponse[] unused) {

        queueProcedureCall("GET_FLIGHTS_FOR_CITY_PAIR", depCity, arrCity, flightDate, flightDate);
    }

    private void getFlightDetails(ClientResponse[] flightListResult) {

        if (flightListResult[0].getStatus() != ClientResponse.SUCCESS) {
            LOG.error("getFlightDetails:"+flightListResult[0].getStatusString());
            this.setAppStatusCode(flightListResult[0].getStatus());
            this.setAppStatusString(flightListResult[0].getAppStatusString());
            completeProcedure(-1L);
        }

        while (flightListResult[0].getResults()[0].advanceRow() && flightListResult[0].getResults()[0].getActiveRowIndex() < 10) {

            queueProcedureCall("GET_FLIGHT_DETAILS", flightListResult[0].getResults()[0].getString("FLIGHT_ID"),
                    flightDate,flightDate);

        }

    }

    private void finish(ClientResponse[] resp) {

        for (ClientResponse element : resp) {
            if (element.getStatus() != ClientResponse.SUCCESS) {
                LOG.error("finish:"+element.getStatusString());
                this.setAppStatusCode(element.getStatus());
                this.setAppStatusString(element.getAppStatusString());
                completeProcedure(-1L);
            }
        }

        VoltTable t = new VoltTable(new VoltTable.ColumnInfo("flight_id", VoltType.STRING),
                new VoltTable.ColumnInfo("flight_date", VoltType.TIMESTAMP),
                new VoltTable.ColumnInfo("seat_count", VoltType.BIGINT),
                new VoltTable.ColumnInfo("ticket_class", VoltType.STRING),
                new VoltTable.ColumnInfo("base_price", VoltType.FLOAT),new VoltTable.ColumnInfo("dep_city", VoltType.STRING),new VoltTable.ColumnInfo("arr_city", VoltType.STRING));
        
        VoltTable[] voltTableArray = { t };

        for (ClientResponse element : resp) {
            while (element.getResults()[0].advanceRow()) {
                t.addRow(element.getResults()[0].getString("flight_id"),
                        element.getResults()[0].getTimestampAsTimestamp("flight_date"),
                        element.getResults()[0].getLong("seat_count"),
                        element.getResults()[0].getString("ticket_class"),
                        element.getResults()[0].getDouble("base_price"),
                        depCity,arrCity);
            }
        }

        //LOG.info(t.toFormattedString());
        completeProcedure(voltTableArray);

    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("CompoundListFlight [depCity=");
        builder.append(depCity);
        builder.append(", arrCity=");
        builder.append(arrCity);
        builder.append(", flightId=");
        builder.append(flightId);
        builder.append(", flightDate=");
        builder.append(flightDate);
        builder.append("]");
        return builder.toString();
    }



}
