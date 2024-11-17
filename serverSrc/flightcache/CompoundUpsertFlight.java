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
import org.voltdb.client.ClientResponse;

public class CompoundUpsertFlight extends VoltCompoundProcedure {

    public static final byte PARAMETER_NULL_CODE = 8;
    public static final String PARAMETER_NULL_MESSAGE = "Parameter can not be null";

    
    static VoltLogger LOG = new VoltLogger("CompoundUpsertFLight");
 
    String depCity, arrCity, flightId,tailNumber;
    Date flightDate;


    public long run(String depCity, String arrCity, Date flightDate, String flightId, String tailNumber) {

        // Save inputs
        this.depCity = depCity;
        this.arrCity = arrCity;
        this.flightId = flightId;
        this.flightDate = flightDate;
        this.tailNumber = tailNumber;

        if (depCity == null || arrCity == null || flightId == null || flightDate == null|| tailNumber == null) {
            LOG.error(PARAMETER_NULL_CODE);
            this.setAppStatusCode(PARAMETER_NULL_CODE);
            this.setAppStatusString(PARAMETER_NULL_MESSAGE);
            abortProcedure(PARAMETER_NULL_MESSAGE);
        }

        // Build stages
        newStageList(this::doUpserts).then(this::finish).build();
        return 0L;
    }

     private void doUpserts(ClientResponse[] unused) {

        queueProcedureCall("city_pair_flight.UPSERT", depCity, arrCity, flightDate, flightId,tailNumber );
        queueProcedureCall("flights.UPSERT", depCity, arrCity, flightDate, flightId,tailNumber );
        queueProcedureCall("flight_inventory.UPSERT", flightId,  flightDate, "K", 12, 100);
        queueProcedureCall("flight_inventory.UPSERT", flightId,  flightDate, "E", 50, 12);
        
       
    }

     private void finish(ClientResponse[] resp) {

        for (ClientResponse element : resp) {
            if (element.getStatus() != ClientResponse.SUCCESS) {
                LOG.error(element.getStatusString());
                this.setAppStatusCode(element.getStatus());
                this.setAppStatusString(element.getAppStatusString());
                completeProcedure(-1L);
            }
        }

        completeProcedure(0L);

    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("CompoundUpsertFLight [depCity=");
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
