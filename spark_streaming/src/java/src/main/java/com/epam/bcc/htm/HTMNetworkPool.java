package com.epam.bcc.htm;

import java.util.Map;
import java.util.HashMap;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;



public class HTMNetworkPool {
    // Holds HTMNetwork state for every deviceId
    private final Map<String, HTMNetwork> pool = new HashMap<>();

    // Return associated HTMNetwork object for given deviceId
    private HTMNetwork getNetwork(String deviceId) {
        if (pool.containsKey(deviceId)) {
            return pool.get(deviceId);
        } else {
            HTMNetwork network = new HTMNetwork(deviceId);
            pool.put(deviceId, network);
            return network;
        }
    };

    // Enrich given monitoring record
    public MonitoringRecord mappingFunc(String deviceId, MonitoringRecord record) throws Exception {
        HTMNetwork htmNetwork = this.getNetwork(deviceId);
        String stateDeviceID = htmNetwork.getId();

        if (!stateDeviceID.equals(deviceId))
            throw new Exception("Wrong behaviour of Spark: stream key is $deviceId%s, while the actual state key is $stateDeviceID%s");

        // get the value of DT and Measurement and pass it to the HTM
        HashMap<String, Object> m = new java.util.HashMap<>();
        m.put("DT", DateTime.parse(record.getDateGMT() + " " + record.getTimeGMT(), DateTimeFormat.forPattern("YY-MM-dd HH:mm")));
        m.put("Measurement", Double.parseDouble(record.getSampleMeasurement()));
        ResultState rs = htmNetwork.compute(m);
        record.setPrediction(rs.getPrediction());
        record.setError(rs.getError());
        record.setAnomaly(rs.getAnomaly());
        record.setPredictionNext(rs.getPredictionNext());

        return record;

    }

}