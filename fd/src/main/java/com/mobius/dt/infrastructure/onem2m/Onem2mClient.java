package com.mobius.dt.infrastructure.onem2m;

import java.util.Map;

public interface Onem2mClient {

    void postSensorData(String region, int sensor, Map<String,Object> con); // /FD/{r}/Sensor{s}/data
}
