/*
 * Copyright (C) 2016 Orange
 *
 * This software is distributed under the terms and conditions of the 'BSD-3-Clause'
 * license which can be found in the file 'LICENSE.txt' in this package distribution
 * or at 'https://opensource.org/licenses/BSD-3-Clause'.
 */
package com.orange.liveobjects.samples.mqtt;

import com.google.gson.Gson;
import com.orange.liveobjects.samples.utils.DeviceData;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;

public class Sample_01_SimpleDevicePushData {

    public static void main(String[] args) {

        Random rand = new Random();
        String API_KEY = "<<< REPLACE WITH valid API key value with Mqtt Device profile>>>"; // <-- REPLACE!

        String SERVER = "tcp://liveobjects.orange-business.com:1883";
        String DEVICE_URN = "urn:lo:nsid:sensor:XX56765";
        int KEEP_ALIVE_INTERVAL = 30;// Must be <= 50

        // *** data to push ***
        DeviceData data = new DeviceData();
        // streamId
        data.s = "test";
        // value: JSON object...
        data.v = new HashMap<String, Object>();
        data.v.put("tempC", ((int) (rand.nextDouble() * 3000)) * 0.01 - 5);
        data.v.put("engineOn", rand.nextBoolean());
        // location (lat/lon)
        data.loc = new Double[] { 45.759723, 4.84223 };
        // model
        data.m = "sample01";
        // tags
        data.t = Arrays.asList("sample.01");
        // encoding to JSON
        String CONTENT = new Gson().toJson(data);

        try {
            MqttClient sampleClient = new MqttClient(SERVER, DEVICE_URN, new MemoryPersistence());
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setUserName("json+device"); // selecting "Device" mode
            connOpts.setPassword(API_KEY.toCharArray()); // passing API key value as password
            connOpts.setCleanSession(true);
            connOpts.setKeepAliveInterval(KEEP_ALIVE_INTERVAL);

            // Connection
            System.out.println("Connecting to broker: " + SERVER);
            sampleClient.connect(connOpts);
            System.out.println("Connected");

            // Publish data
            System.out.println("Publishing message: " + CONTENT);
            MqttMessage message = new MqttMessage(CONTENT.getBytes());
            message.setQos(0);
            sampleClient.publish("dev/data", message);
            System.out.println("Message published");

            // Disconnection
            sampleClient.disconnect();
            System.out.println("Disconnected");
            System.exit(0);

        } catch (MqttException me) {
            System.out.println("reason " + me.getReasonCode());
            System.out.println("msg " + me.getMessage());
            System.out.println("loc " + me.getLocalizedMessage());
            System.out.println("cause " + me.getCause());
            System.out.println("excep " + me);
            me.printStackTrace();
        }

    }

}
