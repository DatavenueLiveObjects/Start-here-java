/*
 * Copyright (C) 2016 Orange
 *
 * This software is distributed under the terms and conditions of the 'BSD-3-Clause'
 * license which can be found in the file 'LICENSE.txt' in this package distribution
 * or at 'https://opensource.org/licenses/BSD-3-Clause'.
 */
package com.orange.liveobjects.samples.mqtt;

import com.google.gson.Gson;
import com.orange.liveobjects.samples.utils.DeviceCommand;
import com.orange.liveobjects.samples.utils.DeviceCommandResponse;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.util.HashMap;

/**
 * Device connects to LO and handles a single command, then disconnects.
 */
public class Sample_03_SimpleDeviceHandleCommand {

    /**
     * Basic "MqttCallback" that handles messages as JSON device commands,
     * and immediately respond.
     */
    public static class SimpleMqttCallback implements MqttCallbackExtended {

        private static final String TOPIC_FILTER = "dev/cmd";
        private final MqttClient mqttClient;
        private Gson gson = new Gson();
        private Integer counter = 0;

        public SimpleMqttCallback(MqttClient mqttClient) {
            this.mqttClient = mqttClient;
        }

        public void connectionLost(Throwable throwable) {
            System.out.println("Connection lost");
        }

        public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
            System.out.println("Received message (i.e. command) - " + mqttMessage);

            // parse message as command
            DeviceCommand command = gson.fromJson(new String(mqttMessage.getPayload()), DeviceCommand.class);
            System.out.println("received command: " + command);

            // return response
            final DeviceCommandResponse response = new DeviceCommandResponse();
            response.cid = command.cid;
            response.res = new HashMap<String, Object>();
            response.res.put("msg", "hello friend!");
            response.res.put("method", command.req);
            response.res.put("counter", this.counter++);

            new Thread(new Runnable() {
                public void run() {
                    try {
                        mqttClient.publish("dev/cmd/res", gson.toJson(response).getBytes(), 0, false);
                    } catch (MqttException me) {
                        System.out.println("reason " + me.getReasonCode());
                        System.out.println("msg " + me.getMessage());
                        System.out.println("loc " + me.getLocalizedMessage());
                        System.out.println("cause " + me.getCause());
                        System.out.println("excep " + me);
                        me.printStackTrace();
                    }
                }
            }).start();

        }

        public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
            System.out.println("Message delivered");
        }

        @Override
        public void connectComplete(boolean b, String s) {
            System.out.println("Connection is established");
            try {
                subscribeToTopic(mqttClient, TOPIC_FILTER);
            } catch (MqttException e) {
                System.out.println("Error during subscription");
                System.out.println(e.toString());
            }
        }

        private void subscribeToTopic(MqttClient mqttClient, String topicFilter) throws MqttException {
            // Subscribe to commands
            System.out.println("Awaiting for commands...");
            mqttClient.subscribe(topicFilter);
            System.out.println("Subscribed");

        }
    }

    public static void main(String[] args) throws InterruptedException {

        String API_KEY = "<<< REPLACE WITH valid API key value>>>"; // <-- REPLACE!

        String SERVER = "tcp://liveobjects.orange-business.com:1883";
        String DEVICE_URN = "urn:lo:nsid:sensor:XX56765";
        int KEEP_ALIVE_INTERVAL = 30;// Must be <= 50

        try {
            MqttClient mqttClient = new MqttClient(SERVER, DEVICE_URN, new MemoryPersistence());

            // register callback (to handle received commands
            mqttClient.setCallback(new SimpleMqttCallback(mqttClient));

            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setUserName("json+device"); // selecting mode "Device"
            connOpts.setPassword(API_KEY.toCharArray()); // passing API key value as password
            connOpts.setCleanSession(true);
            connOpts.setKeepAliveInterval(KEEP_ALIVE_INTERVAL);
            connOpts.setAutomaticReconnect(true);

            // Connection
            System.out.println("Connecting to broker: " + SERVER);
            mqttClient.connect(connOpts);
            System.out.println("Connected");

            // sleep 10 seconds
            Thread.sleep(10000L);

            // Disconnection
            mqttClient.disconnect();
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
