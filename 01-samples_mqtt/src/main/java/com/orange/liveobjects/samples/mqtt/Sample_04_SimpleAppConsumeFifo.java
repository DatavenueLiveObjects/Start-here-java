/*
 * Copyright (C) 2016 Orange
 *
 * This software is distributed under the terms and conditions of the 'BSD-3-Clause'
 * license which can be found in the file 'LICENSE.txt' in this package distribution
 * or at 'https://opensource.org/licenses/BSD-3-Clause'.
 */
package com.orange.liveobjects.samples.mqtt;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.util.UUID;

/**
 * Application connects to LO in MQTT and consumes messages from a FIFO queue.
 *
 * Pre-requisites before running this sample :
 * - you MUST create a FIFO called "alarm" in your LO account (see https://liveobjects.orange-business.com/doc/html/lo_manual_v2.html#FIFO)
 * - you MUST create an action policy to route your messages to the "alarm" FIFO (see
 * https://liveobjects.orange-business.com/doc/html/lo_manual_v2.html#MESSAGE_ROUTING)
 *
 */
public class Sample_11_SimpleAppConsumeFifo {

    final static String TOPIC_FIFO = "fifo/alarm";

    /**
     * Basic "MqttCallback" that prints received messages
     */
    public static class SimpleMqttCallback implements MqttCallbackExtended {
        private MqttClient mqttClient;

        public SimpleMqttCallback(MqttClient mqttClient) {
            this.mqttClient = mqttClient;
        }

        public void connectionLost(Throwable throwable) {
            System.out.println("Connection lost");
        }

        public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
            System.out.println("Received message from FIFO queue - " + mqttMessage);
        }

        public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
            // nothing
        }

        public void connectComplete(boolean b, String s) {
            System.out.println("Connection is established");
            try {
                subscribeToFifo(mqttClient, TOPIC_FIFO);
            } catch (MqttException e) {
                System.out.println("Error during subscription");
            }
        }

        private void subscribeToFifo(MqttClient mqttClient, String routingKey) throws MqttException {
            // Subscribe to fifo
            System.out.printf("Consuming from Router with filter '%s'...%n", routingKey);
            mqttClient.subscribe(routingKey);
            System.out.println("... subscribed.");
        }
    }

    public static void main(String[] args) throws InterruptedException {

        String API_KEY = "<<< REPLACE WITH valid API key value with Application profile>>>"; // <-- REPLACE!

        String SERVER = "tcp://liveobjects.orange-business.com:1883";
        String APP_ID = "app:" + UUID.randomUUID().toString();
        int KEEP_ALIVE_INTERVAL = 30;// Must be <= 50

        MqttClient mqttClient = null;
        try {
            mqttClient = new MqttClient(SERVER, APP_ID, new MemoryPersistence());

            // register callback (to handle received commands
            mqttClient.setCallback(new SimpleMqttCallback(mqttClient));

            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setUserName("application"); // selecting "Application" mode
            connOpts.setPassword(API_KEY.toCharArray()); // passing API key value as password
            connOpts.setCleanSession(true);
            connOpts.setKeepAliveInterval(KEEP_ALIVE_INTERVAL);
            connOpts.setAutomaticReconnect(true);

            // Connection
            System.out.printf("Connecting to broker: %s ...%n", SERVER);
            mqttClient.connect(connOpts);
            System.out.println("... connected.");

            synchronized (mqttClient) {
                mqttClient.wait();
            }

        } catch (MqttException me) {
            me.printStackTrace();

        } finally {
            // close client
            if (mqttClient != null && mqttClient.isConnected()) {
                try {
                    mqttClient.disconnect();
                } catch (MqttException e) {
                    e.printStackTrace();
                }
            }
        }

    }

}
