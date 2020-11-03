/*
 * Copyright (C) 2016 Orange
 *
 * This software is distributed under the terms and conditions of the 'BSD-3-Clause'
 * license which can be found in the file 'LICENSE.txt' in this package distribution
 * or at 'https://opensource.org/licenses/BSD-3-Clause'.
 */
package com.orange.liveobjects.samples.mqtt;

import java.util.UUID;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

/**
 * Application connects to LO in MQTT and consumes messages from a FIFO queue.
 *
 * Pre-requisites before running this sample :
 * - you MUST create a FIFO called "alarm" in your LO account (see https://liveobjects.orange-business.com/doc/html/lo_manual_v2.html#FIFO)
 * - you MUST create an action policy to route your messages to the "alarm" FIFO (see
 * https://liveobjects.orange-business.com/doc/html/lo_manual_v2.html#MESSAGE_ROUTING)
 *
 */
public class Sample_04_SimpleAppConsumeFifo {

    final static String TOPIC_FIFO = "fifo/alarm";

    /**
     * Basic "MqttCallback" that prints received messages
     */
    public static class SimpleMqttCallback implements MqttCallbackExtended {
        private MqttClient mqttClient;

        public SimpleMqttCallback(MqttClient mqttClient) {
            this.mqttClient = mqttClient;
        }

        @Override
        public void connectionLost(Throwable throwable) {
            System.out.println("Connection lost");
        }

        @Override
        public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
            System.out.println("Received message from FIFO queue - " + mqttMessage);
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
            // nothing
        }

        @Override
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

        String SERVER = "ssl://liveobjects.orange-business.com:8883";
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
