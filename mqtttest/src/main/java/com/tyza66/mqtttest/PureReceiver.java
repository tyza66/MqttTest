package com.tyza66.mqtttest;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class PureReceiver {
    public static void main(String[] args) throws MqttException {
        MemoryPersistence persistence = new MemoryPersistence();
        String broker = "tcp://127.0.0.1:1883";
        String clientId = "emqx_test";
        MqttClient client = null;
        client = new MqttClient(broker, clientId, persistence);

        // MQTT 连接选项
        MqttConnectOptions connOpts = new MqttConnectOptions();
        // 保留会话
        connOpts.setCleanSession(true);

        // 设置回调
        client.setCallback(new MqttCallback() {

            // 连接丢失时回调
            @Override
            public void connectionLost(Throwable cause) {
                // 连接丢失后，一般在这里面进行重连
                System.out.println("连接断开，可以做重连");
            }

            // 接收消息时回调
            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
                // subscribe后得到的消息会执行到这里面
                System.out.println("接收消息主题:" + topic);
                System.out.println("接收消息Qos:" + message.getQos());
                System.out.println("接收消息内容:" + new String(message.getPayload()));
            }

            // 消息发送成功后回调
            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
                System.out.println("发送成功" + token.isComplete());
            }
        });

        System.out.println("Connecting to broker: " + broker);
        client.connect(connOpts);

        System.out.println("Connected");

        // 订阅
        client.subscribe("testtopic/#", 2);


//        client.disconnect();
//        System.out.println("Disconnected");
//        client.close();


    }
}
