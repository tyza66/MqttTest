package com.tyza66.mqtttest;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class Main {
    public static void main(String[] args) {
        // subTopic是订阅主题
        // pubTopic是发布主题
        // content是发布内容
        // qos是服务质量
        String subTopic = "testtopic/#"; // 这里的#是通配符，表示订阅所有testtopic下的主题
        String pubTopic = "testtopic/1";
        String content = "Hello World";
        int qos = 2; // 质量等级 0 1 2 表示最多一次、至少一次、只有一次
        // broker是MQTT服务器地址
        // clientId是客户端标识
        String broker = "tcp://127.0.0.1:1883";
        String clientId = "emqx_test";
        // MemoryPersistence设置clientid的保存形式，默认为以内存保存
        MemoryPersistence persistence = new MemoryPersistence();

        try {
            // 创建客户端
            MqttClient client = new MqttClient(broker, clientId, persistence);

            // MQTT 连接选项
            MqttConnectOptions connOpts = new MqttConnectOptions();
            // 如果设置了账号密码 需要填入账密
//            connOpts.setUserName("emqx_test");
//            connOpts.setPassword("emqx_test_password".toCharArray());
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
                    System.out.println("deliveryComplete---------" + token.isComplete());
                }
            });

            // 建立连接
            System.out.println("Connecting to broker: " + broker);
            client.connect(connOpts);

            System.out.println("Connected");

            // 订阅
            client.subscribe(subTopic);

            // 消息发布所需参数
            System.out.println("Publishing message: " + content);
            MqttMessage message = new MqttMessage(content.getBytes());
            message.setQos(qos);
            client.publish(pubTopic, message);
            System.out.println("Message published");

            client.disconnect();
            System.out.println("Disconnected");
            client.close();
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