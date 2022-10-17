/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.producer;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.RequestTimeoutException;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.hook.SendMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.failBecauseExceptionWasNotThrown;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.when;

/**
 * 生产者测试入口，发消息入口
 * MQClientAPIImpl才是真正和netty交互的类
 *
 * 执行发送类 MQClientAPIImpl -> NettyRemotingClient(初始化netty客户端) -> NettyRemotingAbstract
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultMQProducerTest {
    @Spy
    private MQClientInstance mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(new ClientConfig());
    @Mock
    private MQClientAPIImpl mQClientAPIImpl;
    @Mock
    private NettyRemotingClient nettyRemotingClient;

    private DefaultMQProducer producer;
    private Message message;
    private Message zeroMsg;
    private Message bigMessage;
    private String topic = "FooBar";
    private String producerGroupPrefix = "FooBar_PID";

    @Before
    public void init() throws Exception {
        String producerGroupTemp = producerGroupPrefix + System.currentTimeMillis();
        producer = new DefaultMQProducer(producerGroupTemp);
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.setCompressMsgBodyOverHowmuch(16);
        message = new Message(topic, new byte[] {'a'});
        zeroMsg = new Message(topic, new byte[] {});
        bigMessage = new Message(topic, "This is a very huge message!".getBytes());

        /**
         * Start 总体流程
         * 1 设置生产者组
         *
         * 2 检查当前状态，只允许为 CreateJust
         *
         * 3 从 MQClientManage 获取 MQClient 工厂
         *  3.1 已经被创建则直接返回
         *
         * 4 注册生产者组
         *
         * 5 启动 MQClient 工厂
         *
         *  5.1 NameSrv 地址为空时，尝试通过设定的地址使用HTTP获取NameSrv地址
         *  5.2 开启 Netty 的请求响应的 Channel
         *  5.3 开启调度任务
             5.3.1 从远程服务器不断更新 NameServer 地址
             5.3.2 定时从NameServer更新Topic的路由信息
             5.3.3 定期清除离线的Broker地址，同时发送心跳
             5.3.4 持久化所有的拥有的消费者偏移量
             5.3.5 动态对所有消费者的线程池容量进行调整
         *
         * 5.4 开启拉取服务
         *
         * 5.5 开启再均衡服务
         *
         * 5.6 开启push服务
         *
         * 启动 trace dispatcher 服务
         */
        producer.start();

        Field field = DefaultMQProducerImpl.class.getDeclaredField("mQClientFactory");
        field.setAccessible(true);
        field.set(producer.getDefaultMQProducerImpl(), mQClientFactory);

        field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
        field.setAccessible(true);
        field.set(mQClientFactory, mQClientAPIImpl);

        producer.getDefaultMQProducerImpl().getmQClientFactory().registerProducer(producerGroupTemp, producer.getDefaultMQProducerImpl());

        when(mQClientAPIImpl.sendMessage(anyString(), anyString(), any(Message.class), any(SendMessageRequestHeader.class), anyLong(), any(CommunicationMode.class),
            nullable(SendMessageContext.class), any(DefaultMQProducerImpl.class))).thenCallRealMethod();
        when(mQClientAPIImpl.sendMessage(anyString(), anyString(), any(Message.class), any(SendMessageRequestHeader.class), anyLong(), any(CommunicationMode.class),
            nullable(SendCallback.class), nullable(TopicPublishInfo.class), nullable(MQClientInstance.class), anyInt(), nullable(SendMessageContext.class), any(DefaultMQProducerImpl.class)))
            .thenReturn(createSendResult(SendStatus.SEND_OK));
    }

    @After
    public void terminate() {
        producer.shutdown();
    }

    @Test
    public void testSendMessage_ZeroMessage() throws InterruptedException, RemotingException, MQBrokerException {
        try {
            SendResult send = producer.send(zeroMsg);
            System.out.println(send);
            failBecauseExceptionWasNotThrown(MQClientException.class);
        } catch (MQClientException e) {
            assertThat(e).hasMessageContaining("message body length is zero");
        }
    }

    @Test
    public void testSendMessage_NoNameSrv() throws RemotingException, InterruptedException, MQBrokerException {
        when(mQClientAPIImpl.getNameServerAddressList()).thenReturn(new ArrayList<String>());
        try {
            producer.send(message);
            failBecauseExceptionWasNotThrown(MQClientException.class);
        } catch (MQClientException e) {
            e.printStackTrace();
            assertThat(e).hasMessageContaining("No name server address");
        }
    }

    @Test
    public void testSendMessage_NoRoute() throws RemotingException, InterruptedException, MQBrokerException {
        when(mQClientAPIImpl.getNameServerAddressList()).thenReturn(Collections.singletonList("127.0.0.1:9876"));
        try {
            producer.send(message);
            failBecauseExceptionWasNotThrown(MQClientException.class);
        } catch (MQClientException e) {
            e.printStackTrace();
            assertThat(e).hasMessageContaining("No route info of this topic");
        }
    }

    @Test
    public void testSendMessageSync_Success() throws RemotingException, InterruptedException, MQBrokerException, MQClientException {
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(createTopicRoute());
        SendResult sendResult = producer.send(message);
        System.out.println("发送结果：" + sendResult);

        assertThat(sendResult.getSendStatus()).isEqualTo(SendStatus.SEND_OK);
        assertThat(sendResult.getOffsetMsgId()).isEqualTo("123");
        assertThat(sendResult.getQueueOffset()).isEqualTo(456L);
    }

    @Test
    public void testSendMessageSync_WithBodyCompressed() throws RemotingException, InterruptedException, MQBrokerException, MQClientException {
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(createTopicRoute());
        SendResult sendResult = producer.send(bigMessage);

        assertThat(sendResult.getSendStatus()).isEqualTo(SendStatus.SEND_OK);
        assertThat(sendResult.getOffsetMsgId()).isEqualTo("123");
        assertThat(sendResult.getQueueOffset()).isEqualTo(456L);
    }

    @Test
    public void testSendMessageAsync_Success() throws RemotingException, InterruptedException, MQBrokerException, MQClientException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(createTopicRoute());
        /**
         * send 发送流程
         * 1 检查消息
         *
         * 2 从消息的目的 TopicName 中获取元信息；若获取不到 Topic，则抛异常
         *  2.1 从本地获取，没有则从 NameServer 获取
         *      2.1.1 从 NameServer 获取 Topic 元信息，没有则直接返回
         *      2.1.2 更新获取的 Topic 的路由信息
         *  2.2 将获取的 Topic 直接返回，若 NameServer 也没，则进行创建
         *      2.2.1 获取默认 Topic；获取失败直接返回
         *      2.2.2 继承该 Topic 的信息来进行更改以作为新 Topic
         *
         * 3 从 Topic 中选择 Queue
         *  3.1 排除掉在故障退避的 Broker 后，将下一个 Broker 所在的 Queue 返回
         *  3.2 所有 Broker 都需要退避下，选择次优 Broker
         *
         * 4 发送消息；失败则退回第三步
         *  4.1 Vip 检查
         *  4.2 消息类型检查
         *  4.3 调用钩子
         *  4.4 组装消息头
         *  4.5 发送消息
         *
         * 5 更新故障退避信息
         *
         * 6 根据发送方式返回结果
         */
        producer.send(message, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                assertThat(sendResult.getSendStatus()).isEqualTo(SendStatus.SEND_OK);
                assertThat(sendResult.getOffsetMsgId()).isEqualTo("123");
                assertThat(sendResult.getQueueOffset()).isEqualTo(456L);
                countDownLatch.countDown();
            }

            @Override
            public void onException(Throwable e) {
                countDownLatch.countDown();
            }
        });
        countDownLatch.await(3000L, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testSendMessageAsync() throws RemotingException, MQClientException, InterruptedException {
        final AtomicInteger cc = new AtomicInteger(0);
        final CountDownLatch countDownLatch = new CountDownLatch(6);

        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(createTopicRoute());
        SendCallback sendCallback = new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                countDownLatch.countDown();
            }

            @Override
            public void onException(Throwable e) {
                e.printStackTrace();
                cc.incrementAndGet();
                countDownLatch.countDown();
            }
        };
        MessageQueueSelector messageQueueSelector = new MessageQueueSelector() {
            @Override
            public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                return null;
            }
        };

        Message message = new Message();
        message.setTopic("test");
        message.setBody("hello world".getBytes());
        producer.send(new Message(), sendCallback);
        producer.send(message, new MessageQueue(), sendCallback);
        producer.send(new Message(), new MessageQueue(), sendCallback, 1000);
        producer.send(new Message(), messageQueueSelector, null, sendCallback);
        producer.send(message, messageQueueSelector, null, sendCallback, 1000);
        //this message is send success
        producer.send(message, sendCallback, 1000);

        countDownLatch.await(3000L, TimeUnit.MILLISECONDS);
        assertThat(cc.get()).isEqualTo(5);
    }
    
    @Test
    public void testBatchSendMessageAsync()
            throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        final AtomicInteger cc = new AtomicInteger(0);
        final CountDownLatch countDownLatch = new CountDownLatch(4);

        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(createTopicRoute());
        SendCallback sendCallback = new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                countDownLatch.countDown();
            }

            @Override
            public void onException(Throwable e) {
                e.printStackTrace();
                cc.incrementAndGet();
                countDownLatch.countDown();
            }
        };
        MessageQueueSelector messageQueueSelector = new MessageQueueSelector() {
            @Override
            public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                return null;
            }
        };

        List<Message> msgs = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Message message = new Message();
            message.setTopic("test");
            message.setBody(("hello world" + i).getBytes());
            msgs.add(message);
        }
        producer.send(msgs, sendCallback);
        producer.send(msgs, sendCallback, 1000);
        MessageQueue mq = new MessageQueue("test", "BrokerA", 1);
        producer.send(msgs, mq, sendCallback);
        // this message is send failed
        producer.send(msgs, new MessageQueue(), sendCallback, 1000);

        countDownLatch.await(3000L, TimeUnit.MILLISECONDS);
        assertThat(cc.get()).isEqualTo(1);
    }

    @Test
    public void testSendMessageAsync_BodyCompressed() throws RemotingException, InterruptedException, MQBrokerException, MQClientException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(createTopicRoute());
        producer.send(bigMessage, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                assertThat(sendResult.getSendStatus()).isEqualTo(SendStatus.SEND_OK);
                assertThat(sendResult.getOffsetMsgId()).isEqualTo("123");
                assertThat(sendResult.getQueueOffset()).isEqualTo(456L);
                countDownLatch.countDown();
            }

            @Override
            public void onException(Throwable e) {
            }
        });
        countDownLatch.await(3000L, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testSendMessageSync_SuccessWithHook() throws Throwable {
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(createTopicRoute());
        final Throwable[] assertionErrors = new Throwable[1];
        final CountDownLatch countDownLatch = new CountDownLatch(2);
        producer.getDefaultMQProducerImpl().registerSendMessageHook(new SendMessageHook() {
            @Override
            public String hookName() {
                return "TestHook";
            }

            @Override
            public void sendMessageBefore(final SendMessageContext context) {
                assertionErrors[0] = assertInOtherThread(new Runnable() {
                    @Override
                    public void run() {
                        assertThat(context.getMessage()).isEqualTo(message);
                        assertThat(context.getProducer()).isEqualTo(producer);
                        assertThat(context.getCommunicationMode()).isEqualTo(CommunicationMode.SYNC);
                        assertThat(context.getSendResult()).isNull();
                    }
                });
                countDownLatch.countDown();
            }

            @Override
            public void sendMessageAfter(final SendMessageContext context) {
                assertionErrors[0] = assertInOtherThread(new Runnable() {
                    @Override
                    public void run() {
                        assertThat(context.getMessage()).isEqualTo(message);
                        assertThat(context.getProducer()).isEqualTo(producer.getDefaultMQProducerImpl());
                        assertThat(context.getCommunicationMode()).isEqualTo(CommunicationMode.SYNC);
                        assertThat(context.getSendResult()).isNotNull();
                    }
                });
                countDownLatch.countDown();
            }
        });
        SendResult sendResult = producer.send(message);

        assertThat(sendResult.getSendStatus()).isEqualTo(SendStatus.SEND_OK);
        assertThat(sendResult.getOffsetMsgId()).isEqualTo("123");
        assertThat(sendResult.getQueueOffset()).isEqualTo(456L);

        countDownLatch.await();

        if (assertionErrors[0] != null) {
            throw assertionErrors[0];
        }
    }

    @Test
    public void testSetCallbackExecutor() throws MQClientException {
        String producerGroupTemp = "testSetCallbackExecutor_" + System.currentTimeMillis();
        producer = new DefaultMQProducer(producerGroupTemp);
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();

        ExecutorService customized = Executors.newCachedThreadPool();
        producer.setCallbackExecutor(customized);

        NettyRemotingClient remotingClient = (NettyRemotingClient) producer.getDefaultMQProducerImpl()
            .getmQClientFactory().getMQClientAPIImpl().getRemotingClient();

        assertThat(remotingClient.getCallbackExecutor()).isEqualTo(customized);
    }

    @Test
    public void testRequestMessage() throws RemotingException, RequestTimeoutException, MQClientException, InterruptedException, MQBrokerException {
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(createTopicRoute());
        final AtomicBoolean finish = new AtomicBoolean(false);
        new Thread(new Runnable() {
            @Override public void run() {
                ConcurrentHashMap<String, RequestResponseFuture> responseMap = RequestFutureTable.getRequestFutureTable();
                assertThat(responseMap).isNotNull();
                while (!finish.get()) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                    }
                    for (Map.Entry<String, RequestResponseFuture> entry : responseMap.entrySet()) {
                        RequestResponseFuture future = entry.getValue();
                        future.putResponseMessage(message);
                    }
                }
            }
        }).start();
        Message result = producer.request(message, 3 * 1000L);
        finish.getAndSet(true);
        assertThat(result.getTopic()).isEqualTo("FooBar");
        assertThat(result.getBody()).isEqualTo(new byte[] {'a'});
    }

    @Test(expected = RequestTimeoutException.class)
    public void testRequestMessage_RequestTimeoutException() throws RemotingException, RequestTimeoutException, MQClientException, InterruptedException, MQBrokerException {
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(createTopicRoute());
        Message result = producer.request(message, 3 * 1000L);
    }

    @Test
    public void testAsyncRequest_OnSuccess() throws Exception {
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(anyString(), anyLong())).thenReturn(createTopicRoute());
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        RequestCallback requestCallback = new RequestCallback() {
            @Override public void onSuccess(Message message) {
                assertThat(message.getTopic()).isEqualTo("FooBar");
                assertThat(message.getBody()).isEqualTo(new byte[] {'a'});
                assertThat(message.getFlag()).isEqualTo(1);
                countDownLatch.countDown();
            }

            @Override public void onException(Throwable e) {
            }
        };
        producer.request(message, requestCallback, 3 * 1000L);
        ConcurrentHashMap<String, RequestResponseFuture> responseMap = RequestFutureTable.getRequestFutureTable();
        assertThat(responseMap).isNotNull();
        for (Map.Entry<String, RequestResponseFuture> entry : responseMap.entrySet()) {
            RequestResponseFuture future = entry.getValue();
            future.setSendRequestOk(true);
            message.setFlag(1);
            future.getRequestCallback().onSuccess(message);
        }
        countDownLatch.await(3000L, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testAsyncRequest_OnException() throws Exception {
        final AtomicInteger cc = new AtomicInteger(0);
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        RequestCallback requestCallback = new RequestCallback() {
            @Override public void onSuccess(Message message) {

            }

            @Override public void onException(Throwable e) {
                cc.incrementAndGet();
                countDownLatch.countDown();
            }
        };
        MessageQueueSelector messageQueueSelector = new MessageQueueSelector() {
            @Override
            public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                return null;
            }
        };

        try {
            producer.request(message, requestCallback, 3 * 1000L);
            failBecauseExceptionWasNotThrown(Exception.class);
        } catch (Exception e) {
            ConcurrentHashMap<String, RequestResponseFuture> responseMap = RequestFutureTable.getRequestFutureTable();
            assertThat(responseMap).isNotNull();
            for (Map.Entry<String, RequestResponseFuture> entry : responseMap.entrySet()) {
                RequestResponseFuture future = entry.getValue();
                future.getRequestCallback().onException(e);
            }
        }
        countDownLatch.await(3000L, TimeUnit.MILLISECONDS);
        assertThat(cc.get()).isEqualTo(1);
    }

    public static TopicRouteData createTopicRoute() {
        TopicRouteData topicRouteData = new TopicRouteData();

        topicRouteData.setFilterServerTable(new HashMap<String, List<String>>());
        List<BrokerData> brokerDataList = new ArrayList<BrokerData>();
        BrokerData brokerData = new BrokerData();
        brokerData.setBrokerName("BrokerA");
        brokerData.setCluster("DefaultCluster");
        HashMap<Long, String> brokerAddrs = new HashMap<Long, String>();
        brokerAddrs.put(0L, "127.0.0.1:10911");
        brokerData.setBrokerAddrs(brokerAddrs);
        brokerDataList.add(brokerData);
        topicRouteData.setBrokerDatas(brokerDataList);

        List<QueueData> queueDataList = new ArrayList<QueueData>();
        QueueData queueData = new QueueData();
        queueData.setBrokerName("BrokerA");
        queueData.setPerm(6);
        queueData.setReadQueueNums(3);
        queueData.setWriteQueueNums(4);
        queueData.setTopicSysFlag(0);
        queueDataList.add(queueData);
        topicRouteData.setQueueDatas(queueDataList);
        return topicRouteData;
    }

    private SendResult createSendResult(SendStatus sendStatus) {
        SendResult sendResult = new SendResult();
        sendResult.setMsgId("123");
        sendResult.setOffsetMsgId("123");
        sendResult.setQueueOffset(456);
        sendResult.setSendStatus(sendStatus);
        sendResult.setRegionId("HZ");
        return sendResult;
    }

    private Throwable assertInOtherThread(final Runnable runnable) {
        final Throwable[] assertionErrors = new Throwable[1];
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    runnable.run();
                } catch (AssertionError e) {
                    assertionErrors[0] = e;
                }
            }
        });
        thread.start();
        try {
            thread.join();
        } catch (InterruptedException e) {
            assertionErrors[0] = e;
        }
        return assertionErrors[0];
    }
}
