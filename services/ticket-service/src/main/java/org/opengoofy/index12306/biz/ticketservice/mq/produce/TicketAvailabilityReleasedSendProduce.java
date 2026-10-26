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

package org.opengoofy.index12306.biz.ticketservice.mq.produce;

import org.apache.rocketmq.client.producer.SendResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.opengoofy.index12306.biz.ticketservice.mq.domain.MessageWrapper;
import org.opengoofy.index12306.biz.ticketservice.mq.event.TicketAvailabilityReleasedEvent;
import org.opengoofy.index12306.framework.starter.convention.exception.ServiceException;
import org.springframework.stereotype.Component;

import java.util.Objects;

import static org.opengoofy.index12306.biz.ticketservice.common.constant.TicketRocketMQConstant.TICKET_AVAILABILITY_RELEASED_TAG_KEY;
import static org.opengoofy.index12306.biz.ticketservice.common.constant.TicketRocketMQConstant.TICKET_AVAILABILITY_TOPIC_KEY;

/**
 * 余票释放事件生产者。
 * 负责将票务资源恢复事件发送到 RocketMQ，不负责候补订单分配。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TicketAvailabilityReleasedSendProduce {

    private final RocketMQTemplate rocketMQTemplate;

    public SendResult sendMessage(TicketAvailabilityReleasedEvent event) {
        MessageWrapper<TicketAvailabilityReleasedEvent> messageWrapper= new MessageWrapper<>();
        messageWrapper.setKeys(event.getEventId());
        messageWrapper.setMessage(event);

        String destination = TICKET_AVAILABILITY_TOPIC_KEY + ":" + TICKET_AVAILABILITY_RELEASED_TAG_KEY;

        SendResult sendResult = rocketMQTemplate.syncSend(destination,messageWrapper);

        if (!Objects.equals(sendResult.getSendStatus(), SendStatus.SEND_OK)) {
            throw new ServiceException("余票释放消息发送失败");
        }

        return sendResult;
    }
}
