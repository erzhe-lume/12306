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

package org.opengoofy.index12306.biz.ticketservice.mq.consumer;


import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.opengoofy.index12306.biz.ticketservice.common.constant.TicketRocketMQConstant;
import org.opengoofy.index12306.biz.ticketservice.common.enums.CandidateOrderStatusEnum;
import org.opengoofy.index12306.biz.ticketservice.dao.entity.CandidateOrderDO;
import org.opengoofy.index12306.biz.ticketservice.dao.entity.CandidateOrderItemDO;
import org.opengoofy.index12306.biz.ticketservice.dao.mapper.CandidateOrderItemMapper;
import org.opengoofy.index12306.biz.ticketservice.dao.mapper.CandidateOrderMapper;
import org.opengoofy.index12306.biz.ticketservice.mq.domain.MessageWrapper;
import org.opengoofy.index12306.biz.ticketservice.mq.event.TicketAvailabilityReleasedEvent;
import org.opengoofy.index12306.biz.ticketservice.service.TicketService;
import org.opengoofy.index12306.framework.starter.idempotent.annotation.Idempotent;
import org.opengoofy.index12306.framework.starter.idempotent.enums.IdempotentSceneEnum;
import org.opengoofy.index12306.framework.starter.idempotent.enums.IdempotentTypeEnum;
import org.springframework.stereotype.Component;


import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 余票释放事件消费者。
 * 接收释放事件，筛选匹配的 WAITING 候补订单，并驱动后续候补分配流程。
 */
@Slf4j
@Component
@RequiredArgsConstructor
@RocketMQMessageListener(
        topic = TicketRocketMQConstant.TICKET_AVAILABILITY_TOPIC_KEY,
        selectorExpression = TicketRocketMQConstant.TICKET_AVAILABILITY_RELEASED_TAG_KEY,
        consumerGroup = TicketRocketMQConstant.TICKET_CANDIDATE_ORDER_CG_KEY
)
public class TicketAvailabilityReleasedConsumer implements RocketMQListener<MessageWrapper<TicketAvailabilityReleasedEvent>> {
        private final CandidateOrderMapper candidateOrderMapper;
        private final CandidateOrderItemMapper candidateOrderItemMapper;
        private final TicketService ticketService;
        @Idempotent(
                uniqueKeyPrefix = "index12306-ticket:availability-released:",
                key = "#messageWrapper.getKeys()",
                type = IdempotentTypeEnum.SPEL,
                scene = IdempotentSceneEnum.MQ,
                keyTimeout = 7200L
        )
        @Override
        public void onMessage(MessageWrapper<TicketAvailabilityReleasedEvent> messageWrapper) {
            TicketAvailabilityReleasedEvent event = messageWrapper.getMessage();

            log.info(
                    "收到余票释放事件，eventId={}, trainId={}, releasedCount={}",
                    event.getEventId(),
                    event.getTrainId(),
                    event.getReleasedCount()
            );

            // 第一步：根据事件中的车次、区间和 WAITING 状态查询候补主表。
            List<CandidateOrderDO> candidateOrderList = candidateOrderMapper.selectList(Wrappers.lambdaQuery(CandidateOrderDO.class)
                    .eq(CandidateOrderDO::getTrainId, Long.valueOf(event.getTrainId()))
                    .eq(CandidateOrderDO::getDeparture, event.getDeparture())
                    .eq(CandidateOrderDO::getArrival, event.getArrival())
                    .eq(CandidateOrderDO::getStatus, CandidateOrderStatusEnum.WAITING.getCode())
                    .orderByAsc(CandidateOrderDO::getCreateTime)
                    .orderByAsc(CandidateOrderDO::getCandidateOrderSn)
            );

            if (candidateOrderList.isEmpty()) {
                log.info("主表为空，结束消费");
                return;
            }

            // 第二步：提取主表中的候补订单号，用于关联候补明细表。
            List<String> candidateOrderSnList = candidateOrderList.stream()
                    .map(CandidateOrderDO::getCandidateOrderSn)
                    .collect(Collectors.toList());

            // 第三步：只查询这些候补订单中，座位类型匹配且仍在等待的明细。
            List<CandidateOrderItemDO> candidateOrderItemList = candidateOrderItemMapper.selectList(Wrappers.lambdaQuery(CandidateOrderItemDO.class)
                    .in(CandidateOrderItemDO::getCandidateOrderSn, candidateOrderSnList)
                    .eq(CandidateOrderItemDO::getSeatType, event.getSeatType())
                    .eq(CandidateOrderItemDO::getStatus, CandidateOrderStatusEnum.WAITING.getCode())
            );

            if (candidateOrderItemList.isEmpty()) {
                log.info("没有匹配的候补明细，eventId={}", event.getEventId());
                return;
            }
            Map<String,List<CandidateOrderItemDO>> orderItemMap =candidateOrderItemList.stream()
                    .collect(Collectors.groupingBy(CandidateOrderItemDO::getCandidateOrderSn));

            CandidateOrderDO matchedCandidateOrder = null;

            for(CandidateOrderDO candidateOrder:candidateOrderList){
                List<CandidateOrderItemDO> itemList= orderItemMap.get(candidateOrder.getCandidateOrderSn());

                if(itemList==null||itemList.isEmpty()){
                    continue;
                }
                int requiredCount = itemList.size();

                if(requiredCount> event.getReleasedCount()){
                    continue;
                }
                matchedCandidateOrder = candidateOrder;

                log.info("找到可尝试分配的候补订单，candidateOrderSn={}", candidateOrder.getCandidateOrderSn());

                break;
            }
            if (matchedCandidateOrder == null) {
                log.info("没有候补订单可被本次余票满足，eventId={}", event.getEventId());
                return;
            }

             String candidateOrderSn = matchedCandidateOrder.getCandidateOrderSn();
            int affectedRows = candidateOrderMapper.update(null,Wrappers.lambdaUpdate(CandidateOrderDO.class)
                    .set(CandidateOrderDO::getStatus,CandidateOrderStatusEnum.ALLOCATING.getCode())
                    .set(CandidateOrderDO::getAllocateTime, new Date())
                     .eq(CandidateOrderDO::getCandidateOrderSn,candidateOrderSn)
                    .eq(CandidateOrderDO::getStatus,CandidateOrderStatusEnum.WAITING.getCode())
            );
            if(affectedRows==1){
                try{
                    ticketService.allocateCandidateOrder(candidateOrderSn);
                }catch (Throwable ex){
                    candidateOrderMapper.update(null, Wrappers.lambdaUpdate(CandidateOrderDO.class)
                            .set(CandidateOrderDO::getStatus, CandidateOrderStatusEnum.FAILED.getCode())
                            .set(CandidateOrderDO::getFailReason, "候补分配失败：" + ex.getMessage())
                            .eq(CandidateOrderDO::getCandidateOrderSn, candidateOrderSn)
                            .eq(CandidateOrderDO::getStatus, CandidateOrderStatusEnum.ALLOCATING.getCode())
                    );
                    log.error("候补订单分配失败，candidateOrderSn={}", candidateOrderSn, ex);
                    throw ex;
                }
            }else{
                 log.info("候补订单{}状态已变更，跳过分配",candidateOrderSn);
            }

        }

}
