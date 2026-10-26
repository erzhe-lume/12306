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

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson2.JSON;
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
import org.opengoofy.index12306.biz.ticketservice.dto.domain.RouteDTO;
import org.opengoofy.index12306.biz.ticketservice.dto.req.CancelTicketOrderReqDTO;
import org.opengoofy.index12306.biz.ticketservice.mq.domain.MessageWrapper;
import org.opengoofy.index12306.biz.ticketservice.mq.event.DelayCloseOrderEvent;
import org.opengoofy.index12306.biz.ticketservice.remote.TicketOrderRemoteService;
import org.opengoofy.index12306.biz.ticketservice.remote.dto.TicketOrderDetailRespDTO;
import org.opengoofy.index12306.biz.ticketservice.remote.dto.TicketOrderPassengerDetailRespDTO;
import org.opengoofy.index12306.biz.ticketservice.service.SeatService;
import org.opengoofy.index12306.biz.ticketservice.service.TrainStationService;
import org.opengoofy.index12306.biz.ticketservice.service.handler.ticket.dto.TrainPurchaseTicketRespDTO;
import org.opengoofy.index12306.biz.ticketservice.service.handler.ticket.tokenbucket.TicketAvailabilityTokenBucket;
import org.opengoofy.index12306.framework.starter.cache.DistributedCache;
import org.opengoofy.index12306.framework.starter.common.toolkit.BeanUtil;
import org.opengoofy.index12306.framework.starter.convention.exception.ServiceException;
import org.opengoofy.index12306.framework.starter.convention.result.Result;
import org.opengoofy.index12306.framework.starter.idempotent.annotation.Idempotent;
import org.opengoofy.index12306.framework.starter.idempotent.enums.IdempotentSceneEnum;
import org.opengoofy.index12306.framework.starter.idempotent.enums.IdempotentTypeEnum;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.opengoofy.index12306.biz.ticketservice.mq.event.TicketAvailabilityReleasedEvent;
import org.opengoofy.index12306.biz.ticketservice.mq.produce.TicketAvailabilityReleasedSendProduce;
import static org.opengoofy.index12306.biz.ticketservice.common.constant.RedisKeyConstant.TRAIN_STATION_REMAINING_TICKET;

/**
 * 延迟关闭订单消费者
 * 公众号：马丁玩编程，回复：加群，添加马哥微信（备注：12306）获取项目资料
 */
@Slf4j
@Component
@RequiredArgsConstructor
@RocketMQMessageListener(
        topic = TicketRocketMQConstant.ORDER_DELAY_CLOSE_TOPIC_KEY,
        selectorExpression = TicketRocketMQConstant.ORDER_DELAY_CLOSE_TAG_KEY,
        consumerGroup = TicketRocketMQConstant.TICKET_DELAY_CLOSE_CG_KEY
)
public class DelayCloseOrderConsumer implements RocketMQListener<MessageWrapper<DelayCloseOrderEvent>> {

    private final SeatService seatService;
    private final TicketOrderRemoteService ticketOrderRemoteService;
    private final TrainStationService trainStationService;
    private final DistributedCache distributedCache;
    private final TicketAvailabilityTokenBucket ticketAvailabilityTokenBucket;
    private final TicketAvailabilityReleasedSendProduce ticketAvailabilityReleasedSendProduce;
    private final CandidateOrderMapper candidateOrderMapper;
    private final CandidateOrderItemMapper candidateOrderItemMapper;

    @Value("${ticket.availability.cache-update.type:}")
    private String ticketAvailabilityCacheUpdateType;

    @Idempotent(
            uniqueKeyPrefix = "index12306-ticket:delay_close_order:",
            key = "#delayCloseOrderEventMessageWrapper.getKeys()+'_'+#delayCloseOrderEventMessageWrapper.hashCode()",
            type = IdempotentTypeEnum.SPEL,
            scene = IdempotentSceneEnum.MQ,
            keyTimeout = 7200L
    )
    @Override
    public void onMessage(MessageWrapper<DelayCloseOrderEvent> delayCloseOrderEventMessageWrapper) {
        //记录开始日志
        log.info("[延迟关闭订单] 开始消费：{}", JSON.toJSONString(delayCloseOrderEventMessageWrapper));
        //提取事件对象
        DelayCloseOrderEvent delayCloseOrderEvent = delayCloseOrderEventMessageWrapper.getMessage();
        String orderSn = delayCloseOrderEvent.getOrderSn();//订单号
        //远程调用订单服务尝试关闭订单
        Result<Boolean> closedTickOrder;
        try {
            //关闭订单
            closedTickOrder = ticketOrderRemoteService.closeTickOrder(new CancelTicketOrderReqDTO(orderSn));
        } catch (Throwable ex) {
            log.error("[延迟关闭订单] 订单号：{} 远程调用订单服务失败", orderSn, ex);
            throw ex;
        }
        //判断是否要回归资源
        if (closedTickOrder.isSuccess() && !StrUtil.equals(ticketAvailabilityCacheUpdateType, "binlog")) {
            if (!closedTickOrder.getData()) {
                log.info("[延迟关闭订单] 订单号：{} 用户已支付订单", orderSn);
                return;
            }
            String trainId = delayCloseOrderEvent.getTrainId();
            String departure = delayCloseOrderEvent.getDeparture();
            String arrival = delayCloseOrderEvent.getArrival();
            //乘车人购票信息
            List<TrainPurchaseTicketRespDTO> trainPurchaseTicketResults = delayCloseOrderEvent.getTrainPurchaseTicketResults();
            // 按座位类型分组，回滚资源和发送候补事件都需要使用。
            Map<Integer, List<TrainPurchaseTicketRespDTO>> seatTypeMap = trainPurchaseTicketResults.stream()
                    .collect(Collectors.groupingBy(TrainPurchaseTicketRespDTO::getSeatType));
            try {
                //解开锁 也就是更新这个状态
                seatService.unlock(trainId, departure, arrival, trainPurchaseTicketResults);
            } catch (Throwable ex) {
                log.error("[延迟关闭订单] 订单号：{} 回滚列车DB座位状态失败", orderSn, ex);
                throw ex;
            }
            try {
                //回滚redis缓存余票
                StringRedisTemplate stringRedisTemplate = (StringRedisTemplate) distributedCache.getInstance();
                //获取列车经过的所有站点区间
                List<RouteDTO> routeDTOList = trainStationService.listTakeoutTrainStationRoute(trainId, departure, arrival);
                //遍历每一个区间，增加余票数
                routeDTOList.forEach(each -> {
                    //拼接redis的key
                    String keySuffix = StrUtil.join("_", trainId, each.getStartStation(), each.getEndStation());
                    //遍历每种座位类型
                    seatTypeMap.forEach((seatType, trainPurchaseTicketRespDTOList) -> {
                        //增加redis的余票数
                        stringRedisTemplate.opsForHash()
                                .increment(TRAIN_STATION_REMAINING_TICKET + keySuffix,
                                        String.valueOf(seatType),
                                        trainPurchaseTicketRespDTOList.size());//增加的多少
                    });
                });
                //回滚令牌桶
                TicketOrderDetailRespDTO ticketOrderDetail = BeanUtil.convert(delayCloseOrderEvent, TicketOrderDetailRespDTO.class);
                ticketOrderDetail.setPassengerDetails(BeanUtil.convert(delayCloseOrderEvent.getTrainPurchaseTicketResults(), TicketOrderPassengerDetailRespDTO.class));
                ticketAvailabilityTokenBucket.rollbackInBucket(ticketOrderDetail);
            } catch (Throwable ex) {
                log.error("[延迟关闭订单] 订单号：{} 回滚列车Cache余票失败", orderSn, ex);
                throw ex;
            }
            CandidateOrderDO candidateOrderDO = candidateOrderMapper.selectOne(Wrappers.lambdaQuery(CandidateOrderDO.class)
                    .eq(CandidateOrderDO::getFormalOrderSn, orderSn)
                    .eq(CandidateOrderDO::getStatus, CandidateOrderStatusEnum.PENDING_PAYMENT.getCode())
            );
            if (candidateOrderDO != null) {
                int orderUpdatedRows = candidateOrderMapper.update(null, Wrappers.lambdaUpdate(CandidateOrderDO.class)
                        .set(CandidateOrderDO::getStatus, CandidateOrderStatusEnum.EXPIRED.getCode())
                        .set(CandidateOrderDO::getFailReason, "支付超时")
                        .eq(CandidateOrderDO::getCandidateOrderSn, candidateOrderDO.getCandidateOrderSn())
                        .eq(CandidateOrderDO::getStatus, CandidateOrderStatusEnum.PENDING_PAYMENT.getCode())
                );
                if (orderUpdatedRows != 1) {
                    throw new ServiceException("候补订单超时状态更新失败");
                }
                int itemUpdatedRows = candidateOrderItemMapper.update(null, Wrappers.lambdaUpdate(CandidateOrderItemDO.class)
                        .set(CandidateOrderItemDO::getStatus, CandidateOrderStatusEnum.EXPIRED.getCode())
                        .eq(CandidateOrderItemDO::getCandidateOrderSn, candidateOrderDO.getCandidateOrderSn())
                        .eq(CandidateOrderItemDO::getStatus, CandidateOrderStatusEnum.PENDING_PAYMENT.getCode())
                );
                if (itemUpdatedRows == 0) {
                    throw new ServiceException("候补明细超时状态更新失败");
                }
            }
            try {
                // 资源恢复成功后，再通知候补消费者尝试分配余票。
                seatTypeMap.forEach((seatType, ticketList) -> {
                    TicketAvailabilityReleasedEvent event =
                            TicketAvailabilityReleasedEvent.builder()
                                    .eventId("order-release:" + orderSn + ":" + seatType)
                                    .trainId(trainId)
                                    .departure(departure)
                                    .arrival(arrival)
                                    .seatType(seatType)
                                    .releasedCount(ticketList.size())
                                    .releaseReason("TIMEOUT")
                                    .eventTime(new Date())
                                    .build();
                    ticketAvailabilityReleasedSendProduce.sendMessage(event);
                });
            } catch (Throwable ex) {
                log.error("[延迟关闭订单] 订单号：{} 发送余票释放事件失败", orderSn, ex);
                throw ex;
            }
        }
        return;
    }

}
