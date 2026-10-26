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

package org.opengoofy.index12306.biz.ticketservice.service.impl;

import ch.qos.logback.classic.spi.STEUtil;
import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson2.JSON;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.Lists;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opengoofy.index12306.biz.ticketservice.common.enums.*;
import org.opengoofy.index12306.biz.ticketservice.dao.entity.*;
import org.opengoofy.index12306.biz.ticketservice.dao.mapper.*;
import org.opengoofy.index12306.biz.ticketservice.dto.domain.PurchaseTicketPassengerDetailDTO;
import org.opengoofy.index12306.biz.ticketservice.dto.domain.RouteDTO;
import org.opengoofy.index12306.biz.ticketservice.dto.domain.SeatClassDTO;
import org.opengoofy.index12306.biz.ticketservice.dto.domain.SeatTypeCountDTO;
import org.opengoofy.index12306.biz.ticketservice.dto.domain.TicketListDTO;
import org.opengoofy.index12306.biz.ticketservice.dto.req.*;
import org.opengoofy.index12306.biz.ticketservice.dto.resp.*;
import org.opengoofy.index12306.biz.ticketservice.remote.PayRemoteService;
import org.opengoofy.index12306.biz.ticketservice.remote.TicketOrderRemoteService;
import org.opengoofy.index12306.biz.ticketservice.remote.dto.PayInfoRespDTO;
import org.opengoofy.index12306.biz.ticketservice.remote.dto.RefundReqDTO;
import org.opengoofy.index12306.biz.ticketservice.remote.dto.RefundRespDTO;
import org.opengoofy.index12306.biz.ticketservice.remote.dto.TicketOrderCreateRemoteReqDTO;
import org.opengoofy.index12306.biz.ticketservice.remote.dto.TicketOrderItemCreateRemoteReqDTO;
import org.opengoofy.index12306.biz.ticketservice.remote.dto.TicketOrderPassengerDetailRespDTO;
import org.opengoofy.index12306.biz.ticketservice.service.SeatService;
import org.opengoofy.index12306.biz.ticketservice.service.TicketService;
import org.opengoofy.index12306.biz.ticketservice.service.TrainStationService;
import org.opengoofy.index12306.biz.ticketservice.service.cache.SeatMarginCacheLoader;
import org.opengoofy.index12306.biz.ticketservice.service.handler.ticket.dto.TokenResultDTO;
import org.opengoofy.index12306.biz.ticketservice.service.handler.ticket.dto.TrainPurchaseTicketRespDTO;
import org.opengoofy.index12306.biz.ticketservice.service.handler.ticket.select.TrainSeatTypeSelector;
import org.opengoofy.index12306.biz.ticketservice.service.handler.ticket.tokenbucket.TicketAvailabilityTokenBucket;
import org.opengoofy.index12306.biz.ticketservice.mq.event.TicketAvailabilityReleasedEvent;
import org.opengoofy.index12306.biz.ticketservice.mq.produce.TicketAvailabilityReleasedSendProduce;
import org.opengoofy.index12306.biz.ticketservice.toolkit.DateUtil;
import org.opengoofy.index12306.biz.ticketservice.toolkit.TimeStringComparator;
import org.opengoofy.index12306.framework.starter.bases.ApplicationContextHolder;
import org.opengoofy.index12306.framework.starter.cache.DistributedCache;
import org.opengoofy.index12306.framework.starter.cache.toolkit.CacheUtil;
import org.opengoofy.index12306.framework.starter.common.toolkit.BeanUtil;
import org.opengoofy.index12306.framework.starter.convention.exception.ServiceException;
import org.opengoofy.index12306.framework.starter.convention.result.Result;
import org.opengoofy.index12306.framework.starter.designpattern.chain.AbstractChainContext;
import org.opengoofy.index12306.framework.starter.distributedid.toolkit.SnowflakeIdUtil;
import org.opengoofy.index12306.framework.starter.idempotent.annotation.Idempotent;
import org.opengoofy.index12306.framework.starter.idempotent.enums.IdempotentSceneEnum;
import org.opengoofy.index12306.framework.starter.idempotent.enums.IdempotentTypeEnum;
import org.opengoofy.index12306.framework.starter.log.annotation.ILog;
import org.opengoofy.index12306.frameworks.starter.user.core.UserContext;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static org.opengoofy.index12306.biz.ticketservice.common.constant.Index12306Constant.ADVANCE_TICKET_DAY;
import static org.opengoofy.index12306.biz.ticketservice.common.constant.RedisKeyConstant.LOCK_PURCHASE_TICKETS;
import static org.opengoofy.index12306.biz.ticketservice.common.constant.RedisKeyConstant.LOCK_PURCHASE_TICKETS_V2;
import static org.opengoofy.index12306.biz.ticketservice.common.constant.RedisKeyConstant.LOCK_REGION_TRAIN_STATION;
import static org.opengoofy.index12306.biz.ticketservice.common.constant.RedisKeyConstant.LOCK_REGION_TRAIN_STATION_MAPPING;
import static org.opengoofy.index12306.biz.ticketservice.common.constant.RedisKeyConstant.LOCK_TOKEN_BUCKET_ISNULL;
import static org.opengoofy.index12306.biz.ticketservice.common.constant.RedisKeyConstant.REGION_TRAIN_STATION;
import static org.opengoofy.index12306.biz.ticketservice.common.constant.RedisKeyConstant.REGION_TRAIN_STATION_MAPPING;
import static org.opengoofy.index12306.biz.ticketservice.common.constant.RedisKeyConstant.TRAIN_INFO;
import static org.opengoofy.index12306.biz.ticketservice.common.constant.RedisKeyConstant.TRAIN_STATION_PRICE;
import static org.opengoofy.index12306.biz.ticketservice.common.constant.RedisKeyConstant.TRAIN_STATION_REMAINING_TICKET;
import static org.opengoofy.index12306.biz.ticketservice.toolkit.DateUtil.convertDateToLocalTime;

/**
 * 车票接口实现
 * 公众号：马丁玩编程，回复：加群，添加马哥微信（备注：12306）获取项目资料
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TicketServiceImpl extends ServiceImpl<TicketMapper, TicketDO> implements TicketService, CommandLineRunner {

    private final TrainMapper trainMapper;
    private final TicketAvailabilityReleasedSendProduce ticketAvailabilityReleasedSendProduce;
    private final TrainStationRelationMapper trainStationRelationMapper;
    private final TrainStationPriceMapper trainStationPriceMapper;
    private final DistributedCache distributedCache;
    private final TicketOrderRemoteService ticketOrderRemoteService;
    private final PayRemoteService payRemoteService;
    private final StationMapper stationMapper;
    private final SeatService seatService;
    private final TrainStationService trainStationService;
    private final TrainSeatTypeSelector trainSeatTypeSelector;
    private final SeatMarginCacheLoader seatMarginCacheLoader;
    private final AbstractChainContext<TicketPageQueryReqDTO> ticketPageQueryAbstractChainContext;
    private final AbstractChainContext<PurchaseTicketReqDTO> purchaseTicketAbstractChainContext;
    private final AbstractChainContext<RefundTicketReqDTO> refundReqDTOAbstractChainContext;
    private final RedissonClient redissonClient;
    private final ConfigurableEnvironment environment;
    private final TicketAvailabilityTokenBucket ticketAvailabilityTokenBucket;
    private TicketService ticketService;
    private final CandidateOrderMapper candidateOrderMapper;
    private final CandidateOrderItemMapper candidateOrderItemMapper;

    @Value("${ticket.availability.cache-update.type:}")
    private String ticketAvailabilityCacheUpdateType;
    @Value("${framework.cache.redis.prefix:}")
    private String cacheRedisPrefix;

    @Override
    public TicketPageQueryRespDTO pageListTicketQueryV1(TicketPageQueryReqDTO requestParam) {
        // 责任链模式 验证城市名称是否存在、不存在加载缓存以及出发日期不能小于当前日期等等
        ticketPageQueryAbstractChainContext.handler(TicketChainMarkEnum.TRAIN_QUERY_FILTER.name(), requestParam);
        StringRedisTemplate stringRedisTemplate = (StringRedisTemplate) distributedCache.getInstance();
        // 列车查询逻辑较为复杂，详细解析文章查看 https://nageoffer.com/12306/question
        // v1 版本存在严重的性能深渊问题，v2 版本完美的解决了该问题。通过 Jmeter 压测聚合报告得知，性能提升在 300% - 500%+
        List<Object> stationDetails = stringRedisTemplate.opsForHash()
                //一次性获取大量的值                         把两个站点包装成一个新的列表
                .multiGet(REGION_TRAIN_STATION_MAPPING, Lists.newArrayList(requestParam.getFromStation(), requestParam.getToStation()));
        //检查得到的数据信息再redis中是否存在，统计null的个数
        long count = stationDetails.stream().filter(Objects::isNull).count();
        if (count > 0) {
            //得到一个分布式锁
            RLock lock = redissonClient.getLock(LOCK_REGION_TRAIN_STATION_MAPPING);
            lock.lock();
            try {
                //再次检查提高效率 双重检查锁定模式
                stationDetails = stringRedisTemplate.opsForHash()
                        .multiGet(REGION_TRAIN_STATION_MAPPING, Lists.newArrayList(requestParam.getFromStation(), requestParam.getToStation()));
                count = stationDetails.stream().filter(Objects::isNull).count();
                //还是没有数据那就是查询数据库
                if (count > 0) {
                    //查询所有站点信息
                    List<StationDO> stationDOList = stationMapper.selectList(Wrappers.emptyWrapper());
                    //定义一个map用来存储站点信息
                    Map<String, String> regionTrainStationMap = new HashMap<>();
                    stationDOList.forEach(each -> regionTrainStationMap.put(each.getCode(), each.getRegionName()));
                    stringRedisTemplate.opsForHash().putAll(REGION_TRAIN_STATION_MAPPING, regionTrainStationMap);
                    //建立一个新的空列表，将用户查询的信息放入到这个列表中
                    stationDetails = new ArrayList<>();
                    stationDetails.add(regionTrainStationMap.get(requestParam.getFromStation()));
                    stationDetails.add(regionTrainStationMap.get(requestParam.getToStation()));
                }
            } finally {
                lock.unlock();
            }
        }
        //DTO列表
        List<TicketListDTO> seatResults = new ArrayList<>();
        //根据redis的键名用来查询该路线的所有列车
        String buildRegionTrainStationHashKey = String.format(REGION_TRAIN_STATION, stationDetails.get(0), stationDetails.get(1));
        //.entries(...)：获取Hash中的所有键值对
        Map<Object, Object> regionTrainStationAllMap = stringRedisTemplate.opsForHash().entries(buildRegionTrainStationHashKey);
        if (MapUtil.isEmpty(regionTrainStationAllMap)) {
            RLock lock = redissonClient.getLock(LOCK_REGION_TRAIN_STATION);
            lock.lock();
            try {
                regionTrainStationAllMap = stringRedisTemplate.opsForHash().entries(buildRegionTrainStationHashKey);
                if (MapUtil.isEmpty(regionTrainStationAllMap)) {
                    LambdaQueryWrapper<TrainStationRelationDO> queryWrapper = Wrappers.lambdaQuery(TrainStationRelationDO.class)
                            .eq(TrainStationRelationDO::getStartRegion, stationDetails.get(0))
                            .eq(TrainStationRelationDO::getEndRegion, stationDetails.get(1));
                    //获取所有符合条件的列车路线
                    List<TrainStationRelationDO> trainStationRelationList = trainStationRelationMapper.selectList(queryWrapper);
                    //遍历每个列车路线
                    for (TrainStationRelationDO each : trainStationRelationList) {
                        TrainDO trainDO = distributedCache.safeGet(
                                TRAIN_INFO + each.getTrainId(),
                                TrainDO.class,
                                () -> trainMapper.selectById(each.getTrainId()),// 如果缓存没有，就执行这个方法从数据库查询
                                ADVANCE_TICKET_DAY,
                                TimeUnit.DAYS);
                        TicketListDTO result = new TicketListDTO();
                        result.setTrainId(String.valueOf(trainDO.getId()));
                        result.setTrainNumber(trainDO.getTrainNumber());
                        result.setDepartureTime(convertDateToLocalTime(each.getDepartureTime(), "HH:mm"));
                        result.setArrivalTime(convertDateToLocalTime(each.getArrivalTime(), "HH:mm"));
                        result.setDuration(DateUtil.calculateHourDifference(each.getDepartureTime(), each.getArrivalTime()));
                        result.setDeparture(each.getDeparture());
                        result.setArrival(each.getArrival());
                        result.setDepartureFlag(each.getDepartureFlag());
                        result.setArrivalFlag(each.getArrivalFlag());
                        result.setTrainType(trainDO.getTrainType());
                        result.setTrainBrand(trainDO.getTrainBrand());
                        if (StrUtil.isNotBlank(trainDO.getTrainTag())) {
                            result.setTrainTags(StrUtil.split(trainDO.getTrainTag(), ","));
                        }
                        //计算跨天时间
                        long betweenDay = cn.hutool.core.date.DateUtil.betweenDay(each.getDepartureTime(), each.getArrivalTime(), false);
                        result.setDaysArrived((int) betweenDay);
                        //检查是否到了售票时间
                        result.setSaleStatus(new Date().after(trainDO.getSaleTime()) ? 0 : 1);
                        //格式化开售时间
                        result.setSaleTime(convertDateToLocalTime(trainDO.getSaleTime(), "MM-dd HH:mm"));
                        seatResults.add(result);
                        regionTrainStationAllMap.put(CacheUtil.buildKey(String.valueOf(each.getTrainId()), each.getDeparture(), each.getArrival()), JSON.toJSONString(result));
                    }
                    //批量写入redis缓存
                    stringRedisTemplate.opsForHash().putAll(buildRegionTrainStationHashKey, regionTrainStationAllMap);
                }
            } finally {
                lock.unlock();
            }
        }
        //组装最终结果 三元运算符判断是否为redis 数据库
        seatResults = CollUtil.isEmpty(seatResults)
                ? regionTrainStationAllMap.values().stream().map(each -> JSON.parseObject(each.toString(), TicketListDTO.class)).toList()
                : seatResults;
        seatResults = seatResults.stream().sorted(new TimeStringComparator()).toList();
        for (TicketListDTO each : seatResults) {
            //查询该车次下不同座位的价格列表
            String trainStationPriceStr = distributedCache.safeGet(
                    String.format(TRAIN_STATION_PRICE, each.getTrainId(), each.getDeparture(), each.getArrival()),
                    String.class,
                    () -> {
                        LambdaQueryWrapper<TrainStationPriceDO> trainStationPriceQueryWrapper = Wrappers.lambdaQuery(TrainStationPriceDO.class)
                                .eq(TrainStationPriceDO::getDeparture, each.getDeparture())
                                .eq(TrainStationPriceDO::getArrival, each.getArrival())
                                .eq(TrainStationPriceDO::getTrainId, each.getTrainId());
                        return JSON.toJSONString(trainStationPriceMapper.selectList(trainStationPriceQueryWrapper));
                    },
                    ADVANCE_TICKET_DAY,
                    TimeUnit.DAYS
            );
            //将json字符串转化为do对象
            List<TrainStationPriceDO> trainStationPriceDOList = JSON.parseArray(trainStationPriceStr, TrainStationPriceDO.class);
            List<SeatClassDTO> seatClassList = new ArrayList<>();
            trainStationPriceDOList.forEach(item -> {
                String seatType = String.valueOf(item.getSeatType());
                String keySuffix = StrUtil.join("_", each.getTrainId(), item.getDeparture(), item.getArrival());
                //从redis中查询剩余票数
                Object quantityObj = stringRedisTemplate.opsForHash().get(TRAIN_STATION_REMAINING_TICKET + keySuffix, seatType);
                int quantity = Optional.ofNullable(quantityObj)
                        .map(Object::toString)
                        .map(Integer::parseInt)
                        .orElseGet(() -> {
                            Map<String, String> seatMarginMap = seatMarginCacheLoader.load(String.valueOf(each.getTrainId()), seatType, item.getDeparture(), item.getArrival());
                            return Optional.ofNullable(seatMarginMap.get(String.valueOf(item.getSeatType()))).map(Integer::parseInt).orElse(0);
                        });
                seatClassList.add(new SeatClassDTO(item.getSeatType(), quantity, new BigDecimal(item.getPrice()).divide(new BigDecimal("100"), 1, RoundingMode.HALF_UP), false));
            });
            each.setSeatClassList(seatClassList);
        }
        return TicketPageQueryRespDTO.builder()
                .trainList(seatResults)
                .departureStationList(buildDepartureStationList(seatResults))
                .arrivalStationList(buildArrivalStationList(seatResults))
                .trainBrandList(buildTrainBrandList(seatResults))
                .seatClassTypeList(buildSeatClassList(seatResults))
                .build();
    }

    @Override
    public TicketPageQueryRespDTO pageListTicketQueryV2(TicketPageQueryReqDTO requestParam) {
        // 责任链模式 验证城市名称是否存在、不存在加载缓存以及出发日期不能小于当前日期等等
        ticketPageQueryAbstractChainContext.handler(TicketChainMarkEnum.TRAIN_QUERY_FILTER.name(), requestParam);
        StringRedisTemplate stringRedisTemplate = (StringRedisTemplate) distributedCache.getInstance();
        // 列车查询逻辑较为复杂，详细解析文章查看 https://nageoffer.com/12306/question
        // v2 版本更符合企业级高并发真实场景解决方案，完美解决了 v1 版本性能深渊问题。通过 Jmeter 压测聚合报告得知，性能提升在 300% - 500%+
        // 其实还能有 v3 版本，性能估计在原基础上还能进一步提升一倍。不过 v3 版本太过于复杂，不易读且不易扩展，就不写具体的代码了。面试中 v2 版本已经够和面试官吹的了
        List<Object> stationDetails = stringRedisTemplate.opsForHash()
                .multiGet(REGION_TRAIN_STATION_MAPPING, Lists.newArrayList(requestParam.getFromStation(), requestParam.getToStation()));
        //格式化出该路线的名字
        String buildRegionTrainStationHashKey = String.format(REGION_TRAIN_STATION, stationDetails.get(0), stationDetails.get(1));
        Map<Object, Object> regionTrainStationAllMap = stringRedisTemplate.opsForHash().entries(buildRegionTrainStationHashKey);
        List<TicketListDTO> seatResults = regionTrainStationAllMap.values().stream()
                .map(each -> JSON.parseObject(each.toString(), TicketListDTO.class))
                .sorted(new TimeStringComparator())
                .toList();
        List<String> trainStationPriceKeys = seatResults.stream()
                .map(each -> String.format(cacheRedisPrefix + TRAIN_STATION_PRICE, each.getTrainId(), each.getDeparture(), each.getArrival()))
                .toList();
        //运用管道技术 批量查询票价
        List<Object> trainStationPriceObjs = stringRedisTemplate.executePipelined((RedisCallback<String>) connection -> {
            trainStationPriceKeys.forEach(each -> connection.stringCommands().get(each.getBytes()));
            return null;
        });
        List<TrainStationPriceDO> trainStationPriceDOList = new ArrayList<>();
        List<String> trainStationRemainingKeyList = new ArrayList<>();
        for (Object each : trainStationPriceObjs) {
            List<TrainStationPriceDO> trainStationPriceList = JSON.parseArray(each.toString(), TrainStationPriceDO.class);
            trainStationPriceDOList.addAll(trainStationPriceList);
            for (TrainStationPriceDO item : trainStationPriceList) {
                String trainStationRemainingKey = cacheRedisPrefix + TRAIN_STATION_REMAINING_TICKET + StrUtil.join("_", item.getTrainId(), item.getDeparture(), item.getArrival());
                trainStationRemainingKeyList.add(trainStationRemainingKey);
            }
        }
        //再次使用pipeline批量查询余票
        List<Object> trainStationRemainingObjs = stringRedisTemplate.executePipelined((RedisCallback<String>) connection -> {
            for (int i = 0; i < trainStationRemainingKeyList.size(); i++) {
                connection.hashCommands().hGet(trainStationRemainingKeyList.get(i).getBytes(), trainStationPriceDOList.get(i).getSeatType().toString().getBytes());
            }
            return null;
        });
        for (TicketListDTO each : seatResults) {
            //获取当前该车支持的所有座位类型
            List<Integer> seatTypesByCode = VehicleTypeEnum.findSeatTypesByCode(each.getTrainType());
            //从批量查询的结果返回当前列车的数据
            List<Object> remainingTicket = new ArrayList<>(trainStationRemainingObjs.subList(0, seatTypesByCode.size()));
            List<TrainStationPriceDO> trainStationPriceDOSub = new ArrayList<>(trainStationPriceDOList.subList(0, seatTypesByCode.size()));
            //从原列表中移除已使用的数据
            trainStationRemainingObjs.subList(0, seatTypesByCode.size()).clear();
            trainStationPriceDOList.subList(0, seatTypesByCode.size()).clear();
            //构建座位类型列表
            List<SeatClassDTO> seatClassList = new ArrayList<>();
            for (int i = 0; i < trainStationPriceDOSub.size(); i++) {
                TrainStationPriceDO trainStationPriceDO = trainStationPriceDOSub.get(i);
                SeatClassDTO seatClassDTO = SeatClassDTO.builder()
                        .type(trainStationPriceDO.getSeatType())
                        .quantity(Integer.parseInt(remainingTicket.get(i).toString()))
                        .price(new BigDecimal(trainStationPriceDO.getPrice()).divide(new BigDecimal("100"), 1, RoundingMode.HALF_UP))
                        .candidate(false)
                        .build();
                seatClassList.add(seatClassDTO);
            }
            //设置列车在对象上
            each.setSeatClassList(seatClassList);
        }
        return TicketPageQueryRespDTO.builder()
                .trainList(seatResults)
                .departureStationList(buildDepartureStationList(seatResults))
                .arrivalStationList(buildArrivalStationList(seatResults))
                .trainBrandList(buildTrainBrandList(seatResults))
                .seatClassTypeList(buildSeatClassList(seatResults))
                .build();
    }

    @ILog
    @Idempotent(
            uniqueKeyPrefix = "index12306-ticket:lock_purchase-tickets:",
            key = "T(org.opengoofy.index12306.framework.starter.bases.ApplicationContextHolder).getBean('environment').getProperty('unique-name', '')"
                    + "+'_'+"
                    + "T(org.opengoofy.index12306.frameworks.starter.user.core.UserContext).getUsername()",
            message = "正在执行下单流程，请稍后...",
            scene = IdempotentSceneEnum.RESTAPI,
            type = IdempotentTypeEnum.SPEL
    )
    @Override
    public TicketPurchaseRespDTO purchaseTicketsV1(PurchaseTicketReqDTO requestParam) {
        // 责任链模式，验证 1：参数必填 2：参数正确性 3：乘客是否已买当前车次等...
        purchaseTicketAbstractChainContext.handler(TicketChainMarkEnum.TRAIN_PURCHASE_TICKET_FILTER.name(), requestParam);
        // v1 版本购票存在 4 个较为严重的问题，v2 版本相比较 v1 版本更具有业务特点以及性能，整体提升较大
        // 写了详细的 v2 版本购票升级指南，详情查看：https://nageoffer.com/12306/question
        String lockKey = environment.resolvePlaceholders(String.format(LOCK_PURCHASE_TICKETS, requestParam.getTrainId()));
        RLock lock = redissonClient.getLock(lockKey);
        lock.lock();
        try {
            return ticketService.executePurchaseTickets(requestParam);
        } finally {
            lock.unlock();
        }
    }

    private final Cache<String, ReentrantLock> localLockMap = Caffeine.newBuilder()
            .expireAfterWrite(1, TimeUnit.DAYS)
            .build();

    private final Cache<String, Object> tokenTicketsRefreshMap = Caffeine.newBuilder()
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .build();

    @ILog
    //幂等注解
    @Idempotent(
            uniqueKeyPrefix = "index12306-ticket:lock_purchase-tickets:",
            key = "T(org.opengoofy.index12306.framework.starter.bases.ApplicationContextHolder).getBean('environment').getProperty('unique-name', '')"
                    + "+'_'+"
                    + "T(org.opengoofy.index12306.frameworks.starter.user.core.UserContext).getUsername()",
            message = "正在执行下单流程，请稍后...",
            scene = IdempotentSceneEnum.RESTAPI,
            type = IdempotentTypeEnum.SPEL
    )
    @Override
    public TicketPurchaseRespDTO purchaseTicketsV2(PurchaseTicketReqDTO requestParam) {
        // 责任链模式，验证 1：参数必填 2：参数正确性 3：乘客是否已买当前车次等...
        purchaseTicketAbstractChainContext.handler(TicketChainMarkEnum.TRAIN_PURCHASE_TICKET_FILTER.name(), requestParam);
        // 为什么需要令牌限流？余票缓存限流不可以么？详情查看：https://nageoffer.com/12306/question
        TokenResultDTO tokenResult = ticketAvailabilityTokenBucket.takeTokenFromBucket(requestParam);
        if (tokenResult.getTokenIsNull()) {
            //双层检查鉴定
            Object ifPresentObj = tokenTicketsRefreshMap.getIfPresent(requestParam.getTrainId());
            if (ifPresentObj == null) {
                synchronized (TicketService.class) {//类级别的锁
                    if (tokenTicketsRefreshMap.getIfPresent(requestParam.getTrainId()) == null) {
                        ifPresentObj = new Object();
                        tokenTicketsRefreshMap.put(requestParam.getTrainId(), ifPresentObj);
                        tokenIsNullRefreshToken(requestParam, tokenResult);//刷新余票缓存
                    }
                }
            }
            throw new ServiceException("列车站点已无余票");
        }
        // v1 版本购票存在 4 个较为严重的问题，v2 版本相比较 v1 版本更具有业务特点以及性能，整体提升较大
        // 写了详细的 v2 版本购票升级指南，详情查看：https://nageoffer.com/12306/question
        List<ReentrantLock> localLockList = new ArrayList<>();//本地锁列表
        List<RLock> distributedLockList = new ArrayList<>();//分布式锁列表
        //按座位类型分组
        Map<Integer, List<PurchaseTicketPassengerDetailDTO>> seatTypeMap = requestParam.getPassengers().stream()
                .collect(Collectors.groupingBy(PurchaseTicketPassengerDetailDTO::getSeatType));
        seatTypeMap.forEach((searType, count) -> {
            String lockKey = environment.resolvePlaceholders(String.format(LOCK_PURCHASE_TICKETS_V2, requestParam.getTrainId(), searType));
            //本地锁
            ReentrantLock localLock = localLockMap.getIfPresent(lockKey);
            if (localLock == null) {
                synchronized (TicketService.class) {
                    if ((localLock = localLockMap.getIfPresent(lockKey)) == null) {
                        localLock = new ReentrantLock(true);//公平锁
                        localLockMap.put(lockKey, localLock);
                    }
                }
            }
            localLockList.add(localLock);
            //分布式锁
            RLock distributedLock = redissonClient.getFairLock(lockKey);
            distributedLockList.add(distributedLock);
        });
        try {
            localLockList.forEach(ReentrantLock::lock);
            distributedLockList.forEach(RLock::lock);
            return ticketService.executePurchaseTickets(requestParam);
        } finally {
            localLockList.forEach(localLock -> {
                try {
                    localLock.unlock();
                } catch (Throwable ignored) {
                }
            });
            distributedLockList.forEach(distributedLock -> {
                try {
                    distributedLock.unlock();
                } catch (Throwable ignored) {
                }
            });
        }
    }

    @Override
    @Transactional(rollbackFor = Throwable.class)
    public TicketPurchaseRespDTO executePurchaseTickets(PurchaseTicketReqDTO requestParam) {
        // 候补改造：普通 HTTP 请求仍从 UserContext 读取身份，再显式传给内部实现
        return executePurchaseTickets(requestParam, UserContext.getUserId(), UserContext.getUsername());
    }

    // 候补改造：内部购票逻辑显式接收用户身份，MQ 线程不依赖 UserContext
    private TicketPurchaseRespDTO executePurchaseTickets(PurchaseTicketReqDTO requestParam, String userId, String username) {
        List<TicketOrderDetailRespDTO> ticketOrderDetailResults = new ArrayList<>();
        String trainId = requestParam.getTrainId();
        // 节假日高并发购票Redis能扛得住么？详情查看：https://nageoffer.com/12306/question
        TrainDO trainDO = distributedCache.safeGet(
                TRAIN_INFO + trainId,
                TrainDO.class,
                () -> trainMapper.selectById(trainId),
                ADVANCE_TICKET_DAY,
                TimeUnit.DAYS);
        List<TrainPurchaseTicketRespDTO> trainPurchaseTicketResults = trainSeatTypeSelector.select(trainDO.getTrainType(), requestParam, username);
        List<TicketDO> ticketDOList = trainPurchaseTicketResults.stream()
                .map(each -> TicketDO.builder()
                        .username(username) // 候补改造：使用显式传入的用户名
                        .trainId(Long.parseLong(requestParam.getTrainId()))
                        .carriageNumber(each.getCarriageNumber())
                        .seatNumber(each.getSeatNumber())
                        .passengerId(each.getPassengerId())
                        .ticketStatus(TicketStatusEnum.UNPAID.getCode())
                        .build())
                .toList();
        saveBatch(ticketDOList);
        Result<String> ticketOrderResult;
        try {
            List<TicketOrderItemCreateRemoteReqDTO> orderItemCreateRemoteReqDTOList = new ArrayList<>();
            trainPurchaseTicketResults.forEach(each -> {
                TicketOrderItemCreateRemoteReqDTO orderItemCreateRemoteReqDTO = TicketOrderItemCreateRemoteReqDTO.builder()
                        .amount(each.getAmount())
                        .carriageNumber(each.getCarriageNumber())
                        .seatNumber(each.getSeatNumber())
                        .idCard(each.getIdCard())
                        .idType(each.getIdType())
                        .phone(each.getPhone())
                        .seatType(each.getSeatType())
                        .ticketType(each.getUserType())
                        .realName(each.getRealName())
                        .build();
                TicketOrderDetailRespDTO ticketOrderDetailRespDTO = TicketOrderDetailRespDTO.builder()
                        .amount(each.getAmount())
                        .carriageNumber(each.getCarriageNumber())
                        .seatNumber(each.getSeatNumber())
                        .idCard(each.getIdCard())
                        .idType(each.getIdType())
                        .seatType(each.getSeatType())
                        .ticketType(each.getUserType())
                        .realName(each.getRealName())
                        .passengerId(each.getPassengerId())
                        .build();
                orderItemCreateRemoteReqDTOList.add(orderItemCreateRemoteReqDTO);
                ticketOrderDetailResults.add(ticketOrderDetailRespDTO);
            });
            LambdaQueryWrapper<TrainStationRelationDO> queryWrapper = Wrappers.lambdaQuery(TrainStationRelationDO.class)
                    .eq(TrainStationRelationDO::getTrainId, trainId)
                    .eq(TrainStationRelationDO::getDeparture, requestParam.getDeparture())
                    .eq(TrainStationRelationDO::getArrival, requestParam.getArrival());
            TrainStationRelationDO trainStationRelationDO = trainStationRelationMapper.selectOne(queryWrapper);
            TicketOrderCreateRemoteReqDTO orderCreateRemoteReqDTO = TicketOrderCreateRemoteReqDTO.builder()
                    .departure(requestParam.getDeparture())
                    .arrival(requestParam.getArrival())
                    .orderTime(new Date())
                    .source(SourceEnum.INTERNET.getCode())
                    .trainNumber(trainDO.getTrainNumber())
                    .departureTime(trainStationRelationDO.getDepartureTime())
                    .arrivalTime(trainStationRelationDO.getArrivalTime())
                    .ridingDate(trainStationRelationDO.getDepartureTime())
                    .userId(userId) // 候补改造：使用显式传入的用户 ID
                    .username(username) // 候补改造：使用显式传入的用户名
                    .trainId(Long.parseLong(requestParam.getTrainId()))
                    .ticketOrderItems(orderItemCreateRemoteReqDTOList)
                    .build();
            ticketOrderResult = ticketOrderRemoteService.createTicketOrder(orderCreateRemoteReqDTO);
            if (!ticketOrderResult.isSuccess() || StrUtil.isBlank(ticketOrderResult.getData())) {
                log.error("订单服务调用失败，返回结果：{}", ticketOrderResult.getMessage());
                throw new ServiceException("订单服务调用失败");
            }
        } catch (Throwable ex) {
            log.error("远程调用订单服务创建错误，请求参数：{}", JSON.toJSONString(requestParam), ex);
            throw ex;
        }
        return new TicketPurchaseRespDTO(ticketOrderResult.getData(), ticketOrderDetailResults);
    }
    @Transactional(rollbackFor = Throwable.class)
    @Override
    public TicketPurchaseRespDTO allocateCandidateOrder(String candidateOrderSn) {
        CandidateOrderDO candidateOrderDO = candidateOrderMapper.selectOne(Wrappers.lambdaQuery(CandidateOrderDO.class)
                .eq(CandidateOrderDO::getCandidateOrderSn,candidateOrderSn)
                .eq(CandidateOrderDO::getStatus,CandidateOrderStatusEnum.ALLOCATING.getCode())
        );

        if(candidateOrderDO==null){
            throw new ServiceException("候补订单不存在或状态已改变");
        }
        List<CandidateOrderItemDO> itemList = candidateOrderItemMapper.selectList(
                Wrappers.lambdaQuery(CandidateOrderItemDO.class)
                        .eq(CandidateOrderItemDO::getCandidateOrderSn, candidateOrderSn)
                        .eq(CandidateOrderItemDO::getStatus, CandidateOrderStatusEnum.WAITING.getCode())
        );

        if (itemList == null || itemList.isEmpty()) {
            throw new ServiceException("候补订单没有有效的乘车人明细");
        }
        List<PurchaseTicketPassengerDetailDTO> passengerList = itemList.stream().map(item ->{
            PurchaseTicketPassengerDetailDTO passenger = new PurchaseTicketPassengerDetailDTO();
            passenger.setPassengerId(item.getPassengerId());
            passenger.setSeatType(item.getSeatType());
            return passenger;
        }).toList();

        PurchaseTicketReqDTO purchaseTicketReqDTO =new PurchaseTicketReqDTO();
        purchaseTicketReqDTO.setTrainId(String.valueOf(candidateOrderDO.getTrainId()));
        purchaseTicketReqDTO.setDeparture(candidateOrderDO.getDeparture());
        purchaseTicketReqDTO.setArrival(candidateOrderDO.getArrival());
        purchaseTicketReqDTO.setPassengers(passengerList);

        TokenResultDTO tokenResult = ticketAvailabilityTokenBucket.takeTokenFromBucket(purchaseTicketReqDTO);
        if(Boolean.TRUE.equals(tokenResult.getTokenIsNull())){
            int resetRows = candidateOrderMapper.update(null,Wrappers.lambdaUpdate(CandidateOrderDO.class)
                    .set(CandidateOrderDO::getStatus,CandidateOrderStatusEnum.WAITING.getCode())
                    .eq(CandidateOrderDO::getCandidateOrderSn, candidateOrderSn)
                    .eq(CandidateOrderDO::getStatus,CandidateOrderStatusEnum.ALLOCATING.getCode())
            );
            if (resetRows != 1) {
                throw new ServiceException("候补订单等待状态恢复失败");
            }
            log.info("候补订单本次未获取到余票，恢复等待状态，candidateOrderSn={}", candidateOrderSn);
            return null;
        }
        List<ReentrantLock> localLockList = new ArrayList<>();//本地锁列表
        List<RLock> distributedLockList = new ArrayList<>();//分布式锁列表
        Map<Integer, List<PurchaseTicketPassengerDetailDTO>> seatTypeMap = purchaseTicketReqDTO.getPassengers().stream()
                .collect(Collectors.groupingBy(PurchaseTicketPassengerDetailDTO::getSeatType));
        seatTypeMap.forEach((searType, count) -> {
            String lockKey = environment.resolvePlaceholders(String.format(LOCK_PURCHASE_TICKETS_V2, purchaseTicketReqDTO.getTrainId(), searType));
            //本地锁
            ReentrantLock localLock = localLockMap.getIfPresent(lockKey);
            if (localLock == null) {
                synchronized (TicketService.class) {
                    if ((localLock = localLockMap.getIfPresent(lockKey)) == null) {
                        localLock = new ReentrantLock(true);//公平锁
                        localLockMap.put(lockKey, localLock);
                    }
                }
            }
            localLockList.add(localLock);
            //分布式锁
            RLock distributedLock = redissonClient.getFairLock(lockKey);
            distributedLockList.add(distributedLock);
        });
        try {
            localLockList.forEach(ReentrantLock::lock);
            distributedLockList.forEach(RLock::lock);
            // 候补改造：MQ 线程没有 UserContext，使用候补主表保存的用户身份
            TicketPurchaseRespDTO purchaseResult = executePurchaseTickets(
                    purchaseTicketReqDTO,
                    candidateOrderDO.getUserId(),
                    candidateOrderDO.getUsername()
            );

            if(purchaseResult==null|| StrUtil.isBlank(purchaseResult.getOrderSn())){
                throw new ServiceException("正式订单创建失败");
            }
            Date paymentDeadline =new Date(System.currentTimeMillis()+30*60*1000l);
            int updatedRows = candidateOrderMapper.update(null, Wrappers.lambdaUpdate(CandidateOrderDO.class)
                            .set(CandidateOrderDO::getFormalOrderSn, purchaseResult.getOrderSn())
                            .set(CandidateOrderDO::getPaymentDeadline, paymentDeadline)
                            .set(CandidateOrderDO::getStatus, CandidateOrderStatusEnum.PENDING_PAYMENT.getCode())
                            .eq(CandidateOrderDO::getCandidateOrderSn, candidateOrderSn)
                            .eq(CandidateOrderDO::getStatus, CandidateOrderStatusEnum.ALLOCATING.getCode())
            );

            if (updatedRows != 1) {
                throw new ServiceException("候补订单状态更新失败");
            }

            for (TicketOrderDetailRespDTO detail : purchaseResult.getTicketOrderDetails()) {
                int itemUpdatedRows = candidateOrderItemMapper.update(null, Wrappers.lambdaUpdate(CandidateOrderItemDO.class)
                                .set(CandidateOrderItemDO::getCarriageNumber, detail.getCarriageNumber())
                                .set(CandidateOrderItemDO::getSeatNumber, detail.getSeatNumber())
                                .set(CandidateOrderItemDO::getAmount, detail.getAmount())
                                .set(CandidateOrderItemDO::getStatus, CandidateOrderStatusEnum.PENDING_PAYMENT.getCode())
                                .eq(CandidateOrderItemDO::getCandidateOrderSn, candidateOrderSn)
                                .eq(CandidateOrderItemDO::getPassengerId, detail.getPassengerId())
                                .eq(CandidateOrderItemDO::getStatus, CandidateOrderStatusEnum.WAITING.getCode())
                );

                if (itemUpdatedRows != 1) {
                    throw new ServiceException("候补乘车人明细更新失败：" + detail.getPassengerId());
                }
            }

            return purchaseResult;
        } catch (Throwable ex) {
            // 候补改造：令牌已经扣减，但选座或正式订单创建失败时必须恢复令牌
            try {
                org.opengoofy.index12306.biz.ticketservice.remote.dto.TicketOrderDetailRespDTO rollbackRequest =
                        new org.opengoofy.index12306.biz.ticketservice.remote.dto.TicketOrderDetailRespDTO();
                rollbackRequest.setTrainId(Long.valueOf(purchaseTicketReqDTO.getTrainId()));
                rollbackRequest.setDeparture(purchaseTicketReqDTO.getDeparture());
                rollbackRequest.setArrival(purchaseTicketReqDTO.getArrival());
                List<TicketOrderPassengerDetailRespDTO> rollbackPassengers = purchaseTicketReqDTO.getPassengers().stream()
                        .map(passenger -> TicketOrderPassengerDetailRespDTO.builder()
                                .seatType(passenger.getSeatType())
                                .build())
                        .toList();
                rollbackRequest.setPassengerDetails(rollbackPassengers);
                ticketAvailabilityTokenBucket.rollbackInBucket(rollbackRequest);
            } catch (Throwable rollbackEx) {
                log.error("候补订单令牌回滚失败，candidateOrderSn={}", candidateOrderSn, rollbackEx);
            }
            log.error("候补订单分配异常，candidateOrderSn={}", candidateOrderSn, ex);
            throw ex;
        } finally {
            localLockList.forEach(localLock -> {
                try {
                    localLock.unlock();
                } catch (Throwable ignored) {
                }
            });
            distributedLockList.forEach(distributedLock -> {
                try {
                    distributedLock.unlock();
                } catch (Throwable ignored) {
                }
            });
        }
    }


    @Override
    public PayInfoRespDTO getPayInfo(String orderSn) {
        return payRemoteService.getPayInfo(orderSn).getData();
    }

    /**
     * 取消车票订单（核心业务方法）
     *
     * 功能说明：用户主动取消订单或系统自动取消超时未支付订单时调用
     * 主要职责：在订单层面取消后，需要同步释放三层资源（保证数据一致性）
     *
     * 三层资源回滚（按顺序执行）：
     *   第一层：数据库座位状态回滚（将座位从"已锁定"改为"可售"）
     *   第二层：Redis令牌桶库存恢复（增加可用令牌数）
     *   第三层：Redis余票缓存恢复（增加各区间余票显示数量）
     *
     * 调用场景：
     *   1. 用户在APP/网页点击"取消订单"
     *   2. 定时任务扫描到超过30分钟未支付的订单
     *   3. 系统检测到异常订单需要强制取消
     *
     * @param requestParam 取消订单请求参数，包含：
     *                    - orderSn: 订单编号（唯一标识）
     *                    - userId: 用户ID（用于权限校验）
     *                    - 其他取消原因等信息
     *
     * @throws ServiceException 当任何一层资源回滚失败时抛出异常
     */
    @ILog  // 日志注解：自动记录方法入参、出参、执行时间等日志
    @Override
    public void cancelTicketOrder(CancelTicketOrderReqDTO requestParam) {


        Result<Void> cancelOrderResult = ticketOrderRemoteService.cancelTicketOrder(requestParam);


        if (cancelOrderResult.isSuccess() && !StrUtil.equals(ticketAvailabilityCacheUpdateType, "binlog")) {


            Result<org.opengoofy.index12306.biz.ticketservice.remote.dto.TicketOrderDetailRespDTO> ticketOrderDetailResult =
                    ticketOrderRemoteService.queryTicketOrderByOrderSn(requestParam.getOrderSn());

            // 获取订单详情对象
            org.opengoofy.index12306.biz.ticketservice.remote.dto.TicketOrderDetailRespDTO ticketOrderDetail =
                    ticketOrderDetailResult.getData();

            // 提取关键字段（后续回滚要用）
            String trainId = String.valueOf(ticketOrderDetail.getTrainId());       // 列车ID: "G1234"
            String departure = ticketOrderDetail.getDeparture();                   // 出发站: "北京"
            String arrival = ticketOrderDetail.getArrival();                       // 到达站: "上海"

            // 获取该订单的所有乘客明细（每个乘客对应一张票）
            List<TicketOrderPassengerDetailRespDTO> trainPurchaseTicketResults =
                    ticketOrderDetail.getPassengerDetails();

            try {

                seatService.unlock(
                        trainId,
                        departure,
                        arrival,
                        BeanUtil.convert(trainPurchaseTicketResults, TrainPurchaseTicketRespDTO.class)
                );
            } catch (Throwable ex) {

                log.error("[取消订单] 订单号：{} 回滚列车DB座位状态失败", requestParam.getOrderSn(), ex);
                throw ex;  // 抛出异常让上层感知失败，可能触发重试或人工干预
            }


            ticketAvailabilityTokenBucket.rollbackInBucket(ticketOrderDetail);

            try {
                // 获取Redis操作工具
                StringRedisTemplate stringRedisTemplate = (StringRedisTemplate) distributedCache.getInstance();

                Map<Integer, List<TicketOrderPassengerDetailRespDTO>> seatTypeMap =
                        trainPurchaseTicketResults.stream()
                                .collect(Collectors.groupingBy(TicketOrderPassengerDetailRespDTO::getSeatType));

                // 查询列车经过的所有站点区间（与购票时一致）
                // 例如：北京→上海，经过 北京-济南、济南-上海 两个区间
                List<RouteDTO> routeDTOList =
                        trainStationService.listTakeoutTrainStationRoute(trainId, departure, arrival);

                // 双重循环：外层遍历区间，内层遍历座位类型
                routeDTOList.forEach(each -> {  // 外层：每个站点区间
                    // 构建Redis键名后缀："G1234_北京_济南"
                    String keySuffix = StrUtil.join("_", trainId, each.getStartStation(), each.getEndStation());

                    seatTypeMap.forEach((seatType, ticketOrderPassengerDetailRespDTOList) -> {  // 内层：每种座位类型
                        
                        stringRedisTemplate.opsForHash()
                                .increment(
                                        TRAIN_STATION_REMAINING_TICKET + keySuffix,
                                        String.valueOf(seatType),
                                        ticketOrderPassengerDetailRespDTOList.size()  // 正数：恢复余票
                                );
                    });
                });
                // ========== 第四步：通知候补订单尝试分配（释放余票事件） ==========
                seatTypeMap.forEach((seatType, ticketList) -> {
                    TicketAvailabilityReleasedEvent event =
                            TicketAvailabilityReleasedEvent.builder()
                                    .eventId("order-cancel:" + requestParam.getOrderSn() + ":" + seatType)
                                    .trainId(trainId)
                                    .departure(departure)
                                    .arrival(arrival)
                                    .seatType(seatType)
                                    .releasedCount(ticketList.size())
                                    .releaseReason("CANCEL")
                                    .eventTime(new Date())
                                    .build();
                    ticketAvailabilityReleasedSendProduce.sendMessage(event);
                });
            } catch (Throwable ex) {
                // 余票缓存回滚失败也要记录并抛出异常
                // 虽然不影响实际售票（那是令牌桶控制的），但会导致前端显示不准确
                log.error("[取消关闭订单] 订单号：{} 回滚列车Cache余票失败", requestParam.getOrderSn(), ex);
                throw ex;
            }
        }
        // 如果取消订单失败或是binlog模式，直接结束（不需要手动回滚）
    }

    @Override
    public RefundTicketRespDTO commonTicketRefund(RefundTicketReqDTO requestParam) {
        // 责任链模式，验证 1：参数必填
        refundReqDTOAbstractChainContext.handler(TicketChainMarkEnum.TRAIN_REFUND_TICKET_FILTER.name(), requestParam);
        Result<org.opengoofy.index12306.biz.ticketservice.remote.dto.TicketOrderDetailRespDTO> orderDetailRespDTOResult = ticketOrderRemoteService.queryTicketOrderByOrderSn(requestParam.getOrderSn());
        if (!orderDetailRespDTOResult.isSuccess() && Objects.isNull(orderDetailRespDTOResult.getData())) {
            throw new ServiceException("车票订单不存在");
        }
        org.opengoofy.index12306.biz.ticketservice.remote.dto.TicketOrderDetailRespDTO ticketOrderDetailRespDTO = orderDetailRespDTOResult.getData();
        List<TicketOrderPassengerDetailRespDTO> passengerDetails = ticketOrderDetailRespDTO.getPassengerDetails();
        if (CollectionUtil.isEmpty(passengerDetails)) {
            throw new ServiceException("车票子订单不存在");
        }
        RefundReqDTO refundReqDTO = new RefundReqDTO();
        if (RefundTypeEnum.PARTIAL_REFUND.getType().equals(requestParam.getType())) {
            TicketOrderItemQueryReqDTO ticketOrderItemQueryReqDTO = new TicketOrderItemQueryReqDTO();
            ticketOrderItemQueryReqDTO.setOrderSn(requestParam.getOrderSn());
            ticketOrderItemQueryReqDTO.setOrderItemRecordIds(requestParam.getSubOrderRecordIdReqList());
            Result<List<TicketOrderPassengerDetailRespDTO>> queryTicketItemOrderById = ticketOrderRemoteService.queryTicketItemOrderById(ticketOrderItemQueryReqDTO);
            List<TicketOrderPassengerDetailRespDTO> partialRefundPassengerDetails = passengerDetails.stream()
                    .filter(item -> queryTicketItemOrderById.getData().contains(item))
                    .collect(Collectors.toList());
            refundReqDTO.setRefundTypeEnum(RefundTypeEnum.PARTIAL_REFUND);
            refundReqDTO.setRefundDetailReqDTOList(partialRefundPassengerDetails);
        } else if (RefundTypeEnum.FULL_REFUND.getType().equals(requestParam.getType())) {
            refundReqDTO.setRefundTypeEnum(RefundTypeEnum.FULL_REFUND);
            refundReqDTO.setRefundDetailReqDTOList(passengerDetails);
        }
        if (CollectionUtil.isNotEmpty(passengerDetails)) {
            Integer partialRefundAmount = passengerDetails.stream()
                    .mapToInt(TicketOrderPassengerDetailRespDTO::getAmount)
                    .sum();
            refundReqDTO.setRefundAmount(partialRefundAmount);
        }
        refundReqDTO.setOrderSn(requestParam.getOrderSn());
        Result<RefundRespDTO> refundRespDTOResult = payRemoteService.commonRefund(refundReqDTO);
        if (!refundRespDTOResult.isSuccess() && Objects.isNull(refundRespDTOResult.getData())) {
            throw new ServiceException("车票订单退款失败");
        }
        return null; // 暂时返回空实体
    }

    private List<String> buildDepartureStationList(List<TicketListDTO> seatResults) {
        return seatResults.stream().map(TicketListDTO::getDeparture).distinct().collect(Collectors.toList());
    }

    private List<String> buildArrivalStationList(List<TicketListDTO> seatResults) {
        return seatResults.stream().map(TicketListDTO::getArrival).distinct().collect(Collectors.toList());
    }

    private List<Integer> buildSeatClassList(List<TicketListDTO> seatResults) {
        Set<Integer> resultSeatClassList = new HashSet<>();
        for (TicketListDTO each : seatResults) {
            for (SeatClassDTO item : each.getSeatClassList()) {
                resultSeatClassList.add(item.getType());
            }
        }
        return resultSeatClassList.stream().toList();
    }

    private List<Integer> buildTrainBrandList(List<TicketListDTO> seatResults) {
        Set<Integer> trainBrandSet = new HashSet<>();
        for (TicketListDTO each : seatResults) {
            if (StrUtil.isNotBlank(each.getTrainBrand())) {
                trainBrandSet.addAll(StrUtil.split(each.getTrainBrand(), ",").stream().map(Integer::parseInt).toList());
            }
        }
        return trainBrandSet.stream().toList();
    }

    private final ScheduledExecutorService tokenIsNullRefreshExecutor = Executors.newScheduledThreadPool(1);

    private void tokenIsNullRefreshToken(PurchaseTicketReqDTO requestParam, TokenResultDTO tokenResult) {
        RLock lock = redissonClient.getLock(String.format(LOCK_TOKEN_BUCKET_ISNULL, requestParam.getTrainId()));
        if (!lock.tryLock()) {
            return;
        }
        tokenIsNullRefreshExecutor.schedule(() -> {
            try {
                List<Integer> seatTypes = new ArrayList<>();
                Map<Integer, Integer> tokenCountMap = new HashMap<>();
                tokenResult.getTokenIsNullSeatTypeCounts().stream()
                        .map(each -> each.split("_"))
                        .forEach(split -> {
                            int seatType = Integer.parseInt(split[0]);
                            seatTypes.add(seatType);
                            tokenCountMap.put(seatType, Integer.parseInt(split[1]));
                        });
                List<SeatTypeCountDTO> seatTypeCountDTOList = seatService.listSeatTypeCount(Long.parseLong(requestParam.getTrainId()), requestParam.getDeparture(), requestParam.getArrival(), seatTypes);
                for (SeatTypeCountDTO each : seatTypeCountDTOList) {
                    Integer tokenCount = tokenCountMap.get(each.getSeatType());
                    if (tokenCount <= each.getSeatCount()) {
                        ticketAvailabilityTokenBucket.delTokenInBucket(requestParam);
                        break;
                    }
                }
            } finally {
                lock.unlock();
            }
        }, 10, TimeUnit.SECONDS);
    }

    @Override
    public void run(String... args) throws Exception {
        ticketService = ApplicationContextHolder.getBean(TicketService.class);
    }


    @Transactional(rollbackFor = Throwable.class)
    @Override
    public CandidateOrderCreateRespDTO createCandidateOrder(CandidateOrderCreateReqDTO requestParam) {
        String candidateOrderSn = SnowflakeIdUtil.nextIdStr();
        if (CollUtil.isEmpty(requestParam.getPassengers())) {
            throw new ServiceException("候补乘车人不能为空");
        }
        String userId = UserContext.getUserId();

        CandidateOrderDO candidateOrderDO=new CandidateOrderDO();
        candidateOrderDO.setCandidateOrderSn(candidateOrderSn);
        candidateOrderDO.setUserId(userId);
        candidateOrderDO.setTrainId(Long.valueOf(requestParam.getTrainId()));
        candidateOrderDO.setDeparture(requestParam.getDeparture());
        candidateOrderDO.setArrival(requestParam.getArrival());
        candidateOrderDO.setStatus(CandidateOrderStatusEnum.WAITING.getCode());
        candidateOrderDO.setUsername(UserContext.getUsername());
        candidateOrderMapper.insert(candidateOrderDO);

        for (PurchaseTicketPassengerDetailDTO passenger : requestParam.getPassengers()) {
            CandidateOrderItemDO itemDO = new CandidateOrderItemDO();
            itemDO.setCandidateOrderSn(candidateOrderSn);
            itemDO.setPassengerId(passenger.getPassengerId());
            itemDO.setSeatType(passenger.getSeatType());
            itemDO.setStatus(CandidateOrderStatusEnum.WAITING.getCode());

            candidateOrderItemMapper.insert(itemDO);
        }

        CandidateOrderCreateRespDTO response = new CandidateOrderCreateRespDTO();
        response.setCandidateOrderSn(candidateOrderSn);
        response.setStatus(CandidateOrderStatusEnum.WAITING.getCode());
        return response;
    }

    @Override
    public CandidateOrderQueryRespDTO queryCandidateOrder(CandidateOrderQueryReqDTO requestParam) {
        CandidateOrderDO candidateOrderDO= candidateOrderMapper.selectOne(Wrappers.lambdaQuery(CandidateOrderDO.class)
                .eq(CandidateOrderDO::getCandidateOrderSn,
                        requestParam.getCandidateOrderSn())
                .eq(CandidateOrderDO::getUserId,
                        UserContext.getUserId())
        );

        if(candidateOrderDO==null){
            throw new ServiceException("候补订单不存在");
        }

        CandidateOrderQueryRespDTO response= new CandidateOrderQueryRespDTO();
        response.setCandidateOrderSn(candidateOrderDO.getCandidateOrderSn());
        response.setStatus(candidateOrderDO.getStatus());
        response.setFormalOrderSn(candidateOrderDO.getFormalOrderSn());
        response.setFailReason(candidateOrderDO.getFailReason());
        // 补充车次、区间、创建时间等展示信息，便于前端候补页面渲染
        response.setTrainId(candidateOrderDO.getTrainId());
        TrainDO trainDO = trainMapper.selectById(candidateOrderDO.getTrainId());
        response.setTrainNumber(trainDO == null ? null : trainDO.getTrainNumber());
        response.setDeparture(candidateOrderDO.getDeparture());
        response.setArrival(candidateOrderDO.getArrival());
        response.setCreateTime(candidateOrderDO.getCreateTime());
        return response;
    }

    @Override
    public List<CandidateOrderQueryRespDTO> listCandidateOrder() {
        // 查询当前登录用户的全部候补订单，按创建时间倒序排列
        List<CandidateOrderDO> candidateOrderDOList = candidateOrderMapper.selectList(Wrappers.lambdaQuery(CandidateOrderDO.class)
                .eq(CandidateOrderDO::getUserId, UserContext.getUserId())
                .orderByDesc(CandidateOrderDO::getCreateTime));
        if (CollUtil.isEmpty(candidateOrderDOList)) {
            return new ArrayList<>();
        }
        // 批量查询车次号并转为 Map，避免循环内逐条查询数据库
        List<Long> trainIds = candidateOrderDOList.stream()
                .map(CandidateOrderDO::getTrainId)
                .distinct()
                .collect(Collectors.toList());
        Map<Long, String> trainNumberMap = trainMapper.selectBatchIds(trainIds)
                .stream()
                .collect(Collectors.toMap(TrainDO::getId, TrainDO::getTrainNumber, (a, b) -> a));
        return candidateOrderDOList.stream()
                .map(each -> {
                    CandidateOrderQueryRespDTO response = new CandidateOrderQueryRespDTO();
                    response.setCandidateOrderSn(each.getCandidateOrderSn());
                    response.setStatus(each.getStatus());
                    response.setFormalOrderSn(each.getFormalOrderSn());
                    response.setFailReason(each.getFailReason());
                    response.setTrainId(each.getTrainId());
                    response.setTrainNumber(trainNumberMap.get(each.getTrainId()));
                    response.setDeparture(each.getDeparture());
                    response.setArrival(each.getArrival());
                    response.setCreateTime(each.getCreateTime());
                    return response;
                })
                .collect(Collectors.toList());
    }

    @Override
    public void cancelCandidateOrder(CandidateOrderCancelReqDTO requestParam) {
        int affectedRows=candidateOrderMapper.update(null,Wrappers.lambdaUpdate(CandidateOrderDO.class)
                .set(CandidateOrderDO::getStatus,CandidateOrderStatusEnum.CANCELLED.getCode())
                .eq(CandidateOrderDO::getCandidateOrderSn,requestParam.getCandidateOrderSn())
                .eq(CandidateOrderDO::getUserId,UserContext.getUserId())
                .eq(CandidateOrderDO::getStatus,CandidateOrderStatusEnum.WAITING.getCode()));

        if (affectedRows == 0) {
            throw new ServiceException("候补订单不存在、无权操作或当前状态不可取消");
        }
    }
}
