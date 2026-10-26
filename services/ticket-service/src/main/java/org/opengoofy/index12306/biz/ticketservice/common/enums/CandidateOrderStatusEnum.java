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

package org.opengoofy.index12306.biz.ticketservice.common.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * 候补订单状态。
 */
@Getter
@RequiredArgsConstructor
public enum CandidateOrderStatusEnum {

    /** 等待余票。 */
    WAITING(0),

    /** 正在尝试分配座位。 */
    ALLOCATING(1),

    /** 已分配座位，等待支付。 */
    PENDING_PAYMENT(2),

    /** 已支付，候补完成。 */
    SUCCESS(3),

    /** 系统分配失败或候补条件不满足。 */
    FAILED(4),

    /** 用户主动取消。 */
    CANCELLED(5),

    /** 已分配座位但支付超时。 */
    EXPIRED(6);

    private final Integer code;
}
