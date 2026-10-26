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

package org.opengoofy.index12306.biz.ticketservice.dao.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.opengoofy.index12306.framework.starter.database.base.BaseDO;

import java.util.Date;

/**
 * 候补订单实体。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@TableName("t_candidate_order")
public class CandidateOrderDO extends BaseDO {

    private Long id;

    /** 候补订单号。 */
    private String candidateOrderSn;

    /** 分配成功后关联的正式订单号。 */
    private String formalOrderSn;

    /** 用户上下文中的用户 ID，项目统一按字符串传递。 */
    private String userId;

    private String username;

    private Long trainId;

    private String departure;

    private String arrival;

    private Integer status;

    /** 开始尝试分配的时间。 */
    private Date allocateTime;

    /** 候补分配成功后的支付截止时间。 */
    private Date paymentDeadline;

    /** 失败或过期原因。 */
    private String failReason;
}
