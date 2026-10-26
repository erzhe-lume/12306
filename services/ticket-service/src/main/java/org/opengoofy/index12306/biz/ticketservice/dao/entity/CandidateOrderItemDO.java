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

/**
 * 候补订单明细实体，对应 t_candidate_order_item。
 * 一条记录对应候补订单中的一名乘车人，并保存座位类型和分配后的座位信息。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@TableName("t_candidate_order_item")
public class CandidateOrderItemDO extends BaseDO {
    /** 主键。 */
    private Long id;

    /** 关联候补订单号。 */
    private String candidateOrderSn;

    /** 乘车人 ID。 */
    private String passengerId;

    /** 乘车人姓名。 */
    private String realName;

    /** 证件类型。 */
    private Integer idType;

    /** 证件号码。 */
    private String idCard;

    /** 手机号。 */
    private String phone;

    /** 申请的座位类型。 */
    private Integer seatType;

    /** 车票类型。 */
    private Integer ticketType;

    /** 分配后的车厢号。 */
    private String carriageNumber;

    /** 分配后的座位号。 */
    private String seatNumber;

    /** 票价，单位沿用正式订单明细的金额单位。 */
    private Integer amount;

    /** 明细状态。 */
    private Integer status;
}
