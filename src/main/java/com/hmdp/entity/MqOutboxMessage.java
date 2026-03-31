package com.hmdp.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
@EqualsAndHashCode(callSuper = false)
@Accessors(chain = true)
@TableName("tb_mq_outbox")
public class MqOutboxMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    @TableId(value = "id", type = IdType.INPUT)
    private Long id;

    private String bizType;

    private String bizKey;

    private String exchangeName;

    private String routingKey;

    private String payload;

    private Integer status;

    private Integer retryCount;

    private Integer consumeRetryCount;

    private LocalDateTime nextRetryTime;

    private String lastError;

    private LocalDateTime publishedTime;

    private LocalDateTime consumedTime;

    private LocalDateTime compensateTime;

    private LocalDateTime createTime;

    private LocalDateTime updateTime;
}
