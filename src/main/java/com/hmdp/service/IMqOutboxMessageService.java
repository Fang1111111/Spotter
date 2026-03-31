package com.hmdp.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.hmdp.entity.MqOutboxMessage;
import com.hmdp.entity.VoucherOrder;

public interface IMqOutboxMessageService extends IService<MqOutboxMessage> {

    void createAndSendVoucherOrderOutbox(VoucherOrder voucherOrder, String payload, String exchangeName, String routingKey);

    void markPublished(Long outboxId);

    void markPublishFailed(Long outboxId, String errorMsg);

    void markReturned(Long outboxId, String errorMsg);

    boolean markConsuming(Long outboxId);

    void markConsumed(Long outboxId);

    void markConsumeFailed(Long outboxId, String errorMsg);

    void markDead(Long outboxId, String errorMsg);

    int markConsumeFailedAndGetRetry(Long outboxId, String errorMsg);

    boolean replay(Long outboxId);

    void compensate(Long outboxId, String reason);
}
