package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.entity.MqOutboxMessage;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.MqOutboxMessageMapper;
import com.hmdp.service.IMqOutboxMessageService;
import com.hmdp.utils.OutboxStatus;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Service
public class MqOutboxMessageServiceImpl extends ServiceImpl<MqOutboxMessageMapper, MqOutboxMessage>
        implements IMqOutboxMessageService {

    private static final String BIZ_TYPE_VOUCHER_ORDER = "voucher_order";

    @Resource
    private RabbitTemplate rabbitTemplate;

    @Override
    @Transactional
    public void createAndSendVoucherOrderOutbox(VoucherOrder voucherOrder, String payload, String exchangeName, String routingKey) {
        MqOutboxMessage outbox = new MqOutboxMessage()
                .setId(voucherOrder.getId())
                .setBizType(BIZ_TYPE_VOUCHER_ORDER)
                .setBizKey(String.valueOf(voucherOrder.getId()))
                .setExchangeName(exchangeName)
                .setRoutingKey(routingKey)
                .setPayload(payload)
                .setStatus(OutboxStatus.INIT)
                .setRetryCount(0)
                .setNextRetryTime(LocalDateTime.now());
        saveOrUpdate(outbox);
        send(outbox);
    }

    @Override
    public void markPublished(Long outboxId) {
        update()
                .eq("id", outboxId)
                .set("status", OutboxStatus.PUBLISHED)
                .set("last_error", null)
                .set("published_time", LocalDateTime.now())
                .update();
    }

    @Override
    public void markPublishFailed(Long outboxId, String errorMsg) {
        MqOutboxMessage outbox = getById(outboxId);
        if (outbox == null) {
            return;
        }
        int retryCount = outbox.getRetryCount() == null ? 0 : outbox.getRetryCount();
        int nextRetrySeconds = Math.min(60, (int) Math.pow(2, Math.min(6, retryCount + 1)));
        update()
                .eq("id", outboxId)
                .set("status", OutboxStatus.PUBLISH_FAILED)
                .set("retry_count", retryCount + 1)
                .set("next_retry_time", LocalDateTime.now().plusSeconds(nextRetrySeconds))
                .set("last_error", cut(errorMsg))
                .update();
    }

    @Override
    public void markReturned(Long outboxId, String errorMsg) {
        update()
                .eq("id", outboxId)
                .set("status", OutboxStatus.RETURNED)
                .set("last_error", cut(errorMsg))
                .set("next_retry_time", LocalDateTime.now().plusSeconds(5))
                .update();
    }

    @Override
    public boolean markConsuming(Long outboxId) {
        return update()
                .eq("id", outboxId)
                .in("status", OutboxStatus.PUBLISHED, OutboxStatus.RETURNED, OutboxStatus.CONSUME_FAILED, OutboxStatus.DEAD)
                .set("status", OutboxStatus.CONSUMING)
                .update();
    }

    @Override
    public void markConsumed(Long outboxId) {
        update()
                .eq("id", outboxId)
                .set("status", OutboxStatus.CONSUMED)
                .set("last_error", null)
                .set("consumed_time", LocalDateTime.now())
                .update();
    }

    @Override
    public void markConsumeFailed(Long outboxId, String errorMsg) {
        update()
                .eq("id", outboxId)
                .set("status", OutboxStatus.CONSUME_FAILED)
                .set("last_error", cut(errorMsg))
                .update();
    }

    @Override
    public int markConsumeFailedAndGetRetry(Long outboxId, String errorMsg) {
        MqOutboxMessage outbox = getById(outboxId);
        if (outbox == null) {
            return 0;
        }
        int retryCount = outbox.getConsumeRetryCount() == null ? 0 : outbox.getConsumeRetryCount();
        int nextRetry = retryCount + 1;
        update()
                .eq("id", outboxId)
                .set("status", OutboxStatus.CONSUME_FAILED)
                .set("consume_retry_count", nextRetry)
                .set("last_error", cut(errorMsg))
                .update();
        return nextRetry;
    }

    @Override
    public void markDead(Long outboxId, String errorMsg) {
        update()
                .eq("id", outboxId)
                .set("status", OutboxStatus.DEAD)
                .set("last_error", cut(errorMsg))
                .update();
    }

    @Override
    public boolean replay(Long outboxId) {
        MqOutboxMessage outbox = getById(outboxId);
        if (outbox == null) {
            return false;
        }
        send(outbox);
        return true;
    }

    @Override
    public void compensate(Long outboxId, String reason) {
        update()
                .eq("id", outboxId)
                .set("status", OutboxStatus.COMPENSATED)
                .set("last_error", cut(reason))
                .set("compensate_time", LocalDateTime.now())
                .update();
    }

    @Scheduled(fixedDelay = 10000)
    public void retryPublish() {
        List<MqOutboxMessage> list = lambdaQuery()
                .in(MqOutboxMessage::getStatus, OutboxStatus.INIT, OutboxStatus.PUBLISH_FAILED, OutboxStatus.RETURNED)
                .le(MqOutboxMessage::getNextRetryTime, LocalDateTime.now())
                .last("limit 50")
                .list();
        for (MqOutboxMessage message : list) {
            send(message);
        }
    }

    private void send(MqOutboxMessage outbox) {
        try {
            Message message = MessageBuilder
                    .withBody(outbox.getPayload().getBytes(StandardCharsets.UTF_8))
                    .setContentType("application/json")
                    .setContentEncoding(StandardCharsets.UTF_8.name())
                    .setDeliveryMode(MessageDeliveryMode.PERSISTENT)
                    .setMessageId(String.valueOf(outbox.getId()))
                    .build();
            CorrelationData correlationData = new CorrelationData(String.valueOf(outbox.getId()));
            rabbitTemplate.send(outbox.getExchangeName(), outbox.getRoutingKey(), message, correlationData);
        } catch (AmqpException e) {
            markPublishFailed(outbox.getId(), e.getMessage());
            log.error("发送 outbox 消息失败, outboxId={}", outbox.getId(), e);
        }
    }

    private String cut(String errorMsg) {
        if (errorMsg == null) {
            return null;
        }
        if (errorMsg.length() <= 500) {
            return errorMsg;
        }
        return errorMsg.substring(0, 500);
    }
}
