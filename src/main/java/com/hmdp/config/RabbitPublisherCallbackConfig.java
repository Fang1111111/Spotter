package com.hmdp.config;

import com.hmdp.service.IMqOutboxMessageService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

@Slf4j
@Component
public class RabbitPublisherCallbackConfig implements RabbitTemplate.ConfirmCallback, RabbitTemplate.ReturnCallback {

    @Resource
    private RabbitTemplate rabbitTemplate;

    @Resource
    private IMqOutboxMessageService outboxMessageService;

    @PostConstruct
    public void init() {
        rabbitTemplate.setMandatory(true);
        rabbitTemplate.setConfirmCallback(this);
        rabbitTemplate.setReturnCallback(this);
    }

    @Override
    public void confirm(CorrelationData correlationData, boolean ack, String cause) {
        if (correlationData == null || correlationData.getId() == null) {
            return;
        }
        Long outboxId = Long.valueOf(correlationData.getId());
        if (ack) {
            outboxMessageService.markPublished(outboxId);
            return;
        }
        outboxMessageService.markPublishFailed(outboxId, cause);
        log.error("消息发布失败，outboxId={}, cause={}", outboxId, cause);
    }

    @Override
    public void returnedMessage(Message message, int replyCode, String replyText, String exchange, String routingKey) {
        if (message == null || message.getMessageProperties() == null) {
            return;
        }
        String messageId = message.getMessageProperties().getMessageId();
        if (messageId == null) {
            return;
        }
        Long outboxId = Long.valueOf(messageId);
        String reason = replyCode + ":" + replyText + ", exchange=" + exchange + ", routingKey=" + routingKey;
        outboxMessageService.markReturned(outboxId, reason);
        log.error("消息路由失败并退回，outboxId={}, reason={}", outboxId, reason);
    }
}
