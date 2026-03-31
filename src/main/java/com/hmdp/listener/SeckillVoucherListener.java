package com.hmdp.listener;

import cn.hutool.json.JSONUtil;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IMqOutboxMessageService;
import com.hmdp.service.impl.VoucherOrderServiceImpl;
import com.hmdp.utils.RedisConstants;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Arrays;

@Component
@RequiredArgsConstructor
@Slf4j
public class SeckillVoucherListener {

    private static final int MAX_CONSUME_RETRY = 3;

    private static final DefaultRedisScript<Long> COMPENSATE_SCRIPT;
    static {
        COMPENSATE_SCRIPT = new DefaultRedisScript<>();
        COMPENSATE_SCRIPT.setLocation(new ClassPathResource("compensate.lua"));
        COMPENSATE_SCRIPT.setResultType(Long.class);
    }

    @Resource
    VoucherOrderServiceImpl voucherOrderService;
    @Resource
    IMqOutboxMessageService outboxMessageService;
    @Resource
    StringRedisTemplate stringRedisTemplate;
    /**
     * sheng  消费者1
     * @param message
     * @param channel
     * @throws Exception
     */
    @RabbitListener(queues = "QA")
    public void receivedA(Message message, Channel channel) throws Exception {
        long tag = message.getMessageProperties().getDeliveryTag();
        Long outboxId = null;
        try {
            String msg = new String(message.getBody());
            VoucherOrder voucherOrder = JSONUtil.toBean(msg, VoucherOrder.class);
            outboxId = resolveOutboxId(message, voucherOrder);
            if (outboxId != null && !outboxMessageService.markConsuming(outboxId)) {
                channel.basicAck(tag, false);
                return;
            }
            try {
                voucherOrderService.createVoucherOrder(voucherOrder);
            } catch (Exception e) {
                log.error("检测到一人多单，已拦截 {}", voucherOrder.getId(), e);
                
            } // 原子业务
            if (outboxId != null) {
                outboxMessageService.markConsumed(outboxId);
            }
            channel.basicAck(tag, false);
        } catch (Exception e) {
            log.error("消费QA失败", e);
            if (outboxId != null) {
                outboxMessageService.markConsumeFailed(outboxId, e.getMessage());
            }
            channel.basicNack(tag, false, true);
        }
    }

    /**
     * sheng  消费者2
     * @param message
     * @throws Exception
     */
    @RabbitListener(queues = "QD")
    public void receivedD(Message message, Channel channel) throws Exception {
        long tag = message.getMessageProperties().getDeliveryTag();
        Long outboxId = null;
        VoucherOrder voucherOrder = null;
        try {
            String msg = new String(message.getBody());
            voucherOrder = JSONUtil.toBean(msg, VoucherOrder.class);
            outboxId = resolveOutboxId(message, voucherOrder);
            if (outboxId != null && !outboxMessageService.markConsuming(outboxId)) {
                channel.basicAck(tag, false);
                return;
            }
            try {
                voucherOrderService.createVoucherOrder(voucherOrder);
            } catch (Exception e) {
                log.error("检测到一人多单，已拦截 {}", voucherOrder.getId(), e);
            }
            if (outboxId != null) {
                outboxMessageService.markConsumed(outboxId);
            }
            channel.basicAck(tag, false);
        } catch (Exception e) {
            log.error("消费QD失败", e);
            if (outboxId == null) {
                channel.basicNack(tag, false, false);
                return;
            }
            int retry = outboxMessageService.markConsumeFailedAndGetRetry(outboxId, e.getMessage());
            if (retry <= MAX_CONSUME_RETRY) {
                outboxMessageService.replay(outboxId);
                channel.basicAck(tag, false);
                return;
            }
            if (voucherOrder != null) {
                String stockKey = RedisConstants.SECKILL_STOCK_KEY + voucherOrder.getVoucherId();
                String orderKey = "seckill:order:" + voucherOrder.getVoucherId();
                stringRedisTemplate.execute(
                        COMPENSATE_SCRIPT,
                        Arrays.asList(stockKey, orderKey),
                        voucherOrder.getUserId().toString()
                );
            }
            outboxMessageService.compensate(outboxId, "consume_failed_compensated");
            channel.basicAck(tag, false);
        }
    }

    private Long resolveOutboxId(Message message, VoucherOrder voucherOrder) {
        String messageId = message.getMessageProperties().getMessageId();
        if (messageId != null) {
            return Long.valueOf(messageId);
        }
        if (voucherOrder == null) {
            return null;
        }
        return voucherOrder.getId();
    }
}
