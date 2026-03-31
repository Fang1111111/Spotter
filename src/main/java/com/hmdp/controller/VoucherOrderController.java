package com.hmdp.controller;


import com.hmdp.dto.Result;
import com.hmdp.service.IMqOutboxMessageService;
import com.hmdp.service.IVoucherOrderService;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * <p>
 *  前端控制器
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@RestController
@RequestMapping("/voucher-order")
public class VoucherOrderController {
    @Resource
    private IVoucherOrderService voucherOrderService;
    @Resource
    private IMqOutboxMessageService outboxMessageService;

    @PostMapping("seckill/{id}")
    public Result seckillVoucher(@PathVariable("id") Long voucherId) {
        return voucherOrderService.seckillVoucher(voucherId);
    }

    @PostMapping("outbox/replay/{id}")
    public Result replay(@PathVariable("id") Long outboxId) {
        boolean replayed = outboxMessageService.replay(outboxId);
        if (!replayed) {
            return Result.fail("outbox消息不存在");
        }
        return Result.ok();
    }

    @PostMapping("outbox/compensate/{id}")
    public Result compensate(@PathVariable("id") Long outboxId,
                             @RequestParam(value = "reason", defaultValue = "manual_compensate") String reason) {
        outboxMessageService.compensate(outboxId, reason);
        return Result.ok();
    }
}
