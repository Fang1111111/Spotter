package com.hmdp.utils;

public final class OutboxStatus {
    private OutboxStatus() {
    }

    public static final int INIT = 0;
    public static final int PUBLISHED = 1;
    public static final int PUBLISH_FAILED = 2;
    public static final int RETURNED = 3;
    public static final int CONSUMED = 4;
    public static final int DEAD = 5;
    public static final int COMPENSATED = 6;
    public static final int CONSUMING = 7;
    public static final int CONSUME_FAILED = 8;
}
