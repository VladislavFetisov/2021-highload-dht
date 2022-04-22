package ru.mail.polis.service.vladislav_fetisov.topology;

public enum Range {
    INT(Integer.MAX_VALUE),
    LONG(Long.MAX_VALUE);

    Range(long maxValue) {
        this.maxValue = maxValue;
    }
    public final long maxValue;
}
