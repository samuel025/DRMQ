package com.drmq.broker.persistence;

import java.io.IOException;

public class CorruptRecordException extends IOException {
    public CorruptRecordException(String message) {
        super(message);
    }
    public CorruptRecordException(String message, Throwable cause) {
        super(message, cause);
    }
}
