package com.datastreaming;

import lombok.*;

public class SamplePayload {
    public SamplePayload(String message) {
        this.message = message;
    }

    @Setter
    @Getter
    private String message;
}
