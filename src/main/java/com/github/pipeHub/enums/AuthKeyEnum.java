package com.github.pipeHub.enums;

import lombok.Getter;

@Getter
public enum AuthKeyEnum {
    X_PROXY_TOKEN("X-Proxy-Token") {
        @Override
        public String handleAuth(String authValue) {
            return authValue;
        }
    },

    AUTHORIZATION("Authorization") {
        @Override
        public String handleAuth(String authValue) {
            return "Bearer " + authValue;
        }
    };

    private final String value;

    AuthKeyEnum(String value) {
        this.value = value;
    }

    public static AuthKeyEnum of(String val) {
        if (val == null || val.trim().isEmpty()) {
            return null;
        }

        for (AuthKeyEnum constant : AuthKeyEnum.values()) {
            if (constant.getValue().equalsIgnoreCase(val)) {
                return constant;
            }
        }
        return null;
    }

    public abstract String handleAuth(String authValue);
}
