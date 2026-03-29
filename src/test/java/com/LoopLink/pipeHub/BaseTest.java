package com.LoopLink.pipeHub;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;

public class BaseTest {
    public static void main(String[] args) {
        String servers = "localhost:9092,null,kafka-01:9092";

        List<String> list = Arrays.asList(StringUtils.split(servers, ","));

        System.out.println(list);
    }
}
