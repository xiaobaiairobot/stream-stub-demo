package com.yunli.bigdata.eventbus.sdk.operator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.google.gson.Gson;
import com.yunli.bigdata.eventbus.sdk.config.OperatorConfig;
import com.yunli.bigdata.eventbus.sdk.operator.rule.config.SpringELRuleConfig;
import com.yunli.bigdata.eventbus.sdk.operator.sink.DemoSink;
import com.yunli.bigdata.eventbus.sdk.operator.sink.SinkType;
import com.yunli.bigdata.eventbus.sdk.operator.sink.config.HiveSinkConfig;
import com.yunli.bigdata.eventbus.sdk.operator.sink.config.KafkaSinkConfig;
import com.yunli.bigdata.eventbus.sdk.operator.sink.config.SinkConfig;
import com.yunli.bigdata.eventbus.sdk.operator.source.DemoTopicParser;
import com.yunli.bigdata.eventbus.sdk.operator.source.ParserType;
import com.yunli.bigdata.eventbus.sdk.operator.source.config.ParserConfig;
import com.yunli.bigdata.eventbus.sdk.util.ObjectUtil;
import com.yunli.bigdata.eventbus.sdk.util.TypeConvertUtil;

/**
 * 用于生成配置
 * @author david
 * @date 2020/8/19 7:04 下午
 */
public class OperatorConfigTest {

  @Test
  public void testGenerateConfig() {
    Gson gson = new Gson();

    OperatorConfig csvParser = new OperatorConfig();
    csvParser.setType(OperatorType.PARSER);
    ParserConfig parserConfig = new ParserConfig();
    parserConfig.setType(ParserType.CUSTOM);
    parserConfig.setClassName(DemoTopicParser.class.getName());
    csvParser.setConfig(ObjectUtil.objToMap(parserConfig));
    List<OperatorConfig> childRule = new ArrayList<>();

    OperatorConfig ruleParser = new OperatorConfig();
    ruleParser.setType(OperatorType.RULE);
    SpringELRuleConfig ruleConfig = new SpringELRuleConfig();
    ruleConfig.setExpression(" #biz_type = '3' ");
    ruleParser.setConfig(ObjectUtil.objToMap(ruleConfig));
    List<OperatorConfig> childSink = new ArrayList<>();

    OperatorConfig sinkPrint = new OperatorConfig();
    sinkPrint.setType(OperatorType.SINK);
    SinkConfig sinkPrintConfig = new SinkConfig();
    sinkPrintConfig.setType(SinkType.CUSTOM);
    sinkPrintConfig.setClassName(DemoSink.class.getName());
    Map<String, Object> mapPrint = new HashMap<>();
    mapPrint.put("customContent", "myselfConfigContent");
    sinkPrintConfig.setConfig(mapPrint);
    sinkPrint.setConfig(ObjectUtil.objToMap(sinkPrintConfig));
    childSink.add(sinkPrint);

    ruleParser.setChildren(childSink);
    childRule.add(ruleParser);
    csvParser.setChildren(childRule);
    // csvParser.setChildren(childRule);

    String json = gson.toJson(csvParser);
    System.out.println(json);
    System.out.println("---------");
    OperatorConfig convertConfig = gson.fromJson(json, OperatorConfig.class);
    System.out.println(convertConfig.toString());
  }
}
