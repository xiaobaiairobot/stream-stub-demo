package com.yunli.bigdata.eventbus.sdk.operator.source;

import java.io.StringReader;
import java.text.SimpleDateFormat;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yunli.bigdata.eventbus.sdk.common.MessageCode;
import com.yunli.bigdata.eventbus.sdk.common.ParseException;
import com.yunli.bigdata.eventbus.sdk.message.ColumnType;
import com.yunli.bigdata.eventbus.sdk.message.DefaultRowSet;
import com.yunli.bigdata.eventbus.sdk.message.Row;
import com.yunli.bigdata.eventbus.sdk.message.RowSet;
import com.yunli.bigdata.eventbus.sdk.message.Schema;

/**
 * @author david
 * @date 2020/8/19 5:16 下午
 */
public class DemoTopicParser extends AbstractParser {

  private static final Logger logger = LoggerFactory.getLogger(DemoTopicParser.class);

  public DemoTopicParser(Schema schema, String config) {
    super(schema, config);
  }

  // 范例数据见测试用例：DemoParserTest
  // 说明：这是一条订单明细消息，第一行为文件元数据，包含文件名等信息，从第二行开始为具体数据
  @Override
  public RowSet parse(byte[] data) throws Exception {
    String strMsg = new String(data);
    logger.info("总线中接收到的消息内容为:{}", strMsg);
    CSVFormat format = CSVFormat.DEFAULT;
    CSVParser csvParser = new CSVParser(new StringReader(strMsg), format);
    DefaultRowSet rowSet = new DefaultRowSet(this.schema);
    int i = 0;
    for (CSVRecord next : csvParser) {
      if (i == 0) {
        transportHeader(next);
      } else {
        rowSet.addRow(transportOneRecord(next));
      }
      i++;
    }
    return rowSet;
  }


  private void transportHeader(CSVRecord record) {
    if (record.size() >= 3) {
      String fileName = record.get(0);
      Integer sourceType = Integer.valueOf(record.get(1));
      Integer isLast = Integer.valueOf(record.get(2));
      logger.info("文件：{}，类别：{}，是否最后一个:{}", fileName, sourceType, isLast);
    }
  }

  private Row transportOneRecord(CSVRecord record) {
    Row row = new Row(this.schema);
    for (int i = 0; i < this.schema.getColumnsSize(); i++) {
      String value = record.get(i);
      // 根据writer端类型配置做类型转换
      try {
        if (StringUtils.isBlank(value)) {
          row.set(i, null);
        } else {
          ColumnType type = this.schema.getColumns().get(i).getColumnType();
          switch (type) {
            case INT:
              row.setInt(i, Integer.valueOf(value));
              break;
            case LONG:
              row.setLong(i, Long.valueOf(value));
              break;
            case FLOAT:
              row.setFloat(i, Float.valueOf(value));
              break;
            case DOUBLE:
              row.setDouble(i, Double.valueOf(value));
              break;
            case STRING:
              row.setString(i, value);
              break;
            case BOOLEAN:
              row.setBoolean(i, Boolean.valueOf(value));
              break;
            case DATE:
              SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
              row.setDatetime(i, new java.sql.Date(fmt.parse(value).getTime()));
              break;
            default:
              throw new ParseException(MessageCode.ERROR_1003, this.schema.getColumns().get(i));
          }
        }
      } catch (Exception e) {
        throw new ParseException(MessageCode.ERROR_1004, e.getMessage());
      }
    }
    return row;
  }

  @Override
  public void close() {

  }
}
