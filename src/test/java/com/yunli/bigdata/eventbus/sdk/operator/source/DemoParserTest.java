package com.yunli.bigdata.eventbus.sdk.operator.source;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.google.gson.Gson;
import com.yunli.bigdata.eventbus.sdk.message.ColumnDesc;
import com.yunli.bigdata.eventbus.sdk.message.ColumnType;
import com.yunli.bigdata.eventbus.sdk.message.Row;
import com.yunli.bigdata.eventbus.sdk.message.RowSet;
import com.yunli.bigdata.eventbus.sdk.message.Schema;

/**
 * @author david
 * @date 2020/7/23 11:30 上午
 */
public class DemoParserTest {

  /**
   * GPRS清单数据解析
   * @throws Exception
   */
  @Test
  public void testParser() throws Exception {
    String strData = "GPP31_0000000GJSJ0099000000202006191347200UXX.0.a_29707.out.C.2006.20200619134726,0,0,\n"
        + "31,3,FT3801533230.15502131358.4262133759.147830339.1,0,,0619134719,11,0,15502130000,460012130428216,116.79.206.104,0,59285,0,209915659,2319507074,124.160.223.36,3GNET,,3,,0,46001,0,,021,0571,,3,2011,0,4,,8683940330590000,20200619,132710,340,4262133759,,340,22006,15254,0,0,X,,37888,1|0|3070000|3071101|12;1|3120042491196432|43100660|5501400|0,0|1|202006|0|0|99997|37260;0|1|202006|3120042491196432|0|18106|37260;0|1|202006|3120042491196432|0|2240026|37260;3120042468785019|2|0|3120042491196432|20200601000000|40026|37260:10060|0,0,12,0,0,0,0,,DCC_31_20200619134714_2844130071_2844131270,0,3118032288770000,3120042468780000,90331533,4G00,24002,312531,0,20180322161549,,,002,002,0,1,0619134719,744FCE68,,1,6,,1,,0,31,,,1|0|3120042468785019|0021|202006|20200619132710|20200619133250|43100660|40026|3120042491196432|24811472748|24811510008|999999999999999|0|0|90331533|51964792|10060|0;6|0|3120042468785019|0021|202006|20200619132710|20200619133250|23994678683|23994715943|0|0|0|0|0|0|0|0|0|0;7|0|0|0|90331533|202006|24811472748|24811510008|46170898432|0|0|0|0|0|0,\n"
        + "31,3,FT3801533867.15502131358.4262133759.147830342.1,0,,0619134719,11,0,15502130000,460012130428216,116.79.206.104,0,59285,0,209915659,2319507074,124.160.223.36,3GNET,,3,,0,46001,0,,021,0571,,3,2011,0,4,,8683940330590000,20200619,133747,95,4262133759,,95,3775,4901,0,0,X,,9216,1|0|3070000|3071101|3;1|3120042491196432|43100660|5501400|0,0|1|202006|0|0|99997|8676;0|1|202006|3120042491196432|0|18106|8676;0|1|202006|3120042491196432|0|2240026|8676;3120042468785019|2|0|3120042491196432|20200601000000|40026|8676:10060|0,0,3,0,0,0,0,,DCC_31_20200619134714_2844130071_2844131270,0,3118032288770000,3120042468780000,90331533,4G00,24002,312531,0,20180322161549,,,002,002,0,1,0619134719,744FCE68,,1,6,,1,,0,31,,,1|0|3120042468785019|0021|202006|20200619133747|20200619133922|43100660|40026|3120042491196432|24811512519|24811521195|999999999999999|0|0|90331533|51964792|10060|0;6|0|3120042468785019|0021|202006|20200619133747|20200619133922|23994718454|23994727130|0|0|0|0|0|0|0|0|0|0;7|0|0|0|90331533|202006|24811512519|24811521195|46170898432|0|0|0|0|0|0,";
    Schema schema = new Schema();
    schema.setColumns(generateColumns());
    DemoTopicParser parser = new DemoTopicParser(schema, "");
    RowSet rowSet = parser.parse(strData.getBytes());
    int index = 0;
    for (Row row : rowSet) {
      Assert.assertEquals("3",row.getString(1));
      if(index == 0){
        Assert.assertEquals("1|0|3120042468785019|0021|202006|20200619132710|20200619133250|43100660|40026|3120042491196432|24811472748|24811510008|999999999999999|0|0|90331533|51964792|10060|0;6|0|3120042468785019|0021|202006|20200619132710|20200619133250|23994678683|23994715943|0|0|0|0|0|0|0|0|0|0;7|0|0|0|90331533|202006|24811472748|24811510008|46170898432|0|0|0|0|0|0",row.getString(84));
      }else if(index == 1){
        Assert.assertEquals("1|0|3120042468785019|0021|202006|20200619133747|20200619133922|43100660|40026|3120042491196432|24811512519|24811521195|999999999999999|0|0|90331533|51964792|10060|0;6|0|3120042468785019|0021|202006|20200619133747|20200619133922|23994718454|23994727130|0|0|0|0|0|0|0|0|0|0;7|0|0|0|90331533|202006|24811512519|24811521195|46170898432|0|0|0|0|0|0",row.getString(84));
      }
      index++;
//      System.out.println(
//          schema.getColumns().get(1).getColumnName() + "/" + schema.getColumns().get(1).getColumnType() + ":" + row
//              .getString(1));
//      System.out.println(
//          schema.getColumns().get(84).getColumnName() + "/" + schema.getColumns().get(84).getColumnType() + ":" + row
//              .getString(84));
    }
  }


  private List<ColumnDesc> generateColumns() {
    String columnName = "source_type,biz_type,fid,rr_flag,file_name,deal_time,record_type,ni_pdp,msisdn,imsi_number,sgsn,msnc,lac,ra,cell_id,charging_id,ggsn,apnni,apnoi,pdp_type,spa,sgsn_change,sgsn_plmn_id,cause_close,result,home_area_code,visit_area_code,city_code,visit_area_hometype,user_type,fee_type,roam_type,service_type,imei,start_date,start_time,call_duration,serv_id,serv_group,serv_duration,data_up1,data_down1,data_up2,data_down2,charged_item,charged_operation,charged_units,free_code,bill_item,cfee_org,cfee,dis_cfee,dfee_org,dfee,dis_dfee,recordseqnum,file_no,error_code,cust_id,user_id,a_product_id,a_serv_type,channel_no,office_code,doublemode,open_datetime,a_user_stat,inter_gprsgroup,apn_group,apn_type,tariff_fee,rate_times,indb_time,reserver1,reserver2,reserver3,reserver4,reserver5,reserver6,reserver7,reserver8,PROVINCE_CODE,RATE_TYPE,RESOURCELIST,SMSREMIND";
    String columnType = "string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,string,long,string,string,long,long,long,long,long,string,string,string,string,string,long,long,long,long,long,long,string,string,long,long,long,long,string,long,string,string,string,string,string,string,string,long,long,string,string,string,string,string,string,string,string,string,string,long,string,string";
    String[] listColumn = columnName.split(",");
    String[] listType = columnType.split(",");
    List<ColumnDesc> list = new ArrayList<>();
    for (int i = 0; i < listColumn.length; i++) {
      String type = listType[i];
      list.add(new ColumnDesc(listColumn[i].toLowerCase(), type.equals("string") ? ColumnType.STRING : ColumnType.LONG));
    }
    Gson gson = new Gson();
    System.out.println(gson.toJson(list));
    return list;
  }
}
