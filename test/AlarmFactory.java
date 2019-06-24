/** 
* Copyright (C) 2012-2017 ZEEI Inc.All Rights Reserved.
* 项目名称：aas
* 文件名称：AlarmFactory.java
* 包  名  称：com.zeei.das.aas.alarm
* 文件描述：告警采集适配器
* 创建日期：2017年4月21日上午8:10:43
* 
* 修改历史
* 1.0 quanhongsheng 2017年4月21日上午8:10:43 创建文件
*
*/

package com.zeei.das.aas.alarm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zeei.das.aas.AasService;
import com.zeei.das.aas.alarm.custom.AqiAlarm;
import com.zeei.das.aas.alarm.custom.FlagAlarm;
import com.zeei.das.aas.alarm.custom.FlowAlarm;
import com.zeei.das.aas.alarm.custom.FrequencyAlarm;
import com.zeei.das.aas.alarm.custom.QGAlarm;
import com.zeei.das.aas.alarm.custom.SlowAlarm;
import com.zeei.das.aas.alarm.custom.YSAlarm;
import com.zeei.das.aas.mq.Publish;
import com.zeei.das.aas.vo.StationVO;
import com.zeei.das.common.constants.Constant;
import com.zeei.das.common.constants.LogType;
import com.zeei.das.common.constants.T212Code;
import com.zeei.das.common.utils.DateUtil;
import com.zeei.das.common.utils.LoggerUtil;
import com.zeei.das.common.utils.StringUtil;

/**
 * 类 名 称：AlarmFactory 类 描 述：告警采集适配器 功能描述：告警采集适配器 创建作者：quanhongsheng
 */
@Component("alarmFactory")
public class AlarmFactory {

	private static Logger logger = LoggerFactory.getLogger(Alarm.class);

	@Autowired
	Publish publish;

	@Autowired
	T212Alarm t212Alarm;

	@Autowired
	ServiceAlarm serviceAlarm;

	@Autowired
	T2021Alarm t2021Alarm;

	@Autowired
	T3020Alarm t3020Alarm;

	@Autowired
	QGAlarm qgAlarm;

	@Autowired
	YSAlarm ysAlarm;

	@Autowired
	SlowAlarm slowAlarm;
	
	@Autowired
	FlowAlarm flowAlarm;
	
	@Autowired
	FlagAlarm flagAlarm;

	@Autowired
	AqiAlarm aqiAlarm;
	
	@Autowired
	FrequencyAlarm frequencyAlarm;

	public boolean alarmHandler(String data) {

		try {

			JSONObject map = JSON.parseObject(data);

			String CN = map.getString("CN");
			String MN = map.getString("MN");

			// 接收到心跳检测的数据包 ， 更新站点接收数据时间,
			if (!StringUtil.isEmptyOrNull(MN) && map.containsKey("HT")) {
				 upDataTime(map);
			}

			switch (CN) {
			case T212Code.T2011:
			case T212Code.T2051:
			case T212Code.T2061:
				//对原始数据及分钟小时数据   计算周期
				//frequencyAlarm.alarmHandler(map);
			case T212Code.T2031:
				//logger.info(data);
				t212Alarm.alarmHandler(map);
				qgAlarm.alarmHandler(map);
				flagAlarm.alarmHandler(map);
				ysAlarm.alarmHandler(map);
				
				//缓慢告警
				slowAlarm.alarmHandler(map);
				//流量告警
				flowAlarm.alarmHandler(map);
				break;
			case T212Code.T2021:
				t2021Alarm.alarmHandler(map);
				break;
			case T212Code.T3020:
				t3020Alarm.alarmHandler(map);
				break;
			case T212Code.TAQI:
				aqiAlarm.alarmHandler(map);
				break;
			default:
				serviceAlarm.alarmHandler(map);
				break;
			}

		} catch (Exception e) {			
			logger.error("",e);
			publish.send(Constant.MQ_QUEUE_LOGS, LoggerUtil.getLogInfo(LogType.LOG_TYPE_EXCEPTION, e.toString()));
		}
		return true;
	}

	/**
	 * 
	 * upDataTime:更新站点最新数据时间
	 *
	 * @param MN
	 *            void
	 */
	public void upDataTime(JSONObject data) {

		String MN = data.getString("MN");

		StationVO station = AasService.stationMap.get(MN);

		if (station != null) {
			station.setHeartTime(DateUtil.getCurrentDate());
		}
	}

}
