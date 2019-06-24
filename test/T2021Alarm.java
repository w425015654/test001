/** 
* Copyright (C) 2012-2017 ZEEI Inc.All Rights Reserved.
* 项目名称：aas
* 文件名称：T2021Alarm.java
* 包  名  称：com.zeei.das.aas.alarm
* 文件描述：TODO 请修改文件描述
* 创建日期：2017年5月9日下午1:56:46
* 
* 修改历史
* 1.0 quanhongsheng 2017年5月9日下午1:56:46 创建文件
*
*/

package com.zeei.das.aas.alarm;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.zeei.das.aas.AasService;
import com.zeei.das.aas.mq.Publish;
import com.zeei.das.aas.vo.AlarmInfoVO;
import com.zeei.das.aas.vo.StationVO;
import com.zeei.das.common.constants.Constant;
import com.zeei.das.common.constants.DataType;
import com.zeei.das.common.constants.LogType;
import com.zeei.das.common.utils.DateUtil;
import com.zeei.das.common.utils.LoggerUtil;
import com.zeei.das.common.utils.StringUtil;

/**
 * 类 名 称：T2021Alarm 类 描 述：TODO 请修改文件描述 功能描述：TODO 请修改功能描述 创建作者：quanhongsheng
 */
@Component("t2021Alarm")
public class T2021Alarm extends Alarm {
	private static Logger logger = LoggerFactory.getLogger(Alarm.class);

	@Autowired
	Publish publish;

	@Override
	public void alarmHandler(JSONObject data) {

		try {

			String MN = data.getString("MN");

			JSONObject CP = data.getJSONObject("CP");

			Date dataTime = data.getDate("DataTime");

			StationVO station = AasService.stationMap.get(MN);

			if (station != null && !StringUtil.isEmptyOrNull(station.getPointCode())) {

				String pointCode = station.getPointCode();

				Map<String, JSONArray> TCP = new HashMap<String, JSONArray>();

				List<String> cids = new ArrayList<String>();

				for (Object value : CP.values()) {
					JSONArray values = (JSONArray) value;

					String polluteCode = values.getString(0);
					String code = values.getString(1);
					String alarmCode = code;
					if (!StringUtil.isEmptyOrNull(code)) {
						alarmCode = AasService.statusAlarm.get(code);
					}

					if (StringUtil.isEmptyOrNull(code)) {
						alarmCode = code;
					}

					// 根据站点ID，告警码和因子ID，取md5 作为规则的ID
					String alarmId = AlarmIDUtil.generatingAlarmID(pointCode, alarmCode, polluteCode, null);

					TCP.put(alarmId, values);
					cids.add(alarmId);
				}

				// 内存告警id 集合
				@SuppressWarnings("unchecked")
				List<String> alarmIds = (List<String>) AasService.T2021AlarmMap.get(pointCode);

				if (alarmIds == null) {
					alarmIds = new ArrayList<String>();
					AasService.T2021AlarmMap.put(pointCode, alarmIds);
				}

				List<String> result = new ArrayList<String>();
				result.addAll(alarmIds);

				// 告警key集合和站点上报告警key集合差值
				result.removeAll(cids);

				// 站点没有上报的告警，则进行消警处理
				for (String alarmId : result) {

					AlarmInfoVO vo = AasService.alarmMap.get(alarmId);

					if (vo != null) {
						vo.setEndTime(dataTime);
						vo.setNewAlarm(false);
						String json = JSON.toJSONStringWithDateFormat(vo, "yyyy-MM-dd HH:mm:ss",
								SerializerFeature.WriteDateUseDateFormat);

						publish.send(Constant.MQ_QUEUE_ALARM, json);
						publish.send(Constant.MQ_QUEUE_LOGS, LoggerUtil.getLogInfo(LogType.LOG_TYPE_ALARM, vo));

						String info = String.format("站点:%s 2021告警消除[%s|%s]---%s", pointCode, vo.getAlarmCode(),
								vo.getPolluteCode(), DateUtil.dateToStr(dataTime, "yyyy-MM-dd HH:mm:ss"));
						logger.info(info);
						publish.send(Constant.MQ_QUEUE_LOGS, LoggerUtil.getLogInfo(LogType.LOG_TYPE_EXCEPTION, info));
						AasService.alarmMap.remove(alarmId);
					}

					// 删除内存数据
					alarmIds.remove(alarmId);
				}

				result.clear();
				// 站点没有上报的告警 key集合
				result.addAll(cids);

				// 告警key集合和站点上报告警key集合差值
				result.removeAll(alarmIds);

				// 站点上报的告警，在内存中没有告警信息则生成告警
				for (String key : result) {

					JSONArray params = TCP.get(key);
					String polluteCode = params.getString(0);
					String code = params.getString(1);
					String alarmCode = code;

					if (!StringUtil.isEmptyOrNull(code)) {
						alarmCode = AasService.statusAlarm.get(code);
					}
					if (StringUtil.isEmptyOrNull(code)) {
						alarmCode = code;
					}
					// String value = params.getString(2);

					AlarmInfoVO vo = new AlarmInfoVO();
					vo.setAlarmCode(alarmCode);
					// vo.setAlarmValue(value);
					vo.setPointCode(pointCode);
					vo.setStartTime(dataTime);
					vo.setDataType(DataType.T2011);
					vo.setStorage(true);
					vo.setPolluteCode(polluteCode);

					// 根据站点ID，告警码和因子ID，取md5 作为规则的ID
					String alarmId = AlarmIDUtil.generatingAlarmID(pointCode, alarmCode, polluteCode, null);
					alarmIds.add(alarmId);
					AasService.alarmMap.put(alarmId, vo);

					String json = JSON.toJSONStringWithDateFormat(vo, "yyyy-MM-dd HH:mm:ss",
							SerializerFeature.WriteDateUseDateFormat);

					if (!StringUtil.isEmptyOrNull(alarmCode)) {
						publish.send(Constant.MQ_QUEUE_ALARM, json);
					}
					publish.send(Constant.MQ_QUEUE_LOGS, LoggerUtil.getLogInfo(LogType.LOG_TYPE_ALARM, vo));

					String info = String.format("站点:%s 2021告警[%s|%s]---%s", pointCode, alarmCode, polluteCode,
							DateUtil.dateToStr(dataTime, "yyyy-MM-dd HH:mm:ss"));
					logger.info(info);

					publish.send(Constant.MQ_QUEUE_LOGS, LoggerUtil.getLogInfo(LogType.LOG_TYPE_EXCEPTION, info));

				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.toString());
			publish.send(Constant.MQ_QUEUE_LOGS, LoggerUtil.getLogInfo(LogType.LOG_TYPE_EXCEPTION, e.toString()));
		}

	}

}
