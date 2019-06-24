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

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.zeei.das.aas.AasService;
import com.zeei.das.aas.mq.Publish;
import com.zeei.das.aas.vo.AlarmInfoVO;
import com.zeei.das.aas.vo.T3020RuleVO;
import com.zeei.das.common.constants.Constant;
import com.zeei.das.common.constants.DataType;
import com.zeei.das.common.constants.LogType;
import com.zeei.das.common.utils.DateUtil;
import com.zeei.das.common.utils.LoggerUtil;
import com.zeei.das.common.utils.Md5Util;
import com.zeei.das.common.utils.StringUtil;

/**
 * @类 名 称：T2021Alarm
 * @类 描 述：提取设备采样时间周期
 * @功能描述：提取设备采样时间周期
 * @创建作者：quanhongsheng
 */
@Component("t3020Alarm")
public class T3020Alarm extends Alarm {
	private static Logger logger = LoggerFactory.getLogger(T3020Alarm.class);

	@Autowired
	Publish publish;

	@Override
	public void alarmHandler(JSONObject data) {

		try {

			String pointCode = data.getString("ID");
			String MN = data.getString("MN");
			JSONObject CP = data.getJSONObject("CP");

			// 获取站点配置告警
			List<String> cr = AasService.stationAlarm.get(MN);

			if (cr != null) {

				if (CP != null && CP.size() > 0) {

					String dataTimeStr = CP.getString("DataTime");

					Date dataTime = DateUtil.strToDate(dataTimeStr, null);

					@SuppressWarnings("unchecked")
					Map<String, Map<String, Object>> items = (Map<String, Map<String, Object>>) CP.get("Item");

					if (items != null && items.size() > 0) {

						for (Map.Entry<String, Map<String, Object>> map : items.entrySet()) {

							String polluteCode = map.getKey();
							Map<String, Object> item = map.getValue();

							if (item != null) {
								String overValue = (String) item.get("Overvalue");

								for (Entry<String, Object> entry : item.entrySet()) {

									String code = entry.getKey();

									String value = (String) entry.getValue();

									String ruleid = Md5Util.getMd5(polluteCode + code);

									T3020RuleVO rule = AasService.t3020AlarmRule.get(ruleid);

									if (rule != null) {

										boolean result = false;

										if (rule.getFormula().equalsIgnoreCase(value)) {
											result = true;
										}

										generationAlarm(result, rule, pointCode, dataTime, polluteCode, overValue);
									} else {
										logger.error("3020告警规则不存在!因子:" + polluteCode + " 状态码:" + code);
									}

								}
							}

						}
					}
				} else {
					logger.error("消息体CP为空:" + data.toJSONString());
				}
			}

		} catch (Exception e) {
			logger.error(e.toString());
			publish.send(Constant.MQ_QUEUE_LOGS, LoggerUtil.getLogInfo(LogType.LOG_TYPE_EXCEPTION, e.toString()));
		}

	}

	/**
	 * 
	 * generationAlarm:告警生成器
	 *
	 * @param result
	 *            数据分析结果
	 * @param rule
	 *            告警规则实体
	 * 
	 *            void
	 */
	private void generationAlarm(boolean result, T3020RuleVO rule, String pointCode, Date dataTime, String polluteCode,
			String overValue) {

		try {

			String alarmCode = rule.getAlarmCode();

			// 根据站点ID，告警码和因子ID，取md5 作为规则的ID
			String alarmId = AlarmIDUtil.generatingAlarmID(pointCode, alarmCode, polluteCode, DataType.T2011);

			// 根据规则id 获取内存告警数据
			AlarmInfoVO alarm = AasService.alarmMap.get(alarmId);

			if (result) {
				// 符合规则，内存中不存在告警数据
				if (alarm == null) {

					alarm = new AlarmInfoVO();
					alarm.setAlarmCode(alarmCode);
					alarm.setStartTime(dataTime);
					alarm.setPointCode(pointCode);
					alarm.setPolluteCode(polluteCode);
					if (!StringUtil.isEmptyOrNull(overValue)) {
						alarm.setAlarmValue(overValue);
					}
					alarm.setAlarmType(rule.getAlarmType());
					alarm.setDataType(DataType.T2011);
					alarm.setStorage(false);
					AasService.alarmMap.put(alarmId, alarm);

					String info = String.format("站点:%s 3020告警[%s]---%s", pointCode, polluteCode,
							DateUtil.dateToStr(dataTime, "yyyy-MM-dd HH:mm:ss"));
					logger.info(info);
					publish.send(Constant.MQ_QUEUE_LOGS, LoggerUtil.getLogInfo(LogType.LOG_TYPE_EXCEPTION, info));

				}

				Date alarmDate = alarm.getStartTime();
				long durTime = DateUtil.dateDiffMin(alarmDate, dataTime);

				// 告警是否持久化,没有进行持久化处理
				if (alarm.isStorage()) {
					alarm.setEndTime(dataTime);
					return;
				}

				// 告警持续时间 大于 规则设置时间，进行持久化处理
				if (rule.getDurTime() <= durTime) {
					alarm.setStorage(true);
					String json = JSON.toJSONStringWithDateFormat(alarm, "yyyy-MM-dd HH:mm:ss",
							SerializerFeature.WriteDateUseDateFormat);

					if (!StringUtil.isEmptyOrNull(alarmCode)) {
						publish.send(Constant.MQ_QUEUE_ALARM, json);
					}

					publish.send(Constant.MQ_QUEUE_LOGS, LoggerUtil.getLogInfo(LogType.LOG_TYPE_ALARM, alarm));

					String info = String.format("站点:%s 3020告警入库[%s]---%s", pointCode, polluteCode,
							DateUtil.dateToStr(dataTime, "yyyy-MM-dd HH:mm:ss"));
					logger.info(info);
					publish.send(Constant.MQ_QUEUE_LOGS, LoggerUtil.getLogInfo(LogType.LOG_TYPE_EXCEPTION, info));
				}

			} else {
				// 分析结果不符合规则，且内存存在告警数据
				if (alarm != null) {

					// 告警已经持久化，进行消警处理
					if (alarm.isStorage()) {

						alarm.setNewAlarm(false);

						if (alarm.getEndTime() == null) {
							alarm.setEndTime(dataTime);
						}

						String json = JSON.toJSONStringWithDateFormat(alarm, "yyyy-MM-dd HH:mm:ss",
								SerializerFeature.WriteDateUseDateFormat);

						if (!StringUtil.isEmptyOrNull(alarmCode)) {
							publish.send(Constant.MQ_QUEUE_ALARM, json);
						}

						publish.send(Constant.MQ_QUEUE_LOGS, LoggerUtil.getLogInfo(LogType.LOG_TYPE_ALARM, alarm));
					}

					String info = String.format("站点:%s 3020消除[%s]---%s", pointCode, polluteCode,
							DateUtil.dateToStr(dataTime, "yyyy-MM-dd HH:mm:ss"));
					logger.info(info);
					publish.send(Constant.MQ_QUEUE_LOGS, LoggerUtil.getLogInfo(LogType.LOG_TYPE_EXCEPTION, info));

					// 删除告警内存数据
					AasService.alarmMap.remove(alarmId);
				}
			}
		} catch (Exception e) {
			logger.error("3020告警异常:" + e.toString());
			publish.send(Constant.MQ_QUEUE_LOGS, LoggerUtil.getLogInfo(LogType.LOG_TYPE_EXCEPTION, e.toString()));
		}
	}

}
