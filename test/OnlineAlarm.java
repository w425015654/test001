/** 
* Copyright (C) 2012-2017 ZEEI Inc.All Rights Reserved.
* 项目名称：aas
* 文件名称：OnlineAlarm.java
* 包  名  称：com.zeei.das.aas.alarm
* 文件描述：站点离线告警采集
* 创建日期：2017年4月21日上午8:19:06
* 
* 修改历史
* 1.0 quanhongsheng 2017年4月21日上午8:19:06 创建文件
*
*/

package com.zeei.das.aas.alarm;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.zeei.das.aas.AasService;
import com.zeei.das.aas.mq.Publish;
import com.zeei.das.aas.service.UpdatePointStatusService;
import com.zeei.das.aas.vo.AlarmInfoVO;
import com.zeei.das.aas.vo.StationVO;
import com.zeei.das.common.constants.Constant;
import com.zeei.das.common.constants.DataType;
import com.zeei.das.common.constants.LogType;
import com.zeei.das.common.utils.DateUtil;
import com.zeei.das.common.utils.LoggerUtil;
import com.zeei.das.common.utils.StringUtil;

/**
 * 类 名 称：OnlineAlarm 类 描 述：站点离线告警采集 功能描述：站点离线告警采集 创建作者：quanhongsheng
 */

@Component
public class OnlineAlarm extends Alarm {
	private static Logger logger = LoggerFactory.getLogger(OnlineAlarm.class);

	@Autowired
	Publish publish;

	@Autowired
	UpdatePointStatusService updatePointStatusService;

	@Autowired
	ExcludeTimeHandler excludeTimeHandler;

	@Override
	public void alarmHandler(JSONObject data) {

		try {
			Runnable runnable = new Runnable() {
				public void run() {

					Map<String, Set<String>> stationStatus = new HashMap<String, Set<String>>();

					// 判断站点离线告警
					for (StationVO station : AasService.stationMap.values()) {
						try {

							// 判断告警时间是否在规律性停产和异常申报时段内
							boolean isExclude = false;
							Date currentDate = DateUtil.getCurrentDate();
							if (AasService.isTC == 1) {
								isExclude = excludeTimeHandler.excludeTime(station.getPointCode(), currentDate);
								if (isExclude) {
									station.setHeartTime(currentDate);
								}
							}
							// 周期
							double cycle = station.getrCycle();

							cycle = Math.ceil(cycle / 60);

							double diff = AasService.onlineTime;

							if (AasService.onlineTime > cycle) {
								diff = AasService.onlineTime + cycle;
							} else {
								diff = cycle;
							}
							// 计算最新心跳时间与当前时间的间隔
							Date heartTime = station.getHeartTime();
							if (heartTime == null) {
								// logger.error("获取站点心跳时间为空;站点编码pointCode=" +
								// station.getPointCode());
								heartTime = DateUtil.dateAddMin(currentDate, (int) (-diff - 1));
								station.setHeartTime(heartTime);
							}
							long diffMin = DateUtil.dateDiffMin(heartTime, currentDate);
							// 根据站点ID，告警码和因子ID，取md5 作为规则的ID
							String alarmId = AlarmIDUtil.generatingAlarmID(station.getPointCode(),
									AasService.onlineCode, null, null);

							// logger.info(String.format("在线时间:%s %s %s %s",
							// station.getPointCode(), heartTime,
							// diffMin,diff));

							int status = 1;
							// 站点离线 逻辑
							// 根据站点是否上班数据为依据，时间规则，设定基本时间，如果实时数据周期大于设定的基准时间则
							// 离线时间未基准时间+周期时间，否则离线时间等于基准时间
							// 符合告警条件
							// 告警时间在规律性停产和异常申报时段内，不产生告警
							if (diffMin > diff && !AasService.alarmMap.containsKey(alarmId)) {

								// logger.error("告警KEY:" +
								// JSON.toJSONString(AasService.alarmMap.keySet()));
								// 生成告警入库
								AlarmInfoVO alarm = new AlarmInfoVO();
								alarm.setAlarmCode(AasService.onlineCode);
								alarm.setStartTime(DateUtil.dateAddSecond(heartTime, station.getrCycle()));
								alarm.setPointCode(station.getPointCode());
								alarm.setStorage(true);
								alarm.setDataType(DataType.T2011);
								alarm.setAlarmType(AasService.onlineType);
								AasService.alarmMap.put(alarmId, alarm);
								String json = JSON.toJSONStringWithDateFormat(alarm, "yyyy-MM-dd HH:mm:ss",
										SerializerFeature.WriteDateUseDateFormat);
								publish.send(Constant.MQ_QUEUE_ALARM, json);
								publish.send(Constant.MQ_QUEUE_LOGS,
										LoggerUtil.getLogInfo(LogType.LOG_TYPE_ALARM, alarm));

								String info = String.format("站点:%s 离线告警【产生】---%s ~ %s  时间间隔：%s 分钟    离线标准：%s 分钟",
										alarm.getPointCode(), DateUtil.dateToStr(heartTime, "yyyy-MM-dd HH:mm:ss"),
										DateUtil.dateToStr(currentDate, "yyyy-MM-dd HH:mm:ss"), diffMin, diff);
								logger.info(info);
								publish.send(Constant.MQ_QUEUE_LOGS,
										LoggerUtil.getLogInfo(LogType.LOG_TYPE_EXCEPTION, info));

							}

							if (diffMin <= diff && AasService.alarmMap.containsKey(alarmId)) {
								// 不符合告警条件，且告警信息入库，生成消警信息入库
								// 查询内存告警信息
								AlarmInfoVO alarm = AasService.alarmMap.get(alarmId);

								Date endTime = heartTime;
								if (diffMin > 2 * diff) {
									endTime = currentDate;
								}

								alarm.setEndTime(endTime);
								alarm.setNewAlarm(false);

								String json = JSON.toJSONStringWithDateFormat(alarm, "yyyy-MM-dd HH:mm:ss",
										SerializerFeature.WriteDateUseDateFormat);
								publish.send(Constant.MQ_QUEUE_ALARM, json);

								publish.send(Constant.MQ_QUEUE_LOGS,
										LoggerUtil.getLogInfo(LogType.LOG_TYPE_ALARM, alarm));

								String info = String.format("站点:%s 离线告警【消除】(%s)---%s ~ %s 时间间隔:%s  ",
										alarm.getPointCode(), isExclude ? "申报" : "数据",
										DateUtil.dateToStr(alarm.getStartTime(), "yyyy-MM-dd HH:mm:ss"),
										DateUtil.dateToStr(heartTime, "yyyy-MM-dd HH:mm:ss"), diffMin);

								logger.info(info);
								publish.send(Constant.MQ_QUEUE_LOGS,
										LoggerUtil.getLogInfo(LogType.LOG_TYPE_EXCEPTION, info));

								// 删除内存数据
								AasService.alarmMap.remove(alarmId);

							}

							if (diffMin > diff) {

								status = 0;
							} else {
								status = 1;
							}

							String ST = station.getST();

							String key = String.format("%s_%s", ST, status);

							if (!StringUtil.isEmptyOrNull(ST)) {
								Set<String> status_list = stationStatus.get(key);
								if (status_list == null) {
									status_list = new HashSet<String>();
									stationStatus.put(key, status_list);
								}

								status_list.add(station.getPointCode());
							}
						} catch (Exception e) {
							logger.error("", e);
							publish.send(Constant.MQ_QUEUE_LOGS,
									LoggerUtil.getLogInfo(LogType.LOG_TYPE_EXCEPTION, e.toString()));
						}
					}
					// 更新站点状态
					updateStatus(stationStatus);

				}
			};
			ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
			// 第二个参数为首次执行的延时时间，第三个参数为定时执行的间隔时间

			logger.info(String.format("执行站点在线检测周期：1 分钟"));
			service.scheduleAtFixedRate(runnable, 1, 1, TimeUnit.MINUTES);
		} catch (Exception e) {
			logger.error("站点在线离线判定服务异常:" + e.toString());
			publish.send(Constant.MQ_QUEUE_LOGS, LoggerUtil.getLogInfo(LogType.LOG_TYPE_EXCEPTION, e.toString()));
		}

	}

	/**
	 * 
	 * updateStatus:更新站点状态
	 *
	 * @param stationStatus
	 *            void
	 */
	private void updateStatus(Map<String, Set<String>> stationStatus) {

		logger.info(String.format("站点状态:%s", JSON.toJSONString(stationStatus)));
		for (Map.Entry<String, Set<String>> entry : stationStatus.entrySet()) {
			try {
				String key = entry.getKey();

				String ST = key.split("_")[0];
				String status = key.split("_")[1];

				List<String> data = new ArrayList<>(entry.getValue());

				String tableName = AasService.STMap.get(ST);

				if (data != null && data.size() > 0 && !StringUtil.isEmptyOrNull(tableName)) {
					int ret = updatePointStatusService.updateStatusByBatch2(tableName, status, data);

					logger.info(String.format("更新站点状态:[%s]%s,影响点位[%s]个:%s", tableName, status, ret,
							JSON.toJSONString(data)));
				}
			} catch (Exception e) {
				logger.error("", e);
			}

		}
	}

}
