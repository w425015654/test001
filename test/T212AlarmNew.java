/** 
* Copyright (C) 2012-2017 ZEEI Inc.All Rights Reserved.
* 项目名称：aas
* 文件名称：T212Alarm.java
* 包  名  称：com.zeei.das.aas.alarm
* 文件描述：T212 监测数据告警采集
* 创建日期：2017年4月21日上午8:18:19
* 
* 修改历史
* 1.0 quanhongsheng 2017年4月21日上午8:18:19 创建文件
*
*/

package com.zeei.das.aas.alarm;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.zeei.das.aas.AasService;
import com.zeei.das.aas.alarm.engine.AbnormalEngine;
import com.zeei.das.aas.alarm.engine.ExpressionEngine;
import com.zeei.das.aas.alarm.engine.FluctuationEngine;
import com.zeei.das.aas.alarm.engine.RuleEngine;
import com.zeei.das.aas.alarm.engine.UpsideDownEngine;
import com.zeei.das.aas.alarm.engine.UpsideDownSkEngine;
import com.zeei.das.aas.mq.Publish;
import com.zeei.das.aas.vo.AlarmInfoVO;
import com.zeei.das.aas.vo.AlarmRuleVO;
import com.zeei.das.aas.vo.MonitorDataVO;
import com.zeei.das.aas.vo.StationVO;
import com.zeei.das.aas.service.AlarmProcessService;
import com.zeei.das.aas.vo.RuleVO;
import com.zeei.das.common.constants.Constant;
import com.zeei.das.common.constants.DataType;
import com.zeei.das.common.constants.LogType;
import com.zeei.das.common.utils.DateUtil;
import com.zeei.das.common.utils.LoggerUtil;
import com.zeei.das.common.utils.StringUtil;
import com.zeei.das.aas.service.QueryDataService;
import com.zeei.das.aas.utils.AssembleParamUtil;
import com.zeei.das.aas.utils.PartitionTableUtil;

/**
 * 类 名 称：T212Alarm 类 描 述：T212 监测数据告警采集 功能描述：T212 监测数据告警采集 创建作者：quanhongsheng
 */

@Component("t212AlarmNew")
public class T212AlarmNew extends Alarm {

	private static Logger logger = LoggerFactory.getLogger(Alarm.class);

	@Autowired
	Publish publish;

	@Autowired
	UpsideDownEngine upsideDownEngine;
	@Autowired
	UpsideDownSkEngine upsideDownSkEngine;
	@Autowired
	AbnormalEngine abnormalEngine;
	@Autowired
	FluctuationEngine fluctuationEngine;
	@Autowired
	ExpressionEngine expressionEngine;
	@Autowired
	ExcludeTimeHandler excludeTimeHandler;
	@Autowired
	QueryDataService queryDataService;
	@Autowired
	AlarmProcessService alarmProcessService;

	@Override
	public void alarmHandler(JSONObject data) {

		try {
			// 发送服务心跳数据
			// reportUtil.report();

			String MN = data.getString("MN");
			String CN = data.getString("CN");

			JSONObject CP = data.getJSONObject("CP");

			if (CP == null || CP.size() < 1) {
				String err = String.format("站点【%s】数据为空！", MN);
				publish.send(Constant.MQ_QUEUE_LOGS, LoggerUtil.getLogInfo(LogType.LOG_TYPE_EXCEPTION, err));
				return;
			}

			JSONArray params = CP.getJSONArray("Params");

			if (params == null || params.size() < 1) {
				String err = String.format("站点【%s】数据为空！", MN);
				publish.send(Constant.MQ_QUEUE_LOGS, LoggerUtil.getLogInfo(LogType.LOG_TYPE_EXCEPTION, err));
				return;
			}

			RuleVO ruleVO = AasService.ruleMap.get(MN);

			if (ruleVO == null) {
				return;
			}

			List<AlarmRuleVO> alramRules = new ArrayList<AlarmRuleVO>();

			switch (CN) {
			case DataType.RTDATA:
				alramRules = ruleVO.getR2011();
				break;
			case DataType.DAYDATA:
				alramRules = ruleVO.getR2031();
				break;
			case DataType.MINUTEDATA:
				alramRules = ruleVO.getR2051();
				break;
			case DataType.HOURDATA:
				alramRules = ruleVO.getR2061();
				break;
			}

			RuleEngine engine = null;

			for (AlarmRuleVO rule : alramRules) {

				if (rule == null) {
					continue;
				}

				String alarmCode = rule.getAlarmCode();

				Date dataTime = CP.getDate("DataTime");

				rule.setDataTime(dataTime);

				RuleEngine skengine = null;

				// 根据告警码，构造对应的分析引擎
				if (AasService.UPSIDEDOWNCODE.equals(alarmCode)) {
					// pm2.5 pm10倒挂告警
					engine = upsideDownEngine;
					skengine = upsideDownSkEngine;
				} else if (AasService.ABNORMALCODE.equals(alarmCode)) {
					// 数据异常告警
					engine = abnormalEngine;
				} else if (AasService.FLUCTUATIONCODE.equals(alarmCode)) {
					// 数据波动告警
					engine = fluctuationEngine;
				} else {
					engine = expressionEngine;
				}

				int result = 2;

				// 引擎分析结果 1标示符合规则，0标示不符合规则，2标示异常退出无需进行后续操作
				result = engine.analysis(rule, params);

				if (result != 2) {
					generationAlarm(result == 1 ? true : false, rule);
				}
				
				// 计算实况倒挂告警
				if (skengine != null) {					
					result = 2;
					result = skengine.analysis(rule, params);
					if (result != 2) {
						generationAlarm(result == 1 ? true : false, rule);
					}					
				}

			
			}
		} catch (Exception e) {
			logger.error(e.toString());
			logger.error(data.toString());
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
	private void generationAlarm(boolean result, AlarmRuleVO rule) {

		try {

			String ruleId = rule.getRuleId();
			String pointCode = rule.getPointCode();
			Date dataTime = rule.getDataTime();
			String alarmCode = rule.getAlarmCode();

			// 根据规则id 获取内存告警数据
			AlarmInfoVO alarm = AasService.alarmMap.get(ruleId);

			if (result) {

				// 告警规则与规律性停止，异常申报相关
				if (rule.getIsGenAlarm() == 0) {

					// 判断告警时间是否在规律性停产和异常申报时段内
					boolean isExclude = excludeTimeHandler.excludeTime(pointCode, dataTime);

					// 告警时间在规律性停产和异常申报时段内，不产生告警
					if (isExclude) {

						if (rule.isOut() == false) {
							String info = String.format("[规则失效留档]站点:%s 公式告警[%s|%s](%s)---%s", pointCode,
									rule.getAlarmCode(), rule.getPolluteCode(), rule.getFormula(),
									DateUtil.dateToStr(dataTime, "yyyy-MM-dd HH:mm:ss"));
							logger.info(info);
							rule.setOut(true);
						}

						// 分析结果不符合规则，且内存存在告警数据
						if (alarm != null && alarm.getStartTime().getTime() < dataTime.getTime()) {

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

								publish.send(Constant.MQ_QUEUE_LOGS,
										LoggerUtil.getLogInfo(LogType.LOG_TYPE_ALARM, alarm));
							}

							String info = String.format("站点:%s 公式告警消除[%s|%s](%s)---%s", pointCode, rule.getAlarmCode(),
									rule.getPolluteCode(), rule.getFormula(),
									DateUtil.dateToStr(dataTime, "yyyy-MM-dd HH:mm:ss"));
							logger.info(info);
							publish.send(Constant.MQ_QUEUE_LOGS,
									LoggerUtil.getLogInfo(LogType.LOG_TYPE_EXCEPTION, info));

							// 删除告警内存数据
							AasService.alarmMap.remove(ruleId);
						}

						return;
					}
				}

				if (alarm == null) {

					alarm = new AlarmInfoVO();
					alarm.setAlarmCode(alarmCode);
					alarm.setAlarmValue(rule.getAlarmValue());
					alarm.setStartTime(dataTime);
					alarm.setPointCode(pointCode);
					alarm.setPolluteCode(rule.getPolluteCode());
					alarm.setDataType(rule.getDataType());
					alarm.setAlarmType(rule.getAlarmType());
					alarm.setStorage(false);
					AasService.alarmMap.put(ruleId, alarm);
					String info = String.format("站点:%s 公式告警[%s|%s](%s)---%s", pointCode, rule.getAlarmCode(),
							rule.getPolluteCode(), rule.getFormula(), DateUtil.dateToStr(dataTime, "yyyy-MM-dd HH:mm:ss"));
					logger.info(info);
					publish.send(Constant.MQ_QUEUE_LOGS, LoggerUtil.getLogInfo(LogType.LOG_TYPE_EXCEPTION, info));
					rule.setOut(false);

				} else if (Double.parseDouble(alarm.getAlarmValue()) < Double.parseDouble(rule.getAlarmValue())) {
					alarm.setAlarmValue(rule.getAlarmValue());
				}

				Date alarmDate = alarm.getStartTime();
				long durTime = DateUtil.dateDiffMin(alarmDate, dataTime);

				// 设置结束时间
				if (alarm.isStorage()) {
					alarm.setEndTime(dataTime);
					return;
				}

				// 新增字段
				if (rule.getDurTime() <= durTime) {
					alarm.setIsDurtime(1);
				} else {
					alarm.setIsDurtime(0);
				}
				alarm.setStorage(true);

				// 处理补数数据
				procAlarmData(alarm, rule);

			} else {
				rule.setOut(false);
				// 分析结果不符合规则，且内存存在告警数据
				if (alarm != null && alarm.getStartTime().getTime() < dataTime.getTime()) {

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

					String info = String.format("站点:%s 公式告警消除[%s|%s](%s)---%s", pointCode, rule.getAlarmCode(),
							rule.getPolluteCode(), rule.getFormula(), DateUtil.dateToStr(dataTime, "yyyy-MM-dd HH:mm:ss"));
					logger.info(info);
					publish.send(Constant.MQ_QUEUE_LOGS, LoggerUtil.getLogInfo(LogType.LOG_TYPE_EXCEPTION, info));

					// 删除告警内存数据
					AasService.alarmMap.remove(ruleId);

					// 处理补数数据
					procMonitorData(alarm, rule);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.toString());
			publish.send(Constant.MQ_QUEUE_LOGS, LoggerUtil.getLogInfo(LogType.LOG_TYPE_EXCEPTION, e.toString()));
		}
	}

	private void procAlarmData(AlarmInfoVO alarm, AlarmRuleVO rule) {
		if ((alarm == null) || (rule == null)) {
			return;
		}

		StationVO station = AasService.stationMap.get(String.valueOf(alarm.getPointCode()));

		if (station == null) {
			return;
		}
		String ST = station.getST();
		String MN = station.getMN();

		String tableName = PartitionTableUtil.getTableName(ST, DataType.RTDATA, station.getPointCode(),
				alarm.getStartTime());

		// 查有没有该告警
		AlarmInfoVO existed = alarmProcessService.queryAlarmInfoByBsCondition(alarm);

		// 存在该告警
		if (existed != null) {
			// 补数告警包含在内
			if (alarm.getStartTime().compareTo(existed.getStartTime()) >= 0
					&& alarm.getEndTime().compareTo(existed.getEndTime()) <= 0) {
			} else { // 补数数据是告警数据，系统不存在告警
				// 查监测数据
				List<MonitorDataVO> beforeData = queryDataService.queryBeforeDataByCondition(tableName,
						alarm.getPointCode(), alarm.getStartTime(), alarm.getEndTime(), alarm.getPolluteCode(), ST);

				List<MonitorDataVO> nextData = queryDataService.queryNextDataByCondition(tableName,
						alarm.getPointCode(), alarm.getStartTime(), alarm.getEndTime(), alarm.getPolluteCode(), ST);

				int bIsAlarm = 2;
				int aIsAlarm = 2;

				if ((beforeData != null) && (beforeData.size() > 0)) {
					JSONArray params = AssembleParamUtil.data2Params(ST, DataType.RTDATA, MN, beforeData.get(0));
					bIsAlarm = expressionEngine.analysis(rule, params);
				}

				if ((nextData != null) && (nextData.size() > 0)) {
					JSONArray params = AssembleParamUtil.data2Params(ST, DataType.RTDATA, MN, nextData.get(0));
					aIsAlarm = expressionEngine.analysis(rule, params);
				}

				if (bIsAlarm == 0) {
					if (aIsAlarm == 0) {
						// 新告警
						sendAlarmMsg(alarm, rule);
					} else if (aIsAlarm == 1) {
						// 存在的告警更新
						if (existed != null) {
							existed.setEndTime(rule.getDataTime());
							alarmProcessService.updateAlarmInfo(existed);
						}
					}
				}
			}
		}
	}

	private void procMonitorData(AlarmInfoVO alarm, AlarmRuleVO rule) {
		if ((alarm == null) || (rule == null)) {
			return;
		}

		StationVO station = AasService.stationMap.get(String.valueOf(alarm.getPointCode()));
		String ST = station.getST();
		String MN = station.getMN();

		String tableName = PartitionTableUtil.getTableName(ST, DataType.RTDATA, station.getPointCode(),
				alarm.getStartTime());

		// 查监测数据
		List<MonitorDataVO> beforeData = queryDataService.queryBeforeDataByCondition(tableName, alarm.getPointCode(),
				alarm.getStartTime(), alarm.getEndTime(), alarm.getPolluteCode(), ST);

		List<MonitorDataVO> nextData = queryDataService.queryNextDataByCondition(tableName, alarm.getPointCode(),
				alarm.getStartTime(), alarm.getEndTime(), alarm.getPolluteCode(), ST);

		int bIsAlarm = 2;
		int aIsAlarm = 2;

		if ((beforeData != null) && (beforeData.size() > 0)) {
			// 前监测数据
			JSONArray params = AssembleParamUtil.data2Params(ST, DataType.RTDATA, MN, beforeData.get(0));
			bIsAlarm = expressionEngine.analysis(rule, params);
		}

		if ((nextData != null) && (nextData.size() > 0)) {
			// 后监测数据
			JSONArray params = AssembleParamUtil.data2Params(ST, DataType.RTDATA, MN, nextData.get(0));
			aIsAlarm = expressionEngine.analysis(rule, params);
		}

		if ((bIsAlarm == 1) && (aIsAlarm == 1)) { // 拆分
			AlarmInfoVO existed = null;

			// 查原数据库中告警（构造查询条件）
			AlarmInfoVO queryCondition = getQueryCondition(rule);
			if (queryCondition != null) {
				// 查有没有该告警
				existed = alarmProcessService.queryAlarmInfoByBsCondition(alarm);
			}

			// 更新
			if (existed != null) {
				existed.setEndTime(rule.getDataTime());
				alarmProcessService.updateAlarmInfo(existed);

				// 产生一条新告警
				AlarmInfoVO newAlarm = getNewAlarm(existed, rule);
				if (newAlarm != null) {
					sendLogMsg(newAlarm, rule);

					Date alarmDate = newAlarm.getStartTime();
					Date dataTime = rule.getDataTime();
					long durTime = DateUtil.dateDiffMin(alarmDate, dataTime);

					// 设置结束时间
					if (newAlarm.isStorage()) {
						newAlarm.setEndTime(dataTime);
						return;
					}

					// 新增字段
					if (rule.getDurTime() <= durTime) {
						newAlarm.setIsDurtime(1);
					} else {
						newAlarm.setIsDurtime(0);
					}
					newAlarm.setStorage(true);

					sendAlarmMsg(newAlarm, rule);
				}
			}
		}
	}

	private AlarmInfoVO getQueryCondition(AlarmRuleVO rule) {
		AlarmInfoVO alarm = null;

		if (rule != null) {
			// 查原数据库中告警（构造查询条件）
			alarm = new AlarmInfoVO();
			alarm.setAlarmCode(rule.getAlarmCode());
			alarm.setAlarmValue(rule.getAlarmValue());
			alarm.setStartTime(rule.getDataTime());
			alarm.setEndTime(rule.getDataTime());
			alarm.setPointCode(rule.getPointCode());
			alarm.setPolluteCode(rule.getPolluteCode());
			alarm.setDataType(rule.getDataType());
			alarm.setAlarmType(rule.getAlarmType());
		}

		return alarm;
	}

	private AlarmInfoVO getNewAlarm(AlarmInfoVO existed, AlarmRuleVO rule) {
		AlarmInfoVO newAlarm = null;

		if (rule != null) {
			newAlarm = new AlarmInfoVO();
			newAlarm.setAlarmCode(existed.getAlarmCode());
			newAlarm.setAlarmValue(existed.getAlarmValue());
			newAlarm.setStartTime(rule.getDataTime());
			newAlarm.setEndTime(existed.getEndTime());
			newAlarm.setPointCode(existed.getPointCode());
			newAlarm.setPolluteCode(existed.getPolluteCode());
			newAlarm.setDataType(existed.getDataType());
			newAlarm.setAlarmType(existed.getAlarmType());
			newAlarm.setStorage(false);
		}

		return newAlarm;
	}

	private void sendLogMsg(AlarmInfoVO alarm, AlarmRuleVO rule) {
		AasService.alarmMap.put(rule.getRuleId(), alarm);
		String info = String.format("站点:%s 公式告警[%s|%s](%s)---%s", alarm.getPointCode(), alarm.getAlarmCode(),
				alarm.getPolluteCode(), rule.getFormula(), DateUtil.dateToStr(rule.getDataTime(), "yyyy-MM-dd HH:mm:ss"));
		logger.info(info);
		publish.send(Constant.MQ_QUEUE_LOGS, LoggerUtil.getLogInfo(LogType.LOG_TYPE_EXCEPTION, info));
	}

	private void sendAlarmMsg(AlarmInfoVO alarm, AlarmRuleVO rule) {
		String json = JSON.toJSONStringWithDateFormat(alarm, "yyyy-MM-dd HH:mm:ss",
				SerializerFeature.WriteDateUseDateFormat);

		if (!StringUtil.isEmptyOrNull(rule.getAlarmCode())) {
			publish.send(Constant.MQ_QUEUE_ALARM, json);
		}

		publish.send(Constant.MQ_QUEUE_LOGS, LoggerUtil.getLogInfo(LogType.LOG_TYPE_ALARM, alarm));

		String info = String.format("站点:%s 公式告警入库[%s|%s](%s)---%s", rule.getPointCode(), rule.getAlarmCode(),
				rule.getPolluteCode(), rule.getFormula(), DateUtil.dateToStr(rule.getDataTime(), "yyyy-MM-dd HH:mm:ss"));
		logger.info(info);
		publish.send(Constant.MQ_QUEUE_LOGS, LoggerUtil.getLogInfo(LogType.LOG_TYPE_EXCEPTION, info));
	}
}
