/** 
* Copyright (C) 2012-2017 ZEEI Inc.All Rights Reserved.
* 项目名称：aps
* 文件名称：GkAlarmHandler.java
* 包  名  称：com.zeei.das.aps.alarm
* 文件描述：TODO 请修改文件描述
* 创建日期：2017年7月31日上午11:34:01
* 
* 修改历史
* 1.0 quanhongsheng 2017年7月31日上午11:34:01 创建文件
*
*/

package com.zeei.das.aas.alarm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSON;
import com.zeei.das.aas.service.AlarmRuleService;
import com.zeei.das.aas.vo.ExcludeTimeVO;
import com.zeei.das.common.utils.DateUtil;

/**
 * 类 名 称：GkAlarmHandler 类 描 述：TODO 请修改文件描述 功能描述：TODO 请修改功能描述 创建作者：quanhongsheng
 */
@Component
public class ExcludeTimeHandler {

	@Autowired
	AlarmRuleService service;

	public static Map<Integer, List<Date[]>> exceptionTimesMap = new ConcurrentHashMap<Integer, List<Date[]>>();
	public static Map<String, List<String[]>> regularStopTimesMap = new ConcurrentHashMap<String, List<String[]>>();

	private static Logger logger = LoggerFactory.getLogger(ExcludeTimeHandler.class);

	@PostConstruct
	public void init() {

		Integer cacheTime = 1000 * 60 * 5;

		Timer timer = new Timer();
		// (TimerTask task, long delay, long period)任务，延迟时间，多久执行
		timer.schedule(new TimerTask() {
			@Override
			public void run() {

				try {
					queryExcludeTime();
				} catch (Exception e) {
					logger.error(e.toString());
				}
			}

		}, 1000, cacheTime);

	}

	public boolean excludeTime(String pointCode, Date dataTime) {

		boolean isExclude = true;

		try {
			List<Date[]> excludeTimes = analysisTime(pointCode, dataTime);

			if (excludeTimes != null && excludeTimes.size() > 0) {

				for (Date[] times : excludeTimes) {

					if (dataTime.getTime() >= times[0].getTime() && dataTime.getTime() < times[1].getTime()) {
						isExclude = false;
						break;
					}
				}
			}

			/*
			 * String info = String.format("异常时段判定(%s)：%s[%s]-%s", !isExclude,
			 * pointCode, DateUtil.dateToStr(dataTime, "yyyy-MM-dd HH:mm:ss"),
			 * JSON.toJSONStringWithDateFormat(excludeTimes,
			 * "yyyy-MM-dd HH:mm:ss"));
			 */

			// logger.info(info);

		} catch (Exception e) {
			logger.error("异常时段判定服务异常:"+e.toString());
		}

		return !isExclude;
	}

	/**
	 * 
	 * queryExcludeTime:查询异常申报时间和规律性停产时间
	 *
	 * @return void
	 */

	public void queryExcludeTime() {

		exceptionTimesMap.clear();

		regularStopTimesMap.clear();

		exceptionTimesMap.putAll(queryExceptionTime());
		regularStopTimesMap.putAll(queryRegularStopTime());
	}

	/**
	 * 
	 * queryExcludeTime:TODO 请修改方法功能描述 void
	 */
	public Map<Integer, List<Date[]>> queryExceptionTime() {

		Map<Integer, List<Date[]>> timemap = new ConcurrentHashMap<Integer, List<Date[]>>();
		try {
			List<ExcludeTimeVO> exceptionTimes = service.queryExceptionTime();

			if (exceptionTimes != null) {
				Map<Integer, List<ExcludeTimeVO>> map = exceptionTimes.stream()
						.collect(Collectors.groupingBy(ExcludeTimeVO::getPointCode, Collectors.toList()));

				if (map != null) {

					for (Map.Entry<Integer, List<ExcludeTimeVO>> entry : map.entrySet()) {

						List<ExcludeTimeVO> list = entry.getValue();

						if (list != null && list.size() > 0) {

							List<Date[]> times = new ArrayList<Date[]>();

							for (ExcludeTimeVO vo : list) {

								times.add(new Date[] { vo.getbDateTime(), vo.geteDateTime() });
							}
							timemap.put(entry.getKey(), times);
						}
					}
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		String info = String.format("初始化异常时段：%s", JSON.toJSONStringWithDateFormat(timemap, "yyyy-MM-dd HH:mm:ss"));
		logger.info(info);

		return timemap;
	}

	/**
	 * 
	 * queryRegularStopTime:TODO 请修改方法功能描述 void
	 */
	public Map<String, List<String[]>> queryRegularStopTime() {

		Map<String, List<String[]>> timemap = new ConcurrentHashMap<String, List<String[]>>();
		try {
			List<ExcludeTimeVO> regularStopTimes = service.queryRegularStopTime();
			// 按星期拆分时间
			if (regularStopTimes != null) {

				// 按站点和week分组
				Map<Integer, Map<Integer, List<ExcludeTimeVO>>> map = regularStopTimes.stream()
						.collect(Collectors.groupingBy(ExcludeTimeVO::getPointCode,
								Collectors.groupingBy(ExcludeTimeVO::getWeek, Collectors.toList())));

				for (Entry<Integer, Map<Integer, List<ExcludeTimeVO>>> entry : map.entrySet()) {

					Map<Integer, List<ExcludeTimeVO>> map1 = entry.getValue();

					for (Entry<Integer, List<ExcludeTimeVO>> entry1 : map1.entrySet()) {

						List<ExcludeTimeVO> list = entry1.getValue();

						if (list != null && list.size() > 0) {

							List<String[]> times = new ArrayList<String[]>();

							for (ExcludeTimeVO vo : list) {

								times.add(new String[] { vo.getStartTime(), vo.getEndTime() });
							}

							String key = String.format("%s-%s", entry.getKey(), entry1.getKey());
							timemap.put(key, times);
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		String info = String.format("初始化停产时段：%s", JSON.toJSONStringWithDateFormat(timemap, "yyyy-MM-dd HH:mm:ss"));
		logger.info(info);

		return timemap;
	}

	/**
	 * 
	 * analysisTime:TODO 请修改方法功能描述
	 *
	 * @param alarm
	 * @return List<Date[]>
	 */
	private List<Date[]> analysisTime(String pointCode, Date dataTime) {

		List<Date[]> excludeTimes = new ArrayList<Date[]>();

		if (exceptionTimesMap != null) {
			List<Date[]> times = exceptionTimesMap.get(Integer.valueOf(pointCode));
			if (times != null && times.size() > 0) {
				excludeTimes.addAll(times);
			}
		}

		if (regularStopTimesMap != null) {

			dataTime = DateUtil.strToDate(DateUtil.dateToStr(dataTime, "yyyy-MM-dd"), "yyyy-MM-dd");
			int week = DateUtil.getWeekOfDate(dataTime);
			String date = DateUtil.dateToStr(dataTime, "yyyy-MM-dd");
			String key = String.format("%s-%s", pointCode, week);
			List<String[]> times = regularStopTimesMap.get(key);
			if (times != null && times.size() > 0) {
				for (String[] s : times) {
					Date beginTime = DateUtil.strToDate(String.format("%s %s", date, s[0]), "yyyy-MM-dd HH:mm:ss");
					Date endTime = DateUtil.strToDate(String.format("%s %s", date, s[1]), "yyyy-MM-dd HH:mm:ss");
					excludeTimes.add(new Date[] { beginTime, endTime });
				}
			}
		}
		return excludeTimes;

	}

	/**
	 * 
	 * mergeTime:时间去重合并
	 *
	 * @param excludeTimes
	 *            void
	 */
	public List<Date[]> mergeTime(List<Date[]> excludeTimes) {

		Date beginTime = null;
		Date endTime = null;

		List<Date[]> times = new ArrayList<Date[]>();

		// 时间排序
		Collections.sort(excludeTimes, new Comparator<Date[]>() {

			public int compare(Date[] o1, Date[] o2) {

				if (o1[0].compareTo(o2[0]) == 0) {
					return o1[1].compareTo(o2[1]);
				} else {
					return o1[0].compareTo(o2[0]);
				}
			}
		});

		for (int i = 0; i < excludeTimes.size(); i++) {

			Date[] excludeTime = excludeTimes.get(i);

			Date cBTime = excludeTime[0];
			Date cETime = excludeTime[1];

			if (i == 0) {
				beginTime = cBTime;
				endTime = cETime;
			} else {

				if (cBTime.getTime() > endTime.getTime()) {

					long mins = DateUtil.dateDiffMin(endTime, cBTime);
					if (mins > 1) {
						times.add(new Date[] { beginTime, endTime });
						beginTime = cBTime;
						endTime = cETime;
					} else {
						endTime = cETime;
					}

				} else if (cETime.getTime() > endTime.getTime() && cBTime.getTime() <= endTime.getTime()) {
					endTime = cETime;
				}

			}

			if (i == excludeTimes.size() - 1) {
				times.add(new Date[] { beginTime, endTime });
			}
		}

		return times;

	}

}
