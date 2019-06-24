/** 
* Copyright (C) 2012-2017 ZEEI Inc.All Rights Reserved.
* 项目名称：aas
* 文件名称：ServiceAlarm.java
* 包  名  称：com.zeei.das.aas.alarm
* 文件描述：服务运行状态告警
* 创建日期：2017年4月21日上午8:20:18
* 
* 修改历史
* 1.0 quanhongsheng 2017年4月21日上午8:20:18 创建文件
*
*/

package com.zeei.das.aas.alarm;

import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSONObject;

/**
 * 类 名 称：ServiceAlarm 类 描 述：服务运行状态告警 功能描述：服务运行状态告警 创建作者：quanhongsheng
 */

@Component("serviceAlarm")
public class ServiceAlarm extends Alarm {

	@Override
	public void alarmHandler(JSONObject data) {

	}

}
