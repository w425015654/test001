/** 
* Copyright (C) 2012-2017 ZEEI Inc.All Rights Reserved.
* 项目名称：aas
* 文件名称：Alarm.java
* 包  名  称：com.zeei.das.aas.alarm
* 文件描述：告警采集抽象类
* 创建日期：2017年4月20日下午2:38:48
* 
* 修改历史
* 1.0 quanhongsheng 2017年4月20日下午2:38:48 创建文件
*
*/

package com.zeei.das.aas.alarm;

import com.alibaba.fastjson.JSONObject;

/**
 * 类 名 称：Alarm 类 描 述：告警采集抽象类 功能描述：告警采集抽象类 创建作者：quanhongsheng
 */

public abstract class Alarm {

	public abstract void alarmHandler(JSONObject data);

}
