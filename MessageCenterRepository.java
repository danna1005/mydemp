package com.yjh.core.repository.messageCenter;


import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.util.StringUtils;

import com.yjh.core.common.CommonStaticConstant;
import com.yjh.core.common.HttpExceptionStatusConstant;
import com.yjh.core.exception.BizException;
import com.yjh.core.mapper.user.UserMapper;
import com.yjh.core.mapper3.message.MessageRecordMapper;
import com.yjh.core.model.messageCenter.MessageInfo;
import com.yjh.core.model.messageCenter.MessageInfoActivity;
import com.yjh.core.model.messageCenter.MessageInfoRecord;
import com.yjh.core.model.user.User;
import com.yjh.core.redis.message.MessageCenterRedisManager;

/**
 * 
 * 类名称：MessageCenterRepository   
 * 类描述：消息中心消息service处理层   
 * 创建人：zhangzhiming
 * 创建时间：2016年12月19日 上午9:39:28   
 * 修改人：zhangzhiming
 * 修改时间：2016年12月19日 上午9:39:28   
 * 修改备注：   
 * @version
 */
@Repository
public class MessageCenterRepository {
	private Logger logger = Logger.getLogger(MessageCenterRepository.class);
	@Autowired
	private MessageCenterRedisManager messageCenterRedisManager;
	@Autowired
	private MessageRecordMapper messageRecordMapper;
	@Autowired
	private UserMapper userMapper;
	
	/**
	 * 
	 * @Description: service层发送消息
	 * @param fieldValue  消息类型
	 * @param user_id 接收消息方用户ID
	 * @param messageInfoRecord   消息记录
	 * @time: 2016年12月19日 上午9:44:10
	 * @author zhangzhiming  
	 * @throws
	 */
	public void sendMessageInfo(String fieldValue, String user_id, MessageInfoRecord messageInfoRecord){
		if (StringUtils.isEmpty(user_id) || null == messageInfoRecord) {
			logger.error("sendMessageInfo发送消息失败:失败原因是user_id或者messageInfoRecord为null");
			return;
		}
		try {
			// 插入消息记录到redis缓存
			messageCenterRedisManager.setMessageRecord(messageInfoRecord, fieldValue, user_id);
			// 消息记录存储数据库,客服聊天消息和推荐活动消息记录除外
			if (messageInfoRecord.getMessage_type() != CommonStaticConstant.MESSAGE_CENTER_CHATSERVICE_TYPE
					&& messageInfoRecord
							.getMessage_type() != CommonStaticConstant.MESSAGE_CENTER_RECOMMEND_ACTIVITY_TYPE) {
				messageRecordMapper.insertMessageRecord(messageInfoRecord);
			}
		} catch (Exception e) {
			logger.error("user_id为:" + user_id + ",消息类型为:" + messageInfoRecord.getMessage_type()
					+ "执行sendMessageInfo发送消息失败,失败原因:" + e.getMessage());
		}
	}
	
	
	/**
	 * 
	 * @Description: 发布的推荐活动存储到redis中
	 * @param infoActivity 
	 * @time: 2016年12月23日 上午10:25:12
	 * @author zhangzhiming  
	 * @throws
	 */
	public void insertRecommendActivity(MessageInfoActivity infoActivity) throws Exception{
		//先清除推荐活动中30天以前的推荐活动数据
		List<MessageInfoActivity> clearActivitys =new ArrayList<MessageInfoActivity>();
		List<MessageInfoActivity> activitys = messageCenterRedisManager.getRecommendActivityList();
		boolean checkIsExist=false;
		if (CollectionUtils.isNotEmpty(activitys)) {
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(new Date());
			calendar.add(Calendar.DAY_OF_MONTH, -30);
			//找出30天的推荐活动消息
			for (MessageInfoActivity messageInfoActivity : activitys) {
				if(messageInfoActivity.getSend_time().before(calendar.getTime())){
					clearActivitys.add(messageInfoActivity);
				}
				if(messageInfoActivity.getId().equals(infoActivity.getId())){  //推荐活动是否已经插入过
					checkIsExist = true;
				}
			}
			//进行从redis中删除
			messageCenterRedisManager.delRecommendActivity(clearActivitys);
			if(checkIsExist){ //如果已经插入过,那就直接返回
				return;
			}
		}
		//再往缓存中插入推荐活动
		messageCenterRedisManager.insertRecommendActivity(infoActivity);
		
	}
	
	/**
	 * 
	 * @Description: 查询一级消息相关信息
	 * @param user
	 * @return 
	 * @time: 2016年12月21日 下午2:11:45
	 * @author zhangzhiming  
	 * @throws
	 */
	public List<MessageInfo> searchUserMessageInfos(String userId) throws Exception {
		User userSearch = new User();
		userSearch.setId(userId);
		User user = userMapper.getDetailInfo(userSearch);
		if (null == user) {
			throw new BizException(HttpExceptionStatusConstant.NOT_MODIFIED, "用户信息有误.");
		}
		// 查询用户的未读消息列表
		List<MessageInfoRecord> records = messageCenterRedisManager.getMessageRecordList(userId);
		/************** 拉取推荐消息信息 *************/
		List<MessageInfoActivity> activitys = messageCenterRedisManager.getRecommendActivityList();
		if (CollectionUtils.isNotEmpty(activitys)) {
			boolean checkResult = true;
			int maxTimeIndex = -1;
			for (int i = 0; i < activitys.size(); i++) {
				// 判断用户是否拉取过该条推荐信息和满足推荐的条件
				checkResult = messageCenterRedisManager.getRecommendActivityMessage(activitys.get(i).getId(), userId);
				if (!checkResult && checkUserActivityMessage(user, activitys.get(i))) {
					// 获取该用户未读的最后一条推荐活动信息
					if (maxTimeIndex == -1 || activitys.get(i).getSend_time().getTime() > activitys.get(maxTimeIndex)
							.getSend_time().getTime()) {
						maxTimeIndex = i;
					}
				}
			}
			// 判断该用户是否满足推荐条件，满足则存入最新的用户未读推荐活动信息
			if (maxTimeIndex != -1) {
				MessageInfoActivity activity = activitys.get(maxTimeIndex);
				if(null != activity){
					MessageInfoRecord infoRecord = new MessageInfoRecord();
					infoRecord.setMessage_content(activity.getActivity_content());
					infoRecord.setCreatetime(activity.getSend_time());
					infoRecord.setMessage_type(CommonStaticConstant.MESSAGE_CENTER_RECOMMEND_ACTIVITY_TYPE);
					records.add(infoRecord); // 加入未读的消息记录
				}
			}
		}
		//组装消息信息,在一级分类消息下加上用户该类型消息的最新消息内容和发送时间
		return converMessageInfos(records,userId);
	}
	
	/**
	 * 
	 * @Description: 按消息分类type分页查询用户30天内的消息记录
	 * @param message_type
	 * @param user_id
	 * @return 
	 * @time: 2016年12月21日 下午2:54:58
	 * @author zhangzhiming  
	 * @throws
	 */
	public List<MessageInfoRecord> searchMessageInfoRecordsByType(int message_type, String user_id, Integer rowNum)
			throws Exception {
		List<MessageInfoRecord> infoRecords = new ArrayList<MessageInfoRecord>();
		rowNum = null == rowNum ? 0 : rowNum;
		//推荐活动记录查询缓存，其他的查询消息记录表
		if(message_type==CommonStaticConstant.MESSAGE_CENTER_RECOMMEND_ACTIVITY_TYPE){
			List<MessageInfoActivity> infos = messageCenterRedisManager.getRecommendActivityList();
			if(CollectionUtils.isNotEmpty(infos)){
				List<MessageInfoActivity> activities = new ArrayList<MessageInfoActivity>();
				//把推荐活动转换为消息记录
				User userSearch = new User();
				userSearch.setId(user_id);
				User user = userMapper.getDetailInfo(userSearch);
				for (MessageInfoActivity activity : infos) {
					//找出满足该用户查询的推荐信息活动
					if(checkUserActivityMessage(user, activity)){
						activities.add(activity);
					}
				}
				//对满足条件的额推荐活动进行倒序
				sortMessageInfoActivitys(activities);
				if(CollectionUtils.isNotEmpty(activities) && activities.size() >= rowNum){//满足条件的推荐活动是否有rowNum条记录
					int searchSize=rowNum+CommonStaticConstant.COUNT;
					//如果满足条件的记录不足以取rowNum到rowCount条，那就取集合剩下的所有记录
					if(searchSize >=activities.size()){
						searchSize = activities.size();
					}
					MessageInfoRecord infoRecord = null;
					MessageInfoActivity activity = null;
					//对集合数据进行分页查询返回
					for (int i = rowNum; i < searchSize; i++) {
						activity = activities.get(i);
						infoRecord = new MessageInfoRecord();
			 			infoRecord.setId(activity.getId());
			 			infoRecord.setTitle(activity.getTitle());
			 			infoRecord.setMessage_content(activity.getActivity_content());
			 			infoRecord.setMessage_type(CommonStaticConstant.MESSAGE_CENTER_RECOMMEND_ACTIVITY_TYPE);
			 			infoRecord.setMessage_pic(activity.getActivity_pic());
			 			infoRecord.setSkip_type(activity.getSkip_type());
			 			infoRecord.setSkip_data(activity.getSkip_data());
			 			infoRecord.setCreatetime(activity.getSend_time());
			 			infoRecords.add(infoRecord);
					}
				}
			}
		}else{
			Map<String, Object> param = new HashMap<String, Object>();
			param.put("message_type", message_type); // 消息类型
			param.put("user_id", user_id); // 所属消息的用户id
			param.put("start_time", this.getSearchDay(-30) + " 00:00:00"); // 查询该用户30天前的数据
			param.put("rowNum", rowNum); // 当前是第几页信息
			param.put("rowCount", CommonStaticConstant.COUNT); // 每页默认拉取30条记录
			infoRecords = messageRecordMapper.getMessageInfoRecordsByType(param);
		}
		return infoRecords;
	}
	
	/**
	 * 
	 * @Description: 从缓存中删除用户类型为message_type的最新消息记录
	 * @param message_type
	 * @param user_id
	 * @return 
	 * @time: 2016年12月21日 下午2:35:58
	 * @author zhangzhiming  
	 * @throws
	 */
	public int delCacheMessageInfo(int message_type, String user_id) throws Exception {
		String file_value = "";
		switch (message_type) {
		case 0: // 客服消息
			file_value = CommonStaticConstant.MESSAGE_CENTER_CHATSERVICE;
			break;
		case 1: // 推荐活动
			file_value = CommonStaticConstant.MESSAGE_CENTER_RECOMMEND_ACTIVITY;
			break;
		case 2: // 三人团团购
			file_value = CommonStaticConstant.MESSAGE_CENTER_COHERE;
			break;
		case 3: // 会员消息
			file_value = CommonStaticConstant.MESSAGE_CENTER_MEMBERS;
			break;
		case 4: // 零元抽奖
			file_value = CommonStaticConstant.MESSAGE_CENTER_LOTTERY;
			break;
		case 5: // 发货和退款
			file_value = CommonStaticConstant.MESSAGE_CENTER_REFUND_LOGISTICS;
			break;
		case 6: // 社区动态
			file_value = CommonStaticConstant.MESSAGE_CENTER_POST;
			break;
		}
		// 从缓存中删除用户类型为message_type的最新消息记录
		return messageCenterRedisManager.delUserMessageRecordByField(file_value, user_id);
	}
	/**
	 * 
	 * @Description: 转换消息信息
	 * @param records 
	 * @time: 2016年12月21日 上午11:15:34
	 * @author zhangzhiming  
	 * @throws
	 */
	private List<MessageInfo> converMessageInfos(List<MessageInfoRecord> infoRecords, String user_id) {
		List<MessageInfo> infos = new ArrayList<MessageInfo>();
		if (CollectionUtils.isNotEmpty(infoRecords)) {
			// 获取消息的配置相关信息
			Map<Integer, MessageInfo> messageInfos = messageCenterRedisManager.getAllMessageInfoConfiguration();
			MessageInfo messageInfo = null;
			MessageInfo chatServiceMessage = null; // 客服消息配置
			if (null != messageInfos && messageInfos.size() > 0) {
				for (MessageInfoRecord infoRecord : infoRecords) {
					// 根据消息记录信息的message_type找到消息对应的配置信息
					messageInfo = messageInfos.get(infoRecord.getMessage_type());
					if (null != messageInfo) {
						messageInfo.setLastMessageContent(infoRecord.getMessage_content());
						messageInfo.setLastMessageSendTime(infoRecord.getCreatetime());
						if (messageInfo.getMessage_type() == CommonStaticConstant.MESSAGE_CENTER_CHATSERVICE_TYPE) {
							chatServiceMessage = messageInfo; // 客服消息需要放在最前面
							continue;
						}
						infos.add(messageInfo); // 组装用户的一级消息
					}
				}
				// 按消息的最新时间进行倒序
				sort(infos);
				if (null != chatServiceMessage) {
					infos.add(0, chatServiceMessage); // 把客服消息放在返回的最前面
				}
				// 清空该用户下的所有最新消息记录
				messageCenterRedisManager.delAllMessageRecordByUserId(user_id);
			}
		}
		return infos;
	}
	
	
	/**
	 * 
	 * @Description: 验证用户是否满足推荐的条件
	 * @param user
	 * @param infoActivity
	 * @return 
	 * @time: 2016年12月21日 下午2:07:19
	 * @author zhangzhiming  
	 * @throws
	 */
	private boolean checkUserActivityMessage(User user, MessageInfoActivity infoActivity) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(new Date());
		calendar.add(Calendar.DAY_OF_MONTH, -30);
		//30天之前的消息过滤掉
		if(infoActivity.getSend_time().before(calendar.getTime())){
			return false;
		}
		boolean checkResult = false;
		if (infoActivity.getSend_range().equals("0")) { // 推荐消息发送范围为全体用户
			checkResult = true;
		} else if (infoActivity.getSend_range().equals("1") && user.getType() == 2) {// 推荐消息发送范围为第三方用户
			checkResult = true;
		} else if (infoActivity.getSend_range().equals("2") && user.getType() == 0) { // 推荐消息发送范围为手机用户
			checkResult = true;
		}
		return checkResult;
	}
	
	
	/**
	 * 
	 * @Description:  获取当前的前dateValue天时间
	 * @param dateValue
	 * @return 
	 * @time: 2016年12月21日 下午3:04:55
	 * @author zhangzhiming  
	 * @throws
	 */
	private String getSearchDay(int dateValue) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(new Date());
		calendar.add(Calendar.DAY_OF_MONTH, dateValue);
		return format.format(calendar.getTime());
	}
	
	
	/**
	 * 
	 * @Description: 对List<MessageInfo>进行按消息的发送时间进行倒序排列
	 * @param messageInfos 
	 * @time: 2016年12月23日 上午9:41:10
	 * @author zhangzhiming  
	 * @throws
	 */
	private void sort(List<MessageInfo> messageInfos) {
		// redis中取出的按lastMessageSendTime倒序
		Collections.sort(messageInfos, new Comparator<MessageInfo>() {
			@Override
			public int compare(MessageInfo m1, MessageInfo m2) {
				return m2.getLastMessageSendTime().compareTo(m1.getLastMessageSendTime());
			}
		});
	}
	
	/**
	 * 
	 * @Description: 对推荐活动进行倒序排序，用于分页
	 * @param activities 
	 * @time: 2016年12月26日 下午2:27:45
	 * @author zhangzhiming  
	 * @throws
	 */
	private void sortMessageInfoActivitys(List<MessageInfoActivity> activities) {
		// redis中取出的按发送时间进行倒序
		Collections.sort(activities, new Comparator<MessageInfoActivity>() {
			@Override
			public int compare(MessageInfoActivity m1, MessageInfoActivity m2) {
				return m2.getSend_time().compareTo(m1.getSend_time());
			}
		});
	}
}
