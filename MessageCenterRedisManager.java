package com.yjh.core.redis.message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.yjh.core.common.CommonStaticConstant;
import com.yjh.core.model.messageCenter.MessageInfo;
import com.yjh.core.model.messageCenter.MessageInfoActivity;
import com.yjh.core.model.messageCenter.MessageInfoRecord;
import com.yjh.core.redis.YNFRedisMessageManager;
import com.yjh.core.util.SerializeUtil;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

/**
 * 
 * 类名称：MessageCenterRedisManager   
 * 类描述： 消息中心cache处理类  
 * 创建人：zhangzhiming
 * 创建时间：2016年12月16日 上午10:36:28   
 * 修改人：zhangzhiming
 * 修改时间：2016年12月16日 上午10:36:28   
 * 修改备注：   
 * @version
 */
@Component
public class MessageCenterRedisManager extends YNFRedisMessageManager {
	private final Logger logger = LoggerFactory.getLogger(MessageCenterRedisManager.class);
	
	//推荐活动消息key
	private static final String RECOMMEND_ACTIVITY_KEY = "recommend_activity:"; 
	
	//所有类型消息的配置信息key
	private static final String ALL_MESSAGE_INFO_CONFIGURATION= "all_message_info_configuration";
	
	//所有的推荐活动key
	private static final String ALL_RECOMMEND_ACTIVITY= "all_recommend_activity";
	
	//用户分类的未读消息列表key
	private static final String USER_NEW_MESSAGE_RECORD= "user_new_message_record:";
	
	//redis存储的超时时间（秒），过期删除
    public static final int EXPIRE = 60 * 60 * 24 * 30;   //默认存储一个月
	 
	/**
	 * 
	 * @Description: 判断用户是否拉取过推荐消息
	 * @param user_id
	 * @return 
	 * @time: 2016年12月16日 上午10:33:11
	 * @author zhangzhiming  
	 * @throws
	 */
	  public boolean getRecommendActivityMessage(String activity_id,String user_id)
	    {
	        Jedis jedis = null;
	        boolean flag = true;
	        try
	        {
	            Long user_offset = Long.valueOf(user_id);
	            // 拿连接
	            jedis = this.getConnection();
	            flag = jedis.setbit(RECOMMEND_ACTIVITY_KEY+activity_id, user_offset, true);
	        }
	        catch (Exception e)
	        {
	            logger.error(user_id+"拉取推荐活动消息为:"+activity_id+"失败:"+e);
	        }
	        finally
	        {
	            // 在返回之前释放连接
	            returnConnection(jedis);
	        }
	        return flag;
	    }
	  
	  /**
	   * 
	   * @Description: TODO
	   * @param activity_id
	   * @param user_id
	   * @return 把发布的推荐活动存储到redis中
	   * @time: 2016年12月16日 上午10:51:09
	   * @author zhangzhiming  
	   * @throws
	   */
	  public void insertRecommendActivity(MessageInfoActivity infoActivity)
	    {
	        Jedis jedis = null;
	        try
	        {
	            // 拿连接
	            jedis = this.getConnection();
	            
	             jedis.rpush(ALL_RECOMMEND_ACTIVITY.getBytes(), SerializeUtil.serialize(infoActivity));
	            
	        }
	        catch (Exception e)
	        {
	            logger.error("发布的推荐活动ID为:"+infoActivity.getId()+"存储到redis中失败:"+e);
	        }
	        finally
	        {
	            // 在返回之前释放连接
	            returnConnection(jedis);
	        }
	    }
	  
	  
	  /**
	   * 
	   * @Description: 删除指定的推荐活动记录
	   * @param activities 
	   * @time: 2016年12月23日 上午10:40:06
	   * @author zhangzhiming  
	   * @throws
	   */
	public void delRecommendActivity(List<MessageInfoActivity> activities) {
		// 列表不能为空
		if (CollectionUtils.isNotEmpty(activities)) {
			logger.debug("开始删除指定的推荐活动记录,记录大小为:"+activities.size());
			Jedis jedis = null;
			try {
				// 拿连接
				jedis = this.getConnection();
				// 清除指定的推荐活动记录信息
				for (MessageInfoActivity messageInfoActivity : activities) {
					jedis.lrem(ALL_RECOMMEND_ACTIVITY.getBytes(), 0, SerializeUtil.serialize(messageInfoActivity));
				}
			} catch (Exception e) {
				logger.error("删除指定的推荐活动信息失败,失败原因:" + e);
			} finally {
				// 在返回之前释放连接
				returnConnection(jedis);
			}
		}
	}
	
	/**
	 * 
	 * @Description: 查询所有推荐活动消息 
	 * @return 
	 * @time: 2016年12月16日 上午11:08:48
	 *  @author zhangzhiming 
	 *  @throws
	 */
	public List<MessageInfoActivity> getRecommendActivityList() {
		Jedis jedis = null;
		List<MessageInfoActivity> result = new ArrayList<MessageInfoActivity>();
		try {
			// 拿连接
			jedis = this.getConnection();
			
			// 这里用redis的管道，一次执行多个命令，提高效率
            Pipeline pipeline = jedis.pipelined();
            
            Response<List<byte[]>> responseList = pipeline.lrange((ALL_RECOMMEND_ACTIVITY).getBytes(), 0, -1);
            
            //触发执行
         	pipeline.sync();
           // 转换redis返回内容
         	List<byte[]> infoList = responseList.get();
         	
			if (CollectionUtils.isNotEmpty(infoList)) {
				for (byte[] bs : infoList) {
					if(null != bs){
						result.add((MessageInfoActivity) SerializeUtil.unserialize(bs));
					}
				}
			}
		} catch (Exception e) {
			logger.error("查询推荐活动消息失败:" + e);
		} finally {
			// 在返回之前释放连接
			returnConnection(jedis);
		}
		return result;
	}
	
	
	/**
	 * 
	 * @Description: 批量存储消息配置信息，用于初始化消息的配置信息
	 * @param messageInfos 
	 * @time: 2016年12月26日 下午1:43:14
	 * @author zhangzhiming  
	 * @throws
	 */
	public void setMessageInfoList(List<MessageInfo> messageInfos) {
		String fieldValue = "";
		Jedis jedis = null;
		try {
			if(CollectionUtils.isNotEmpty(messageInfos)){
				jedis = this.getConnection();
				for (MessageInfo messageInfo : messageInfos) {
					switch (messageInfo.getMessage_type()) {
					case 0: // 客服消息
						fieldValue = CommonStaticConstant.MESSAGE_CENTER_CHATSERVICE;
						break;
					case 1: // 推荐活动
						fieldValue = CommonStaticConstant.MESSAGE_CENTER_RECOMMEND_ACTIVITY;
						break;
					case 2: // 三人团团购
						fieldValue = CommonStaticConstant.MESSAGE_CENTER_COHERE;
						break;
					case 3: // 会员消息
						fieldValue = CommonStaticConstant.MESSAGE_CENTER_MEMBERS;
						break;
					case 4: // 零元抽奖
						fieldValue = CommonStaticConstant.MESSAGE_CENTER_LOTTERY;
						break;
					case 5: // 物流和退款
						fieldValue = CommonStaticConstant.MESSAGE_CENTER_REFUND_LOGISTICS;
						break;
					case 6: //社区动态
						fieldValue = CommonStaticConstant.MESSAGE_CENTER_POST;
						break;
					default:
						continue;
					}
					jedis.hset(ALL_MESSAGE_INFO_CONFIGURATION.getBytes(), fieldValue.getBytes(),
							SerializeUtil.serialize(messageInfo));
				}
			}
		} catch (Exception e) {
			logger.error("批量存储消息配置信息失败,失败原因:" + e);
		}finally {
			// 在返回之前释放连接
			returnConnection(jedis);
		}
	}
	
	
	/**
	 * 
	 * @Description: 存储消息的配置信息
	 * @param messageInfo
	 * @param fieldValue 
	 * @time: 2016年12月16日 下午5:24:11
	 * @author zhangzhiming  
	 * @throws
	 */
	public void setMessageInfo(MessageInfo messageInfo) {
		String fieldValue = "";
		switch (messageInfo.getMessage_type()) {
		case 0: // 客服消息
			fieldValue = CommonStaticConstant.MESSAGE_CENTER_CHATSERVICE;
			break;
		case 1: // 推荐活动
			fieldValue = CommonStaticConstant.MESSAGE_CENTER_RECOMMEND_ACTIVITY;
			break;
		case 2: // 三人团团购
			fieldValue = CommonStaticConstant.MESSAGE_CENTER_COHERE;
			break;
		case 3: // 会员消息
			fieldValue = CommonStaticConstant.MESSAGE_CENTER_MEMBERS;
			break;
		case 4: // 零元抽奖
			fieldValue = CommonStaticConstant.MESSAGE_CENTER_LOTTERY;
			break;
		case 5: // 物流和退款
			fieldValue = CommonStaticConstant.MESSAGE_CENTER_REFUND_LOGISTICS;
			break;
		case 6: // 社区动态
			fieldValue = CommonStaticConstant.MESSAGE_CENTER_POST;
			break;
		default:
			return;
		}
		Jedis jedis = null;
		try {
			// 拿连接
			jedis = this.getConnection();

			jedis.hset(ALL_MESSAGE_INFO_CONFIGURATION.getBytes(), fieldValue.getBytes(),
					SerializeUtil.serialize(messageInfo));

		} catch (Exception e) {
			logger.error("消息ID为:" + messageInfo.getId() + "的配置信息存储到redis中失败:" + e);
		} finally {
			// 在返回之前释放连接
			returnConnection(jedis);
		}
	}
	
	/**
	 * 
	 * @Description: 根据field获取消息的配置信息
	 * @param fieldValue
	 * @return 
	 * @time: 2016年12月21日 上午10:54:49
	 * @author zhangzhiming  
	 * @throws
	 */
	public MessageInfo getMessageInfoByField(String fieldValue)
	{
		Jedis jedis = null;
		MessageInfo messageInfo = null;
		try {
			// 拿连接
			jedis = this.getConnection();
			
			Pipeline pipeline = jedis.pipelined();
			Response<List<byte[]>> resultResponse = pipeline.hmget((ALL_MESSAGE_INFO_CONFIGURATION).getBytes(), fieldValue.getBytes());
			//触发执行
         	pipeline.sync();
         	// 转换redis返回内容
         	List<byte[]> infoList = resultResponse.get();
			if (CollectionUtils.isNotEmpty(infoList) && null != infoList.get(0)) {
				messageInfo = (MessageInfo)SerializeUtil.unserialize(infoList.get(0));
			}
		} catch (Exception e) {
			logger.error("根据field获取消息的配置信息失败,失败原因为:" + e);
		} finally {
			// 在返回之前释放连接
			returnConnection(jedis);
		}
		return messageInfo;
	}
	
	
	/**
	 * 
	 * @Description: 查询所有类型消息的配置信息
	 * @return 
	 * @time: 2016年12月16日 上午11:08:48
	 *  @author zhangzhiming 
	 *  @throws
	 */
	public Map<Integer, MessageInfo> getAllMessageInfoConfiguration() {
		Jedis jedis = null;
		Map<Integer, MessageInfo> resultMap = new HashMap<Integer, MessageInfo>();
		MessageInfo messageInfo = new MessageInfo();
		try {
			// 拿连接
			jedis = this.getConnection();
			// 这里用redis的管道，一次执行多个命令，提高效率
			Pipeline pipeline = jedis.pipelined();
			//获取该key下面所有的field字段和value值
			Response<Map<byte[],byte[]>>  responseResult = pipeline.hgetAll(ALL_MESSAGE_INFO_CONFIGURATION.getBytes());
			//触发执行
         	pipeline.sync();
			// 转换redis返回内容
         	Map<byte[],byte[]>  infoMap = responseResult.get();
			if (null != infoMap && infoMap.size() >=0) {
				Set<byte[]> set = infoMap.keySet();
				 Iterator<byte[]> i = set.iterator();
					while(i.hasNext()){
						messageInfo = (MessageInfo)SerializeUtil.unserialize(infoMap.get(i.next()));   //反序列化Value
						resultMap.put(messageInfo.getMessage_type(), messageInfo);
					}
			}
		} catch (Exception e) {
			logger.error("查询所有的消息配置信息失败:" + e);
		} finally {
			// 在返回之前释放连接
			returnConnection(jedis);
		}
		return resultMap;
	}
	
	
	/**
	 * 
	 * @Description: 把最新的一条消息记录存储到缓存中
	 * @return 
	 * @time: 2016年12月16日 上午11:08:48
	 *  @author zhangzhiming 
	 *  @throws
	 */
	public void setMessageRecord(MessageInfoRecord infoRecord, String fieldValue, String user_id)
	{
		Jedis jedis = null;
		try {
			// 拿连接
			jedis = this.getConnection();
			
			jedis.hset((USER_NEW_MESSAGE_RECORD+user_id).getBytes(), fieldValue.getBytes(), SerializeUtil.serialize(infoRecord));
			
			 jedis.expire((USER_NEW_MESSAGE_RECORD+user_id).getBytes(), EXPIRE);  //更新用户的消息记录有效时间
		} catch (Exception e) {
			logger.error("插入user_id为:"+user_id+"的新消息记录存储到缓存中失败，失败原因:" + e);
		} finally {
			// 在返回之前释放连接
			returnConnection(jedis);
		}
	}
	
	/**
	 * 
	 * @Description: 根据fieldValue删除用户消息的最新一条记录
	 * @param fieldValue
	 * @param user_id
	 * @return 
	 * @time: 2016年12月21日 上午10:34:58
	 * @author zhangzhiming  
	 * @throws
	 */
	public int delUserMessageRecordByField(String fieldValue,String user_id)
	{
		long result = 0L;
		Jedis jedis = null;
		try {
			// 拿连接
			jedis = this.getConnection();
			result = jedis.hdel((USER_NEW_MESSAGE_RECORD+user_id).getBytes(), fieldValue.getBytes());
		} catch (Exception e) {
			logger.error("删除用户ID为:"+user_id+"的field为"+fieldValue+"的消息记录失败，失败原因:" + e);
		} finally {
			// 在返回之前释放连接
			returnConnection(jedis);
		}
		return (int)result;
	}
	
	
	/**
	 * 
	 * @Description: 根据user_id情况缓存中的最新消息记录
	 * @param user_id
	 * @return 
	 * @time: 2016年12月23日 上午9:29:45
	 * @author zhangzhiming  
	 * @throws
	 */
	public int delAllMessageRecordByUserId(String user_id) {
		long result = 0L;
		Jedis jedis = null;
		try {
			// 拿连接
			jedis = this.getConnection();
			result = jedis.hdel((USER_NEW_MESSAGE_RECORD + user_id).getBytes(),
					CommonStaticConstant.MESSAGE_CENTER_CHATSERVICE.getBytes(),  //客服消息
					CommonStaticConstant.MESSAGE_CENTER_MEMBERS.getBytes(),		//会员消息
					CommonStaticConstant.MESSAGE_CENTER_LOTTERY.getBytes(),			//抽奖消息
					CommonStaticConstant.MESSAGE_CENTER_COHERE.getBytes(),			//三人团消息
					CommonStaticConstant.MESSAGE_CENTER_RECOMMEND_ACTIVITY.getBytes(),  //推荐活动消息
					CommonStaticConstant.MESSAGE_CENTER_REFUND_LOGISTICS.getBytes(),//物流和退款消息
					CommonStaticConstant.MESSAGE_CENTER_POST.getBytes());  //内容社区
		} catch (Exception e) {
			logger.error("删除用户ID为:" + user_id + "的所有最新消息记录失败，失败原因:" + e);
		} finally {
			// 在返回之前释放连接
			returnConnection(jedis);
		}
		return (int) result;
	}
	
	/**
	 * 
	 * @Description: 获取消息记录列表
	 * @param user_id
	 * @return 
	 * @time: 2016年12月21日 下午5:53:44
	 * @author zhangzhiming  
	 * @throws
	 */
	public List<MessageInfoRecord> getMessageRecordList(String user_id) {
		Jedis jedis = null;
		List<MessageInfoRecord> resultList =new  ArrayList<MessageInfoRecord>();
		try {
			// 拿连接
			jedis = this.getConnection();
			
			// 这里用redis的管道，一次执行多个命令，提高效率
			Pipeline pipeline = jedis.pipelined();
			//获取该key下面所有的field字段和value值
			//Response<Map<byte[],byte[]>>  responseResult = pipeline.hgetAll((USER_NEW_MESSAGE_RECORD+user_id).getBytes());
			Response<List<byte[]>>  responseResult = pipeline.hvals((USER_NEW_MESSAGE_RECORD+user_id).getBytes());
			
			//触发执行
         	pipeline.sync();
			// 转换redis返回内容
         	List<byte[]>  infoList = responseResult.get();
			if (CollectionUtils.isNotEmpty(infoList)) {
				for (byte[] bs : infoList) {
					if(null != bs){
						resultList.add((MessageInfoRecord)SerializeUtil.unserialize(bs));
					}
				}
			}
		} catch (Exception e) {
			logger.error("查询用户" + user_id + "最新的消息记录失败:" + e);
		} finally {
			// 在返回之前释放连接
			returnConnection(jedis);
		}
		return resultList;
	}
	
}
