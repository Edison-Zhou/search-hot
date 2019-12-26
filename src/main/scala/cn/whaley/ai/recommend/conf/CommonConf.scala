package cn.whaley.ai.recommend.conf

/**
 * Created by xutong on 2017/2/7.
 * 基础配置信息
 */
trait CommonConf {

  //告警邮箱
  val email: String = "zhou.zhenyu@moretv.com.cn"

  //告警邮件前缀
  val prefix: String = "[北京机房][AI][zhouzhenyu]"

  // 推荐长视频类型
  val longVideoTypeArray : Array[String] = Array("movie", "tv", "zongyi", "comic", "kids", "jilu")
  val longVideoType: String = "('" + longVideoTypeArray.mkString("','") + "')"

  // 推荐短视频类型
  val shortVideoTypeArray : Array[String] = Array("hot", "mv", "xiqu", "game")
  val shortVideoType: String = "('" + shortVideoTypeArray.mkString("','") + "')"

  // 推荐视频
  val videoTypeList: List[String] = List("movie","tv","zongyi","comic","jilu","kids","mv","xiqu","hot","sports","game")
  val videoType: String = "('" + videoTypeList.mkString("','") + "')"

  // ods_view层的前缀
  val HELIOS_LOG_PREFIX = "ods_view.log_whaleytv_main_"
  val MEDUSA_LOG_PREFIX = "ods_view.log_medusa_main3x_"

}
