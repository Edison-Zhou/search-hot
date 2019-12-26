package cn.whaley.ai.recommend.voiceoptimize.conf

import cn.whaley.ai.recommend.conf.CommonConf

/**
 * Created by Administrator on 2017/7/26.
 */
trait VoiceOptimizeConf extends CommonConf{

  //评分矩阵路径
  val HELIOS_SCOREMATRIX_PATH = "/user/hive/warehouse/ai.db/dw_base_behavior_raw/product_line=helios/score_source=play/partition_tag="
  val MORETV_SCOREMATRIX_PATH = "/ai/data/dw/moretv/fullAmountScore/scoreMatrix/"

  val HELIOS_VOICE_OPTIMIZE_TOPIC = "helios-voice-search"

  //mysql info
  val moretvMetaData: String = "moretv_recommend_mysql"
  val heliosMetaData: String = "helios_recommend_mysql"
  val metaDataTableName: String = "mtv_program"


}
