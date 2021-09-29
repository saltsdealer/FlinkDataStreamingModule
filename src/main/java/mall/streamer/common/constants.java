package mall.streamer.common;

/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/09/28/16:31
 * @Description:
 */
public class constants {
    public static final String TOPIC_ODS_LOG = "ods_log";
    public static final String TOPIC_ODS_DB = "ods_db";

    public static final String TOPIC_DWD_PAGE = "dwd_page";
    public static final String TOPIC_DWD_START = "dwd_start";
    public static final String TOPIC_DWD_DISPLAY = "dwd_display";

    public static final String DWD_SINK_KAFKA = "kafka";
    public static final String DWD_SINK_HBASE = "hbase";

    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    public static final String PHOENIX_URL = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";
}
