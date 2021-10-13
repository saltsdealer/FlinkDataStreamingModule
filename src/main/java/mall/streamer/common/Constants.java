package mall.streamer.common;

/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/09/28/16:31
 * @Description:
 */
public class Constants {
    public static final String TOPIC_ODS_LOG = "ods_log";
    public static final String TOPIC_ODS_DB = "ods_db";

    public static final String TOPIC_DWD_PAGE = "dwd_page";
    public static final String TOPIC_DWD_START = "dwd_start";
    public static final String TOPIC_DWD_DISPLAY = "dwd_display";

    public static final String TOPIC_DWD_FAVOR_INFO = "dwd_favor_info";
    public static final String TOPIC_DWD_CART_INFO = "dwd_cart_info";

    public static final String TOPIC_DWD_REFUND_PAYMENT = "dwd_refund_payment";
    public static final String TOPIC_DWD_COMMENT_INFO = "dwd_comment_info";


    public static final String DWD_SINK_KAFKA = "kafka";
    public static final String DWD_SINK_HBASE = "hbase";

    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    public static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    public static final String PHOENIX_URL = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";
    public static final String MYSQL_URL = "jdbc:mysql://hadoop162:3306/database?user=root&password=aaaaaa";

    public static final String TOPIC_DWM_UV = "dwm_uv";

    public static final String TOPIC_DWM_USER_JUMP_DETAIL = "dwm_uj";

    public static final String TOPIC_DWD_ORDER_INFO = "dwd_order_info";
    public static final String TOPIC_DWD_ORDER_DETAIL = "dwd_order_detail";

    public static final String TOPIC_DWM_ORDER_WIDE = "dwm_order_wide";
    public static final String TOPIC_DWD_PAYMENT_INFO = "dwd_payment_info";

    public static final String TOPIC_DWM_PAYMENT_WIDE = "dwm_payment_wide";
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";
    public static final String CLICKHOUSE_URL_PRE = "jdbc:clickhouse://hadoop102:8123/";


    public static final String FIVE_STAR = "1205";
    public static final String FOUR_STAR = "1204";
    public static final String TOPIC_DWS_PRODUCT_STATS = "dws_product_stats";
}
