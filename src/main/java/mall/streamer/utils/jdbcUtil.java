package mall.streamer.utils;

import mall.streamer.common.constants;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/09/29/15:16
 * @Description:
 */
public class jdbcUtil {
    public static Connection getPhoenixConnection(String url) {
        String driver = constants.PHOENIX_DRIVER;
        return getJdbcConnection(driver, url);
    }

    public static Connection getMysqlConnection() {
        return null;
    }

    private static Connection getJdbcConnection(String driver, String url) {

        try {
            Class.forName(driver);
            return DriverManager.getConnection(url);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
