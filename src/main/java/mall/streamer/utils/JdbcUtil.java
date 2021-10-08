package mall.streamer.utils;

import mall.streamer.bean.OrderInfo;
import mall.streamer.common.Constants;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/09/29/15:16
 * @Description:
 */
public class JdbcUtil {
    public static Connection getPhoenixConnection(String url) {
        String driver = Constants.PHOENIX_DRIVER;
        return getJdbcConnection(driver, url);
    }

    public static Connection getMysqlConnection(String url) {

        return getJdbcConnection(Constants.MYSQL_DRIVER, url);
    }

    public static Connection getJdbcConnection(String driver, String url) {
        try {
            Class.forName(driver);
            return DriverManager.getConnection(url);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        List<OrderInfo> list =
                queryList(getMysqlConnection(Constants.MYSQL_URL), "select * from order_info", new String[]{}, OrderInfo.class);

        for (OrderInfo obj : list) {
            System.out.println(obj);
        }
    }

    public static <T> List<T> queryList(Connection conn, String sql, Object[] args, Class<T> tClass) {
        // User(id, name)

        List<T> result = new ArrayList<>();
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(sql);
            for (int i = 0; i < args.length; i++) {
                ps.setObject(i + 1, args[i]);
            }
            /*
            id  name
            1    a
            2    b
             */

            ResultSet resultSet = ps.executeQuery();

            // acquire metadata infos
            ResultSetMetaData metaData = resultSet.getMetaData();

            while (resultSet.next()) {
                T t = tClass.newInstance();

                for (int i = 1; i <= metaData.getColumnCount(); i++) {

                    String columnName = metaData.getColumnLabel(i);

                    Object value = resultSet.getObject(columnName);
                    BeanUtils.setProperty(t, columnName, value);
                }
                result.add(t);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        return result;
    }
}
