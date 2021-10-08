package mall.streamer.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/10/08/11:29
 * @Description:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderDetail {

    private Long id;
    private Long order_id;
    private Long sku_id;
    private BigDecimal order_price;
    private Long sku_num;
    private String sku_name;
    private String create_time;
    private BigDecimal split_total_amount;
    private BigDecimal split_activity_amount;
    private BigDecimal split_coupon_amount;
    private Long create_ts;

    public void setCreate_time(String create_time) throws ParseException {
        this.create_time = create_time;
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        this.create_ts = sdf.parse(create_time).getTime();

    }

}
