package mall.streamer.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/10/11/15:20
 * @Description:
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ProductStats {
    // start time of the page
    private String stt;
    // end time of the page
    private String edt;
    private Long sku_id;
    private String sku_name;
    private BigDecimal sku_price;
    private Long spu_id;
    private String spu_name;
    // trademark id
    private Long tm_id;
    private String tm_name;
    private Long category3_id;
    private String category3_name;
    // ad count
    private Long display_ct = 0L;
    private Long click_ct = 0L;
    private Long favor_ct = 0L;
    private Long cart_ct = 0L;
    // sku ordered
    private Long order_sku_num = 0L;
    private BigDecimal order_amount = BigDecimal.ZERO;
    private Long order_ct = 0L;
    // payment
    private BigDecimal payment_amount = BigDecimal.ZERO;
    private Long paid_order_ct = 0L;
    private Long refund_order_ct = 0L;
    private BigDecimal refund_amount = BigDecimal.ZERO;
    private Long comment_ct = 0L;
    private Long good_comment_ct = 0L;

    @NoSink
    private Set<Long> orderIdSet = new HashSet<>();
    @NoSink
    private Set<Long> paidOrderIdSet = new HashSet<>();
    @NoSink
    private Set<Long> refundOrderIdSet = new HashSet<>();

    private Long ts;
}
