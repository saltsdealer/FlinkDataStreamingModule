package mall.streamer.function;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/10/13/16:00
 * @Description:
 */
@FunctionHint(output = @DataTypeHint("row<source string, ct bigint>"))
public class KwProduct extends TableFunction<Row> {
    public void eval(Long click_ct, Long order_ct, Long cart_ct) {
        if (click_ct > 0) {
            collect(Row.of("click", click_ct));

        }
        if (order_ct > 0) {
            collect(Row.of("order", order_ct));

        }
        if (cart_ct > 0) {
            collect(Row.of("cart", cart_ct));

        }

    }
}
