package mall.streamer.function;

import mall.streamer.utils.IkUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2022/10/13/11:43
 * @Description:
 */
@FunctionHint(output = @DataTypeHint("row<word string>"))
public class IkAnalyzer extends TableFunction<Row> {
    public void eval(String keyword){
        for (String word : IkUtil.split(keyword)) {
            collect(Row.of(word));
        }
    }
}
