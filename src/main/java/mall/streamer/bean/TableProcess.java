package mall.streamer.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/09/28/16:29
 * @Description:
 */

@Data
@NoArgsConstructor
@AllArgsConstructor

public class TableProcess {
    private String source_table;
    private String operate_type;
    private String sink_type;
    private String sink_table;
    private String sink_columns;
    private String sink_pk;
    private String sink_extend;
}
