package mall.streamer.bean;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/10/13/11:47
 * @Description:
 */
@NoArgsConstructor
@AllArgsConstructor
public class KeywordStats {
    //start time
    private String stt;
    //end time
    private String edt;

    private String keyword;
    private String source;
    private Long ct;

    private Long ts;
}
