package mall.streamer.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/10/10/16:50
 * @Description:
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class VisitorStats {
    //start time
    private String stt;
    //end time
    private String edt;
    //version
    private String vc;
    //channel
    private String ch;
    //area
    private String ar;
    //if is new user
    private String is_new;
    //user visit counts
    private Long uv_ct = 0L;
    //page visit counts
    private Long pv_ct = 0L;
    //start from = last page id count
    private Long sv_ct = 0L;
    //user jump counts
    private Long uj_ct = 0L;
    // visit duration
    private Long dur_sum = 0L;
    // time stamp
    private Long ts;
}
