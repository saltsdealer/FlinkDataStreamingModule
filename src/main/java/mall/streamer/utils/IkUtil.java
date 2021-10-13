package mall.streamer.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;
/**
 * For educational purposes only
 *
 * @Author: saltsdealer@gmail.com
 * @Date: 2021/10/13/11:17
 * @Description:
 */
public class IkUtil {
    public static void main(String[] args) {

    }
    public static Set<String> split(String keyword) {
        HashSet<String> words = new HashSet<>();
        // string
        StringReader reader = new StringReader(keyword);
        // smart  _ maxword
        IKSegmenter segmenter = new IKSegmenter(reader, true);

        try {
            Lexeme next = segmenter.next();
            while (next != null) {

                String word = next.getLexemeText();

                words.add(word);
                next = segmenter.next();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return words;
    }
}
