package mall.logger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Author saltsdealer@gmail.com
 * @Date 2021/9/27 10:13
 * @Descripition
 */

@RestController
@Slf4j
public class Logger {
    
    @RequestMapping("/applog")
    public String logger(@RequestParam("param") String log) {

       saveToDisk(log);

       sendToKafka(log);
        
        return "ok";
    }
    
    @Autowired
    KafkaTemplate<String, String> kafka;
    
    private void sendToKafka(String log) {
        kafka.send("ods_log", log);
    }
    
    private void saveToDisk(String stringLog) {
        log.info(stringLog);
    }
}
