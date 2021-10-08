package mall.logger;

import mall.streamer.bean.AppModelV1;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class mallLoggerApplication {

    private static AppModelV1 SpringApplication;

    public static void main(String[] args) {
        org.springframework.boot.SpringApplication.run(mallLoggerApplication.class, args);
    }
    
}
