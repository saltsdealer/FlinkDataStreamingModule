package mall.logger;

import mall.streamer.bean.appModel;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class mallLoggerApplication {

    private static appModel SpringApplication;

    public static void main(String[] args) {
        org.springframework.boot.SpringApplication.run(mallLoggerApplication.class, args);
    }
    
}
