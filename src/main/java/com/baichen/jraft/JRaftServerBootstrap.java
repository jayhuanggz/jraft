package com.baichen.jraft;

import com.baichen.jraft.options.JRaftOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.util.concurrent.CountDownLatch;

public class JRaftServerBootstrap {


    public static void main(String[] args) throws Exception {

        Yaml yaml = new Yaml(new Constructor(JRaftOptions.class));
        JRaftOptions options = yaml.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("default-config.yml"));

        JRaftServerBuilder builder = new JRaftServerBuilder(options);
        Server server = builder.build();
        final CountDownLatch latch = new CountDownLatch(1);

        server.subscribe(new ServerListener() {
            @Override
            public void onStarted(Server server) {

            }

            @Override
            public void onDestroyed(Server server) {
                latch.countDown();
            }
        });
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread((server::destroy)));
        latch.await();
    }
}
