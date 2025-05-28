package com.fly.mq;

import com.fly.mq.mqserver.core.Binding;
import com.fly.mq.mqserver.core.ExchangeType;
import com.fly.mq.mqserver.core.Message;
import com.fly.mq.mqserver.core.Router;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class RouterTests {

    Binding binding = null;
    Message message = null;

    @BeforeEach
    public void setUp(){
        binding = new Binding();
        message = new Message();
    }

    @AfterEach
    public void tearDown(){
        binding = null;
        message = null;
    }


    // [测试用例]
    // binding key          routing key         result
    // aaa                  aaa                 true
    // aaa.bbb              aaa.bbb             true
    // aaa.bbb              aaa.bbb.ccc         false
    // aaa.bbb              aaa.ccc             false
    // aaa.bbb.ccc          aaa.bbb.ccc         true
    // aaa.*                aaa.bbb             true
    // aaa.*.bbb            aaa.bbb.ccc         false
    // *.aaa.bbb            aaa.bbb             false
    // #                    aaa.bbb.ccc         true
    // aaa.#                aaa.bbb             true
    // aaa.#                aaa.bbb.ccc         true
    // aaa.#.ccc            aaa.ccc             true
    // aaa.#.ccc            aaa.bbb.ccc         true
    // aaa.#.ccc            aaa.aaa.bbb.ccc     true
    // #.ccc                ccc                 true
    // #.ccc                aaa.bbb.ccc         true

    // aaa                  aaa                 true
    @Test
    public void test1(){
        binding.setBindingKey("aaa");
        message.setRoutingKey("aaa");

        Assertions.assertTrue(Router.rout(ExchangeType.TOPIC, binding, message));
    }

    // aaa.bbb              aaa.bbb             true
    @Test
    public void test2(){
        binding.setBindingKey("aaa.bbb");
        message.setRoutingKey("aaa.bbb");

        Assertions.assertTrue(Router.rout(ExchangeType.TOPIC, binding, message));
    }

    // aaa.bbb              aaa.bbb.ccc         false
    @Test
    public void test3(){
        binding.setBindingKey("aaa.bbb");
        message.setRoutingKey("aaa.bbb.ccc");

        Assertions.assertFalse(Router.rout(ExchangeType.TOPIC, binding, message));
    }

    // aaa.bbb              aaa.ccc             false
    @Test
    public void test4(){
        binding.setBindingKey("aaa.bbb");
        message.setRoutingKey("aaa.ccc");

        Assertions.assertFalse(Router.rout(ExchangeType.TOPIC, binding, message));
    }

    // aaa.bbb.ccc          aaa.bbb.ccc         true
    @Test
    public void test5(){
        binding.setBindingKey("aaa.bbb.ccc");
        message.setRoutingKey("aaa.bbb.ccc");

        Assertions.assertTrue(Router.rout(ExchangeType.TOPIC, binding, message));
    }

    // aaa.*                aaa.bbb             true
    @Test
    public void test6(){
        binding.setBindingKey("aaa.*");
        message.setRoutingKey("aaa.bbb");

        Assertions.assertTrue(Router.rout(ExchangeType.TOPIC, binding, message));
    }

    // aaa.*.bbb            aaa.bbb.ccc         false
    @Test
    public void test7(){
        binding.setBindingKey("aaa.*.bbb");
        message.setRoutingKey("aaa.bbb.ccc");

        Assertions.assertFalse(Router.rout(ExchangeType.TOPIC, binding, message));
    }

    // *.aaa.bbb            aaa.bbb             false
    @Test
    public void test8(){
        binding.setBindingKey("*.aaa.bbb");
        message.setRoutingKey("aaa.bbb");

        Assertions.assertFalse(Router.rout(ExchangeType.TOPIC, binding, message));
    }

    // #                    aaa.bbb.ccc         true
    @Test
    public void test9(){
        binding.setBindingKey("#");
        message.setRoutingKey("aaa.bbb.ccc");

        Assertions.assertTrue(Router.rout(ExchangeType.TOPIC, binding, message));
    }

    // aaa.#                aaa.bbb             true
    @Test
    public void test10(){
        binding.setBindingKey("aaa.#");
        message.setRoutingKey("aaa.bbb");

        Assertions.assertTrue(Router.rout(ExchangeType.TOPIC, binding, message));
    }

    // aaa.#                aaa.bbb.ccc         true
    @Test
    public void test11(){
        binding.setBindingKey("aaa.#");
        message.setRoutingKey("aaa.bbb.ccc");

        Assertions.assertTrue(Router.rout(ExchangeType.TOPIC, binding, message));
    }

    // aaa.#.ccc            aaa.ccc             true
    @Test
    public void test12(){
        binding.setBindingKey("aaa.#.ccc");
        message.setRoutingKey("aaa.ccc");

        Assertions.assertTrue(Router.rout(ExchangeType.TOPIC, binding, message));
    }

    // aaa.#.ccc            aaa.bbb.ccc         true
    @Test
    public void test13(){
        binding.setBindingKey("aaa.#.ccc");
        message.setRoutingKey("aaa.bbb.ccc");

        Assertions.assertTrue(Router.rout(ExchangeType.TOPIC, binding, message));
    }

    // aaa.#.ccc            aaa.aaa.bbb.ccc     true
    @Test
    public void test14(){
        binding.setBindingKey("aaa.#.ccc");
        message.setRoutingKey("aaa.aaa.bbb.ccc");

        Assertions.assertTrue(Router.rout(ExchangeType.TOPIC, binding, message));
    }

    // #.ccc                ccc                 true
    @Test
    public void test15(){
        binding.setBindingKey("#.ccc");
        message.setRoutingKey("ccc");

        Assertions.assertTrue(Router.rout(ExchangeType.TOPIC, binding, message));
    }

    // #.ccc                aaa.bbb.ccc         true
    @Test
    public void test16(){
        binding.setBindingKey("#.ccc");
        message.setRoutingKey("aaa.bbb.ccc");

        Assertions.assertTrue(Router.rout(ExchangeType.TOPIC, binding, message));
    }
}
