package io.nwdaf.eventsubscription.nwdaf_sub_collector.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;



@Controller
public class NefController {
    @GetMapping(value="/collector/notify")
    public String getNotification(@RequestBody String notification) {
        System.out.println(notification);
        return notification;
    }
    
}
