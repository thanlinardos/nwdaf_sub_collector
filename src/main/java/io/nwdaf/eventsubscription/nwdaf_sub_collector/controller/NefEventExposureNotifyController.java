package io.nwdaf.eventsubscription.nwdaf_sub_collector.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

@RestController
public class NefEventExposureNotifyController {

    @PostMapping("/nefeventexposure/notify")
    public ResponseEntity<String> nefEventExposureNotification(@RequestBody @Valid String request) {
        return ResponseEntity.ok("NefEventExposureNotifyController: " + request);
    }
}
