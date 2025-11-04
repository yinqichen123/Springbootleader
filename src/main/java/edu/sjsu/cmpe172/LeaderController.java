package edu.sjsu.cmpe172;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/leader")
public class LeaderController {
    // GET /leader - Get the current cluster status
    @Autowired
    private ZooKeeperService zooKeeperService;

    @GetMapping
    public ResponseEntity<LeaderResponse> getLeaderStatus() {
        LeaderResponse response = new LeaderResponse(
                zooKeeperService.getLeaderStatus().name(),
                zooKeeperService.getZkStatus().name(),
                zooKeeperService.getCurrentLeader(),
                zooKeeperService.getMyId(),
                zooKeeperService.getMyDescription(),
                zooKeeperService.getPeers()
        );

        return ResponseEntity.ok(response);
    }
    
    // POST /leader/watch - Relinquish leadership and simply observe
    @PostMapping("/watch")
    public ResponseEntity<String> watch() {
        zooKeeperService.stopLeading();
        return ResponseEntity.ok("Now watching (not trying to lead)");
    }
    
    // POST /leader/lead - Participate in leadership election
    @PostMapping("/lead")
    public ResponseEntity<String> lead() {
        zooKeeperService.startLeading();
        return ResponseEntity.ok("Now trying to become leader");
    }
}
