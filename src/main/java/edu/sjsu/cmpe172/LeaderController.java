package edu.sjsu.cmpe172;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController                                     // reference:https://www.baeldung.com/spring-boot-customize-jackson-objectmapper
@RequestMapping("/leader")                          // reference: https://docs.spring.io/spring-framework/reference/web/webmvc/mvc-controller/ann-requestmapping.html
public class LeaderController {
    // GET /leader - Get the current cluster status
    @Autowired                                        // reference:https://medium.com/devdomain/spring-boots-autowired-vs-constructor-injection-a-detailed-guide-1b19970d828e
    private ZooKeeperService zooKeeperService;

    @GetMapping                                                    // reference: https://spring.io/guides/tutorials/rest
    // Handling GET requests: GET http://localhost:8082/leader
    public ResponseEntity<LeaderResponse> getLeaderStatus() {
        // This method returns the current state of the cluster

        // Create a response object and package various information
        LeaderResponse response = new LeaderResponse(
            // Convert the enumeration to a string
                zooKeeperService.getLeaderStatus().name(),  // my status
                zooKeeperService.getZkStatus().name(),      // connection status
                zooKeeperService.getCurrentLeader(),        // current leader
                zooKeeperService.getMyId(),                 // my ID
                zooKeeperService.getMyDescription(),        // my description
                zooKeeperService.getPeers()                 // all nodes
        );
        // The response returns HTTP 200 OK, and the body is a JSON response.
        return ResponseEntity.ok(response);
    }
    
    // Handle a POST request: POST http://localhost:8082/leader/watch
    // Test with curl: curl -X POST http://localhost:8082/leader/watch
    @PostMapping("/watch")                                          // reference: https://www.baeldung.com/spring-new-requestmapping-shortcuts
    public ResponseEntity<String> watch() {
        // This method causes the node to stop election and enter observer mode.
        zooKeeperService.stopLeading();
        // If I were the leader, I would relinquish leadership.

        // Change the status to WATCHING.
        return ResponseEntity.ok("Now watching (not trying to lead)");
    }
    
    // Handle a POST request: POST http://localhost:8082/leader/lead
    // Test with curl: curl -X POST http://localhost:8082/leader/lead
    @PostMapping("/lead")
    public ResponseEntity<String> lead() {
        // This method allows nodes to begin electing a leader.
        zooKeeperService.startLeading();
        // Set the "I want to be a leader" flag
        // If there is no leader, attempt to become a leader
        // If there is a leader, enter a waiting state
        return ResponseEntity.ok("Now trying to become leader");
    }
}
