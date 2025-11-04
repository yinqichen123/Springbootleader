package edu.sjsu.cmpe172;

import java.util.List;

public class LeaderResponse {
    private String status;    // Current leader status
    private String zookeeper; // ZooKeeper connection status
    private String leader;    // Current Leader ID
    private String myid;      // own ID
    private String description;  //Node description
    private List<String> peers;  // List of all peer nodes

    public LeaderResponse() {}

    public LeaderResponse(String status, String zookeeper, String leader,                      // reference: https://learn.microsoft.com/en-us/azure/architecture/patterns/leader-election
                          String myid, String description, List<String> peers) {               // reference: https://zookeeper.apache.org/doc/r3.1.2/javaExample.html
        // Create response object
        this.status = status;
        this.zookeeper = zookeeper;
        this.leader = leader;
        this.myid = myid;
        this.description = description;
        this.peers = peers;
    }

    // Getters and Setters                         // reference: https://docs.spring.io/spring-boot/reference/actuator/endpoints.html
    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getZookeeper() {
        return zookeeper;
    }

    public void setZookeeper(String zookeeper) {
        this.zookeeper = zookeeper;
    }

    public String getLeader() {
        return leader;
    }

    public void setLeader(String leader) {
        this.leader = leader;
    }

    public String getMyid() {
        return myid;
    }

    public void setMyid(String myid) {
        this.myid = myid;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<String> getPeers() {
        return peers;
    }

    public void setPeers(List<String> peers) {
        this.peers = peers;
    }
}
