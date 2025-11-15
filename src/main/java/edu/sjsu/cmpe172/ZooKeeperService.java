package edu.sjsu.cmpe172;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

@Service
public class ZooKeeperService implements Watcher {
    // Implement the Watcher interface: used to listen for ZooKeeper events
    private static final Logger logger = LoggerFactory.getLogger(ZooKeeperService.class);

    @Value("${zkConnectString}")                           // reference:https://www.baeldung.com/spring-value-defaults
    // Read the value of zkConnectString from application.properties
    private String zkConnectString;

    @Value("${zkNamespace:}")
    // ${zkNamespace:} The value after the colon is an empty string.
    private String zkNamespace;

    @Value("${myDescription}")
    // My node description
    private String myDescription;

    @Value("${zookeeper.session.timeout:5000}")              // reference: https://www.geeksforgeeks.org/devops/sessions-and-lifecycle-in-zookeeper/
    // Session timeout, default 5000 milliseconds (5 seconds)
    // If no heartbeat occurs within this time, ZooKeeper considers the client to be down.
    private int sessionTimeout;

    @Value("${zookeeper.connection.timeout:5000}")
    // Connection timeout, default 5000 milliseconds
    private int connectionTimeout;

    private String PEERS_PATH;   // Store the paths of all nodes
    private String LEADER_PATH;  // Storage Leader Path

    private ZooKeeper zooKeeper; // ZooKeeper client object, used to communicate with the ZooKeeper server.
    private String myId;         // my ID
    private String currentLeader;  // Current leader's ID
    private List<String> peers = Collections.emptyList();   // list of all nodes
    private LeaderStatus leaderStatus = LeaderStatus.WATCHING;    // My initial status was WATCHING
    private ZooKeeperStatus zkStatus = ZooKeeperStatus.DISCONNECTED;    // Connection status, initially disconnected

    // IMPORTANT FIX! Changed to true so nodes automatically compete for leadership
    // Was false before which caused no one to compete = no leader elected
    private boolean wantsToLead = true;  // Do I want to be leader?

    // Used to wait for a successful connection 
    // The initial value is 1. Calling await() will block. After calling countDown(), the value becomes 0, and await() will unblock.
    private final CountDownLatch connectedSignal = new CountDownLatch(1);

    @PostConstruct                           // reference: https://docs.spring.io/spring-framework/reference/core/beans/annotation-config/postconstruct-and-predestroy-annotations.html
    // Create a new object → inject @Value → call the @PostConstruct method
    public void init() throws IOException, InterruptedException, KeeperException {
        // Initialization method: Connect to ZooKeeper
        // Build path
        // If a namespace exists: prefix = "/my-app"
        // If no namespace exists: prefix = ""
        String prefix = (zkNamespace != null && !zkNamespace.isEmpty()) ? "/" + zkNamespace : "";
        PEERS_PATH = prefix + "/peers";   // Store all nodes
        LEADER_PATH = prefix + "/leader"; // Store leader information

        //output log
        logger.info("Initializing ZooKeeper connection to: {}", zkConnectString);
        logger.info("Using namespace: {}", zkNamespace != null && !zkNamespace.isEmpty() ? zkNamespace : "(none)");
        logger.info("Peers path: {}, Leader path: {}", PEERS_PATH, LEADER_PATH);
        // Connect to ZooKeeper
        connect();
    }

    private void connect() throws IOException, InterruptedException, KeeperException {           // reference: https://www.baeldung.com/java-zookeeper
        // Create a ZooKeeper client
        zooKeeper = new ZooKeeper(zkConnectString, sessionTimeout, this);

        // Wait for connection
        connectedSignal.await();

        // Create root paths if they don't exist
        createPathIfNotExists(PEERS_PATH);
        // Ensure the peers path exists
        
        // Register myself as a node
        registerAsPeer();
        // Create a temporary sequential node under /peers

        // Update node list
        updatePeersList();
        // Get all nodes and set up a listener

        // Watch leader
        watchLeader();
        // Check if there is a leader; if not, try to become a leader.
    }

    private void createPathIfNotExists(String path) throws KeeperException, InterruptedException {    // reference:https://ishan-aggarwal.medium.com/leader-election-distributed-systems-c026cf5afb86
        // Create the path (if it does not exist)

        // Check if the path exists
        Stat stat = zooKeeper.exists(path, false);             // reference:https://zookeeper.apache.org/doc/r3.4.6/api/org/apache/zookeeper/ZooKeeper.html
        // Parameters:
        // path: Path
        // false: Do not set a listener
        // Return value: Returns a Stat object if it exists, otherwise returns null.
        if (stat == null) {
            // The path does not exist, create it.
            try {                                                                // reference: https://zookeeper.apache.org/doc/r3.1.2/zookeeperTutorial.html
                zooKeeper.create(path,                     // Path 
                                 new byte[0],              // Data (empty)
                                 ZooDefs.Ids.OPEN_ACL_UNSAFE,  // Permissions (Fully Open)
                                 CreateMode.PERSISTENT      // Node type (persistent node)
                                );      
                // Will not be automatically deleted
                // Suitable for creating directories
                logger.info("Created path: {}", path);
            } catch (KeeperException.NodeExistsException e) {                    // reference: https://zookeeper.apache.org/doc/r3.1.2/zookeeperTutorial.html
                // Another node created it, that's fine
                logger.debug("Path already exists: {}", path);
            } catch (KeeperException.NoNodeException e) {
                // Parent path doesn't exist, create it first
                String parentPath = path.substring(0, path.lastIndexOf('/'));
                if (parentPath.length() > 0) {
                    // Recursively create parent paths
                    createPathIfNotExists(parentPath);
                    // Then create the current path
                    createPathIfNotExists(path);
                } else {
                    throw e;   // Root path error, throws exception
                }
            }
        }
    }

    private void registerAsPeer() throws KeeperException, InterruptedException {              // reference: https://zookeeper.apache.org/doc/r3.1.2/zookeeperTutorial.html
        // Register as a node in the cluster
        String peerPath = zooKeeper.create(
                PEERS_PATH + "/peer-",            // path prefix
                myDescription.getBytes(StandardCharsets.UTF_8),    // Data: Node Description
                ZooDefs.Ids.OPEN_ACL_UNSAFE,       // Permissions
                CreateMode.EPHEMERAL_SEQUENTIAL    // Key: Temporary Sequential Nodes
        );
        // CreateMode.EPHEMERAL_SEQUENTIAL: Temporary Sequential Node
        // EPHEMERAL: Automatically deleted when the client disconnects
        // SEQUENTIAL: ZooKeeper automatically adds an incrementing sequence number

        // Extract the peer ID from the full path
        myId = peerPath.substring(PEERS_PATH.length() + 1);
        logger.info("Registered as peer: {}", myId);
    }

    private void updatePeersList() {                      // reference: https://bikas-katwal.medium.com/zookeeper-introduction-designing-a-distributed-system-using-zookeeper-and-java-7f1b108e236e
        // Update node list
        try {
            // Get all child nodes under peers
            peers = zooKeeper.getChildren(PEERS_PATH, this);           // reference: https://zookeeper.apache.org/doc/r3.4.8/api/org/apache/zookeeper/ZooKeeper.html
            // PEERS_PATH: Parent path
            // this: Sets the listener (to notify me when there are changes)

            // sort
            Collections.sort(peers);
            // Sorting makes it easier to view
            
            logger.info("Updated peers list: {}", peers);
        } catch (KeeperException | InterruptedException e) {
            // An error occurred, log it.
            logger.error("Error updating peers list", e);
        }
    }
                                            // reference: https://codemia.io/knowledge-hub/path/how_to_re-register_zookeeper_watches
    private void watchLeader() {
        // Listen to the leader node
        try {
            // Check if the leader node exists
            Stat stat = zooKeeper.exists(LEADER_PATH, this);
            // Parameters:
            // LEADER_PATH: Path
            // this: Sets the listener (notifies me when a node changes)
            // Return value: Returns Stat if it exists, returns null if it does not exist
            if (stat != null) {
                // Case 1: Leader Exists 

                // Read the leader's ID
                byte[] data = zooKeeper.getData(LEADER_PATH, this, null);
                // Parameters:
                // LEADER_PATH: Path
                // this: Sets the listener
                // null: No Stat information needed
                
                currentLeader = new String(data, StandardCharsets.UTF_8);
                // Convert byte array to string
                logger.info("Current leader is: {}", currentLeader);
                // Update my status

                if (currentLeader.equals(myId)) {
                    // I am the leader!
                    leaderStatus = LeaderStatus.LEADING;
                } else if (wantsToLead) {
                    // I wanted to be a leader, but someone else already was.
                    leaderStatus = LeaderStatus.WAITING;
                } else {
                    // I don't want to be a leader, I just want to observe.
                    leaderStatus = LeaderStatus.WATCHING;
                }
            } else {
                // Case 2: No Leader
                currentLeader = null;
                logger.info("No current leader");

                if (wantsToLead) {
                    // I want to be a leader
                    tryToBecomeLeader();
                } else {
                    // I don't want to be one, I'll continue to observe.
                    leaderStatus = LeaderStatus.WATCHING;
                }
            }
        } catch (KeeperException | InterruptedException e) {
            logger.error("Error watching leader", e);
        }
    }

    private void tryToBecomeLeader() {                    // reference: https://zookeeper.apache.org/doc/r3.1.2/zookeeperTutorial.html
        // Try to become a leader
        // Principle: Whoever creates the /leader node first becomes the leader.
        try {
            // Attempt to create the /leader node (ephemeral node)
            zooKeeper.create(
                    LEADER_PATH,   // Path: leader
                    myId.getBytes(StandardCharsets.UTF_8),  // Data: My ID
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,        //Permissions
                    CreateMode.EPHEMERAL    // Temporary node, automatically deleted when the leader disconnects.
            );
            // Automatically delete when client disconnects
            // Enables automatic failover

            // Creation successful! I am now the leader! 
            currentLeader = myId;
            leaderStatus = LeaderStatus.LEADING;
            logger.info("Successfully became leader!");

        } catch (KeeperException.NodeExistsException e) {
            // Someone else became leader first (The node already exists, indicating that another node has become the leader.)
            logger.info("Failed to become leader, node already exists");
            leaderStatus = LeaderStatus.WAITING;  // Entering waiting state
            watchLeader();  // Continue listening to the leader
        } catch (KeeperException | InterruptedException e) {
            // other errors
            logger.error("Error trying to become leader", e);
            leaderStatus = LeaderStatus.WAITING; 
        }
    }

    public void startLeading() {
        // Start running for leader
        wantsToLead = true;  // Set flag: I want to be a leader
        if (zkStatus == ZooKeeperStatus.CONNECTED) {
            // Can only run for election in a connected state
            watchLeader();  // Check the leader status immediately
        }
        // If no connection is established, the flag has already been set.
        // Upon successful connection, an election attempt will be automatically retried.
    }

    public void stopLeading() {
        // Stop campaigning and enter observation mode
        wantsToLead = false;  // clear flag

        // If we're currently the leader, give up leadership
        if (leaderStatus == LeaderStatus.LEADING) {             // reference: https://www.php.net/manual/en/zookeeper.delete.php
            try {
                zooKeeper.delete(LEADER_PATH, -1);
                // Delete the /leader node
                // Parameters:
                // LEADER_PATH: Path
                // -1: Version number
                logger.info("Gave up leadership");
            } catch (KeeperException | InterruptedException e) {
                logger.error("Error giving up leadership", e);
            }
        }

        leaderStatus = LeaderStatus.WATCHING;  // Change to observation mode
        currentLeader = null;
        watchLeader();  // Continue listening (although not participating in the election)
    }

    @Override                                                               // reference:https://ishan-aggarwal.medium.com/leader-election-distributed-systems-c026cf5afb86
    public void process(WatchedEvent event) {
        // This callback is triggered by all events in ZooKeeper.
        logger.info("Received event: {}", event);
        // Output event information for easier debugging

        if (event.getType() == Event.EventType.None) {
            // Connection state changed
            switch (event.getState()) {
                case SyncConnected:
                    // Connection successful!
                    zkStatus = ZooKeeperStatus.CONNECTED;
                    connectedSignal.countDown();    // Wake up threads waiting for connections
                    logger.info("Connected to ZooKeeper");
                    break;
                case Disconnected:
                    // Disconnect
                    // The session is still running, and the ephemeral node will not be deleted.
                    zkStatus = ZooKeeperStatus.DISCONNECTED;
                    logger.warn("Disconnected from ZooKeeper");
                    break;
                case Expired:
                    // Session expired
                    // All ephemeral nodes have been deleted
                    zkStatus = ZooKeeperStatus.DISCONNECTED;
                    logger.error("Session expired");
                    System.exit(2); // exit the service   // Important fix! change expired case to exit instead of reconnect
                    break;
            }
        } else {
            // Get the changed node path
            String path = event.getPath();

            if (path != null) {
                if (path.equals(PEERS_PATH)) {
                    // The child nodes of the peers path have changed
                    updatePeersList();   // Re-fetch the node list
                } else if (path.equals(LEADER_PATH)) {
                    // The leader node has changed
                    watchLeader();    // Recheck leader status
                    // If there is no leader, attempt to elect one.
                }
            }
        }
    }

    private void reconnect() throws IOException, InterruptedException, KeeperException {
        // Reconnect to ZooKeeper
        // Called when session expires
        if (zooKeeper != null) {
            zooKeeper.close();  //Close old connection
        }
        connect();   // Create a new connection
        // connect() will be re-executed:
        // Create a ZooKeeper object
        // Wait for the connection to succeed
        // Create the /peers path
        // Register as a new node (get a new ID!)
        // Update the node list
        // Listen for the leader
        // If wantToLead=true, attempt to run for leader.
    }

    @PreDestroy                   // reference:https://www.geeksforgeeks.org/java/bean-life-cycle-in-java-spring/ https://www.geeksforgeeks.org/devops/sessions-and-lifecycle-in-zookeeper/
    // Execution timing: When the application is closed
    public void cleanup() {
        // Clean up resources
        try {
            if (zooKeeper != null) {
                zooKeeper.close();  // close connection
                logger.info("ZK connection closed");
                // After closing the connection:
                // The session ends.
                // All ephemeral nodes are automatically deleted
                // If I am the leader, /leader is deleted.
                // Other nodes are notified and a new election is held.
            }
        } catch (InterruptedException e) {
            logger.error("Error closing ZooKeeper connection", e);
        }
    }

    // Getters
    public LeaderStatus getLeaderStatus() {
        return leaderStatus;  // my status
    }

    public ZooKeeperStatus getZkStatus() {
        return zkStatus;      // connection status
    }

    public String getCurrentLeader() {
        return currentLeader;      // Current leader's ID
    }

    public String getMyId() {
        return myId;        // my ID
    }

    public String getMyDescription() {
        return myDescription;  // my description
    }

    public List<String> getPeers() { 
        return peers;     // list of all nodes
    }
}
