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
    private static final Logger logger = LoggerFactory.getLogger(ZooKeeperService.class);

    @Value("${zkConnectString}")
    private String zkConnectString;

    @Value("${zkNamespace:}")
    private String zkNamespace;

    @Value("${myDescription}")
    private String myDescription;

    @Value("${zookeeper.session.timeout:5000}")
    private int sessionTimeout;

    @Value("${zookeeper.connection.timeout:5000}")
    private int connectionTimeout;

    private String PEERS_PATH;
    private String LEADER_PATH;

    private ZooKeeper zooKeeper;
    private String myId;
    private String currentLeader;
    private List<String> peers = Collections.emptyList();
    private LeaderStatus leaderStatus = LeaderStatus.WATCHING;
    private ZooKeeperStatus zkStatus = ZooKeeperStatus.DISCONNECTED;
    private boolean wantsToLead = false;

    private final CountDownLatch connectedSignal = new CountDownLatch(1);

    @PostConstruct
    public void init() throws IOException, InterruptedException, KeeperException {
        // Initialize paths with namespace
        String prefix = (zkNamespace != null && !zkNamespace.isEmpty()) ? "/" + zkNamespace : "";
        PEERS_PATH = prefix + "/peers";
        LEADER_PATH = prefix + "/leader";

        logger.info("Initializing ZooKeeper connection to: {}", zkConnectString);
        logger.info("Using namespace: {}", zkNamespace != null && !zkNamespace.isEmpty() ? zkNamespace : "(none)");
        logger.info("Peers path: {}, Leader path: {}", PEERS_PATH, LEADER_PATH);
        connect();
    }

    private void connect() throws IOException, InterruptedException, KeeperException {
        zooKeeper = new ZooKeeper(zkConnectString, sessionTimeout, this);

        // Wait for connection
        connectedSignal.await();

        // Create root paths if they don't exist
        createPathIfNotExists(PEERS_PATH);

        // Register this server as a peer
        registerAsPeer();

        // Start watching peers
        updatePeersList();

        // Watch leader
        watchLeader();
    }

    private void createPathIfNotExists(String path) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            try {
                zooKeeper.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                logger.info("Created path: {}", path);
            } catch (KeeperException.NodeExistsException e) {
                // Another node created it, that's fine
                logger.debug("Path already exists: {}", path);
            } catch (KeeperException.NoNodeException e) {
                // Parent path doesn't exist, create it first
                String parentPath = path.substring(0, path.lastIndexOf('/'));
                if (parentPath.length() > 0) {
                    createPathIfNotExists(parentPath);
                    // Now try creating this path again
                    createPathIfNotExists(path);
                } else {
                    throw e;
                }
            }
        }
    }

    private void registerAsPeer() throws KeeperException, InterruptedException {
        String peerPath = zooKeeper.create(
                PEERS_PATH + "/peer-",
                myDescription.getBytes(StandardCharsets.UTF_8),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL
        );

        // Extract the peer ID from the full path
        myId = peerPath.substring(PEERS_PATH.length() + 1);
        logger.info("Registered as peer: {}", myId);
    }

    private void updatePeersList() {
        try {
            peers = zooKeeper.getChildren(PEERS_PATH, this);
            Collections.sort(peers);
            logger.info("Updated peers list: {}", peers);
        } catch (KeeperException | InterruptedException e) {
            logger.error("Error updating peers list", e);
        }
    }

    private void watchLeader() {
        try {
            Stat stat = zooKeeper.exists(LEADER_PATH, this);
            if (stat != null) {
                // Leader exists, get the leader's ID
                byte[] data = zooKeeper.getData(LEADER_PATH, this, null);
                currentLeader = new String(data, StandardCharsets.UTF_8);
                logger.info("Current leader is: {}", currentLeader);

                if (currentLeader.equals(myId)) {
                    leaderStatus = LeaderStatus.LEADING;
                } else if (wantsToLead) {
                    leaderStatus = LeaderStatus.WAITING;
                } else {
                    leaderStatus = LeaderStatus.WATCHING;
                }
            } else {
                // No leader exists
                currentLeader = null;
                logger.info("No current leader");

                if (wantsToLead) {
                    tryToBecomeLeader();
                } else {
                    leaderStatus = LeaderStatus.WATCHING;
                }
            }
        } catch (KeeperException | InterruptedException e) {
            logger.error("Error watching leader", e);
        }
    }

    private void tryToBecomeLeader() {
        try {
            zooKeeper.create(
                    LEADER_PATH,
                    myId.getBytes(StandardCharsets.UTF_8),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL
            );

            currentLeader = myId;
            leaderStatus = LeaderStatus.LEADING;
            logger.info("Successfully became leader!");

        } catch (KeeperException.NodeExistsException e) {
            // Someone else became leader first
            logger.info("Failed to become leader, node already exists");
            leaderStatus = LeaderStatus.WAITING;
            watchLeader();
        } catch (KeeperException | InterruptedException e) {
            logger.error("Error trying to become leader", e);
            leaderStatus = LeaderStatus.WAITING;
        }
    }

    public void startLeading() {
        wantsToLead = true;
        if (zkStatus == ZooKeeperStatus.CONNECTED) {
            watchLeader();
        }
    }

    public void stopLeading() {
        wantsToLead = false;

        // If we're currently the leader, give up leadership
        if (leaderStatus == LeaderStatus.LEADING) {
            try {
                zooKeeper.delete(LEADER_PATH, -1);
                logger.info("Gave up leadership");
            } catch (KeeperException | InterruptedException e) {
                logger.error("Error giving up leadership", e);
            }
        }

        leaderStatus = LeaderStatus.WATCHING;
        currentLeader = null;
        watchLeader();
    }

    @Override
    public void process(WatchedEvent event) {
        logger.info("Received event: {}", event);

        if (event.getType() == Event.EventType.None) {
            // Connection state changed
            switch (event.getState()) {
                case SyncConnected:
                    zkStatus = ZooKeeperStatus.CONNECTED;
                    connectedSignal.countDown();
                    logger.info("Connected to ZooKeeper");
                    break;
                case Disconnected:
                    zkStatus = ZooKeeperStatus.DISCONNECTED;
                    logger.warn("Disconnected from ZooKeeper");
                    break;
                case Expired:
                    zkStatus = ZooKeeperStatus.DISCONNECTED;
                    logger.error("Session expired, need to reconnect");
                    try {
                        reconnect();
                    } catch (Exception e) {
                        logger.error("Failed to reconnect", e);
                    }
                    break;
            }
        } else {
            // Data changed
            String path = event.getPath();

            if (path != null) {
                if (path.equals(PEERS_PATH)) {
                    // Peers list changed
                    updatePeersList();
                } else if (path.equals(LEADER_PATH)) {
                    // Leader changed
                    watchLeader();
                }
            }
        }
    }

    private void reconnect() throws IOException, InterruptedException, KeeperException {
        if (zooKeeper != null) {
            zooKeeper.close();
        }
        connect();
    }

    @PreDestroy
    public void cleanup() {
        try {
            if (zooKeeper != null) {
                zooKeeper.close();
            }
        } catch (InterruptedException e) {
            logger.error("Error closing ZooKeeper connection", e);
        }
    }

    // Getters
    public LeaderStatus getLeaderStatus() {
        return leaderStatus;
    }

    public ZooKeeperStatus getZkStatus() {
        return zkStatus;
    }

    public String getCurrentLeader() {
        return currentLeader;
    }

    public String getMyId() {
        return myId;
    }

    public String getMyDescription() {
        return myDescription;
    }

    public List<String> getPeers() {
        return peers;
    }
}