package edu.sjsu.cmpe172;

public enum LeaderStatus {            // reference:https://www.baeldung.com/java-enum-simple-state-machine
    WATCHING,   // Not interested in being leader
    WAITING,    // Want to be leader but someone else is
    LEADING     // Currently the leader
}
