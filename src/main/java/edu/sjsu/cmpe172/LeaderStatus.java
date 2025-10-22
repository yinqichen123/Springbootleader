package edu.sjsu.cmpe172;

public enum LeaderStatus {
    WATCHING,   // Not interested in being leader
    WAITING,    // Want to be leader but someone else is
    LEADING     // Currently the leader
}