package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by yifan on 4/9/17.
 */

public class Message implements Serializable {
    String nodeId;
    String predId;
    String succId;
    int type;
    String port;
    String predPort;
    String succPort;
    Map<String, String> map = new HashMap<String, String>();
    public Message(String port, String nodeId, String predPort, String succPort, String predId, String succId, int type, Map<String, String> map) {
        this.nodeId = nodeId;
        this.predId = predId;
        this.succId = succId;
        this.type = type;
        this.port = port;
        this.predPort = predPort;
        this.succPort = succPort;
        this.map = map;
    }
}
