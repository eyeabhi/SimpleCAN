
import java.io.Serializable;
import java.util.*;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Abhishek
 */
public class CANMessage implements Serializable {
    String fromIP;
    int fromPort;
    String fromNodeId;
    String toIP;
    int toPort;
    
    String messageType;
    String messageDirection;
    String text;
    Object [] content;
    ArrayList requestHopsIP;
    ArrayList requestHopsNames;
    ArrayList replyHopsIP;
    ArrayList replyHopsNames;
    
    public CANMessage() {
        requestHopsIP = new ArrayList<String>();
        requestHopsNames = new ArrayList<String>();
        replyHopsIP = new ArrayList<String>();
        replyHopsNames = new ArrayList<String>();
    }
    
    public CANMessage(String msg) {
        requestHopsIP = new ArrayList<String>();
        requestHopsNames = new ArrayList<String>();
        replyHopsIP = new ArrayList<String>();
        replyHopsNames = new ArrayList<String>();
        messageType = "TEXT";
        text = msg;
    }
}
