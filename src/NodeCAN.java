/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
/**
 *
 * @author Abhishek
 */
public class NodeCAN implements Serializable{
    
    public static final boolean DEBUGMODE = false;
    
    public static final int INTERVAL = 60;
    
    private ServerSocket listener;
        
    private NodeInfo nodeInfo;
    
    private ConcurrentHashMap<String, Date> lastHeard;
    
    private boolean joinSent = false;
    
    private boolean joined = false;
    
    private FailureTimer timer = null;
    
    private String bootstrap_IP;
    private int bootstrap_port;
    private ListenToUDP udpHandler;
       
    public static void unitTesting() {
        
        
        
        
        
        System.exit(0);
        Zone z1 = new Zone("ksfks", 1234, 7.5, 1.25, 5.0, 7.5);
        Zone z2 = new Zone("ksfks", 1234, 7.5, 1.25, 7.5, 10.0);
        
        System.out.println(z2.isInsideZone(5, 9.1));
        
        Zone[] splitted = z1.splitZone();        
        System.out.println("Splitted as " + splitted[0].x1 + " " + splitted[0].x2 + " " + splitted[0].y1 + " " + splitted[0].y2);
        System.out.println("and " + splitted[1].x1 + " " + splitted[1].x2 + " " + splitted[1].y1 + " " + splitted[1].y2);
        
        splitted = splitted[0].splitZone();        
        System.out.println("Splitted as " + splitted[0].x1 + " " + splitted[0].x2 + " " + splitted[0].y1 + " " + splitted[0].y2);
        System.out.println("and " + splitted[1].x1 + " " + splitted[1].x2 + " " + splitted[1].y1 + " " + splitted[1].y2);
        
        splitted = splitted[0].splitZone();        
        System.out.println("Splitted as " + splitted[0].x1 + " " + splitted[0].x2 + " " + splitted[0].y1 + " " + splitted[0].y2);
        System.out.println("and " + splitted[1].x1 + " " + splitted[1].x2 + " " + splitted[1].y1 + " " + splitted[1].y2);
        
        splitted = splitted[0].splitZone();        
        System.out.println("Splitted as " + splitted[0].x1 + " " + splitted[0].x2 + " " + splitted[0].y1 + " " + splitted[0].y2);
        System.out.println("and " + splitted[1].x1 + " " + splitted[1].x2 + " " + splitted[1].y1 + " " + splitted[1].y2);
        
        splitted = splitted[0].splitZone();        
        System.out.println("Splitted as " + splitted[0].x1 + " " + splitted[0].x2 + " " + splitted[0].y1 + " " + splitted[0].y2);
        System.out.println("and " + splitted[1].x1 + " " + splitted[1].x2 + " " + splitted[1].y1 + " " + splitted[1].y2);
        
        System.out.println(z1.isMergeable(z2));     
        //Zone merged = Zone.mergeZones(z1, z2);
        //System.out.println("Merged zone: " + merged.x1 + " " + merged.x2 + " " + merged.y1 + " " + merged.y2);
        
        //System.out.println("Distance is: " + z1.getDistance(2,2));
        System.exit(1);
    }
    
    public NodeCAN(String name, int port) {
        lastHeard = new ConcurrentHashMap<String, Date>();
        nodeInfo = new NodeInfo();
        nodeInfo.nodeId = name;
        try {
            listener = new ServerSocket(port);            
        } catch (Exception ex) {
            printError(ex, "");
        }
        
        this.nodeInfo.port = port;
        try {
        InetAddress addr = InetAddress.getLocalHost();
        nodeInfo.IP_address = addr.getHostAddress();
        } catch (Exception excp) {printError(excp, nodeInfo.IP_address);}
        
        System.out.println("CAN node: '" + nodeInfo.nodeId + "' is running at IP: " + nodeInfo.IP_address
                + " and port: " + this.nodeInfo.port);
        
        nodeInfo.zones = new ArrayList();
        nodeInfo.zones.add(new Zone(nodeInfo.IP_address, this.nodeInfo.port));
        joined = true;
        bootstrap_IP = nodeInfo.IP_address;
        bootstrap_port = nodeInfo.port;
        viewPeer();
        _listen();        
    }
    
    public NodeCAN(String name, int port, String bootstrap_IP, int bootstrap_port) {
        this.bootstrap_IP = bootstrap_IP;
        this.bootstrap_port = bootstrap_port;
        
        lastHeard = new ConcurrentHashMap<String, Date>();
        nodeInfo = new NodeInfo();
        nodeInfo.nodeId = name;
        try {
            listener = new ServerSocket(port);            
        } catch (Exception ex) {
            printError(ex, "");
        }
        
        this.nodeInfo.port = port;
        try {
            InetAddress addr = InetAddress.getLocalHost();
            nodeInfo.IP_address = addr.getHostAddress();
        } catch (Exception excp) {
            printError(excp, nodeInfo.IP_address);
        }
        
        System.out.println("CAN node: '" + nodeInfo.nodeId + "' is running at IP: " + nodeInfo.IP_address
                + " and port: " + this.nodeInfo.port);
        _listen();
    }
    
    private void joinCAN() {
        if(joined || joinSent) {
            return;
        }
        Point point = new Point(new Random().nextDouble() * 10, new Random().nextDouble() * 10);
        CANMessage m = new CANMessage();
        m.fromIP = nodeInfo.IP_address;
        m.fromPort = nodeInfo.port;
        m.fromNodeId = nodeInfo.nodeId;
        m.messageDirection = "Request";
        m.messageType = "JOIN";
        m.content = new Object[1];
        m.content[0] = point;
        
        Socket socket = null;
        try {
            socket = new Socket(bootstrap_IP, bootstrap_port);
            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
            oos.writeObject(m);
            oos.flush();
            joinSent = true;
            System.out.println("JOIN request sent at point: " + point.x + "  " + point.y + 
                    "  waiting for reply...");
            oos.close();
            socket.close();
            timer = new FailureTimer("Failure to JOIN.");
                    new Thread(timer).start();
        } catch (Exception ex) {
            printError(ex, "Could not connect to the bootstrap server.");
            System.exit(1);
        }
    }
    
    private byte[] readFromFile(String filename) {
        byte[] retVal = null;
        
        File file = new File(nodeInfo.nodeId + File.separator + filename);
        if(file.exists()) {
            try {                
                FileInputStream fin = new FileInputStream(file);                
                byte fileContent[] = new byte[(int) file.length()];                
                fin.read(fileContent);
                retVal = fileContent;
            } catch (FileNotFoundException e) {
                System.out.println("File not found" + e);
            } catch (IOException ioe) {
                System.out.println("Exception while reading the file " + ioe);
            }
        }
        else {
            System.out.println("File not found. Using dummy data.");
            return "My name is Abhishek Roy".getBytes();
        }
        return retVal;
    }
    
    private boolean writeToFile(String filename, byte[] data) {

        File dir = new File(nodeInfo.nodeId);
        File file = new File(nodeInfo.nodeId + File.separator + filename);
        
        try {
            dir.mkdir();
            file.createNewFile();
            FileOutputStream fos = new FileOutputStream(file);     
            fos.write(data);
            fos.close();
            return true;
        } catch (FileNotFoundException ex) {
            printError(ex, "");
        } catch (IOException ioe) {
            printError(ioe, "");
        }
        return false;
    }
    
    private void _listen() {

        try {
            Thread t1 = new Thread(new ListenFromKeyboard());
            t1.start();
        } catch (Exception excp) {
            printError(excp, "");
        }
        
        try {
            Thread t2 = new Thread(new GeneratePeriodicUpdate());
            t2.start();
        } catch (Exception excp) {
            printError(excp, "");
        }
        
        udpHandler = new ListenToUDP();
        try {
            Thread t3 = new Thread(udpHandler);
            t3.start();
        } catch (Exception excp) {
            printError(excp, "");
        }
        
        while (true) {
            Socket s = null;
            try {
                s = listener.accept();                
            } 
            catch (java.net.SocketException ex) {printError(ex, "Bye.");}
            catch (Exception excp) {printError(excp, "");}
            try {
                Thread t = new Thread(new ProcessIncomingRequest(s));
                t.start();
            } catch (Exception excp) {printError(excp, "");}
        }
    }
    
    private void viewPeer() {
        System.out.println();
        System.out.print("********************** THIS PEER ***********************");
        viewPeer(nodeInfo);
        
    }
    
    private void viewPeer(NodeInfo n) {
        System.out.println();
        System.out.println("********************************************************");
        System.out.println("Node identifier: " + n.nodeId);
        System.out.println("IP address: " + n.IP_address + "    Port: " + n.port);
        for (int i = 0; i < n.zones.size(); i++) {
            System.out.println("Zone " + (i + 1) + ":    "
                    + n.zones.get(i).x1 + "   " + n.zones.get(i).x2 + "   " + n.zones.get(i).y1
                    + "   " + n.zones.get(i).y2);            
        }
        System.out.print("Neighbors:  ");
        Iterator it = n.neighbors.keySet().iterator();
        while(it.hasNext()) {
            String x = (String)it.next();
            System.out.print(x + "  ");
        }
        System.out.println();
        
        for (int i = 0; i < n.zones.size(); i++) {
            Zone z = n.zones.get(i);
            System.out.print("Zone " + (i + 1) + " contains: ");
            Iterator iterator = z.dhtData.keySet().iterator();
            while(iterator.hasNext()) {
                String keyword = (String)iterator.next();
                System.out.print(keyword + "  ");
            }
            System.out.println();
        }
        
        
        System.out.println("--------------------------------------------------------");
        System.out.print("Enter command: ");
    }
    
    public void processKeyboardInput(String in) {        
        
        StringTokenizer st = new StringTokenizer(in);
        String cmd = st.nextToken();
        
        if(in.equalsIgnoreCase("QUIT") || in.equalsIgnoreCase("LEAVE")) {
            
            CANMessage m = new CANMessage();
            m.fromIP = nodeInfo.IP_address;
            m.fromNodeId = nodeInfo.nodeId;
            m.fromPort = nodeInfo.port;
            m.messageDirection = "Request";
            m.messageType = "IQUIT";            
            
            for(int i = 0; i < nodeInfo.zones.size(); i++) {
                
                double smallestArea = Double.MAX_VALUE;
                String toIP = null;
                int toPort = 0;
                String toOwner = null;
                
                Zone z = nodeInfo.zones.get(i);
                m.content = null;
                m.content = new Object[1];
                m.content[0] = z;
                for(int j = 0; j < z.neighbors.size(); j++) {
                    Zone neighbor = (Zone)z.neighbors.get(j);
                    if(z.isMergeable(neighbor) && !neighbor.ownerId.equals(nodeInfo.nodeId)) {
                        if(neighbor.area() < smallestArea) {
                            smallestArea = neighbor.area();
                            toIP = neighbor.ownerIP;
                            toPort = neighbor.ownerPort;
                            toOwner = neighbor.ownerId;
                        }
                    }
                }
                
                if (toPort == 0) { // no mergeable neighbor
                    smallestArea = Double.MAX_VALUE;
                    for (int j = 0; j < z.neighbors.size(); j++) {
                        Zone neighbor = (Zone) z.neighbors.get(j);

                        if (neighbor.area() < smallestArea && !neighbor.ownerId.equals(nodeInfo.nodeId)) {
                            smallestArea = neighbor.area();
                            toIP = neighbor.ownerIP;
                            toPort = neighbor.ownerPort;
                            toOwner = neighbor.ownerId;
                        }
                    }
                }
                
                m.toIP = toIP;
                m.toPort = toPort;
                sendMessage(toIP, toPort, m);
                System.out.println("Zone " + (i + 1) + " sent to " + toOwner);
            }            
            nodeInfo.zones.clear();
            nodeInfo.neighbors.clear();            
            System.out.println("Node is shutting down...");
            try {
                //listener.close();
                //Thread.sleep(500);
            } catch (Exception ex) {printError(ex, "");}
            System.out.println("Shutdown OK.");
            joined = false;
            joinSent = false;
            //System.exit(0);
        }
        
        /*else if (cmd.equalsIgnoreCase("Say")) {
            try {
                String IP = st.nextToken();
                int port = Integer.parseInt(st.nextToken());
                String msg = "";
                while (st.hasMoreTokens()) {
                    msg = msg + " " + st.nextToken();
                }
                sendMessage(IP, port, new CANMessage(msg));
                System.out.println("Message sent.");
            } catch (Exception ex) {
                printError(ex, "Message not sent.");
                System.out.println("Message not sent.");
            }
            
        } */
        else if (cmd.equalsIgnoreCase("View")) {
            if(st.hasMoreTokens()) {
                String peer = st.nextToken();
                if(peer.equals(nodeInfo.nodeId)) {
                    viewPeer();
                }
                else {
                    sendViewRequest(peer);
                }
            }
            else {
                broadcastView();
            }
            
        }
        else if (cmd.equalsIgnoreCase("Zone")) {
            try {
                for(int i = 0; i < nodeInfo.zones.size(); i++) {
                    System.out.println("Zone " + (i+1) + ":   "
                        + nodeInfo.zones.get(i).x1 + "  " + nodeInfo.zones.get(i).x2 + "  " + nodeInfo.zones.get(i).y1 
                             + "  " + nodeInfo.zones.get(i).y2);
                }

            } catch (Exception ex) {
                printError(ex, "Message not sent.");
                System.out.println("Message not sent.");
            }
            
        }
        else if (cmd.equalsIgnoreCase("Neighbors")) {
            try {
                
                Iterator it = nodeInfo.neighbors.keySet().iterator();
                while (it.hasNext()) {
                    String peer = (String)it.next();
                    NodeInfo ni = nodeInfo.neighbors.get(peer);
                    System.out.println("Peer name:  " + peer);
                    for(int i = 0; i < ni.zones.size(); i++) {
                        System.out.println("Zone " + (i+1) + ":   "
                            + ni.zones.get(i).x1 + "  " + ni.zones.get(i).x2 + "  " + ni.zones.get(i).y1 
                             + "  " + ni.zones.get(i).y2);
                    }
                }                

            } catch (Exception ex) {
                printError(ex, "Message not sent.");
                System.out.println("Message not sent.");
            }
            
        }
        else if (cmd.equalsIgnoreCase("insert")) {
            if(st.hasMoreTokens()) {
                String keyword = st.nextToken();
                Point p = Zone.getMappingPoint(keyword);
                System.out.println(keyword + " is to be inserted at point: " + p.x + "  " + p.y);
                byte[] value = readFromFile(keyword);
                boolean inserted = false;
                for(int i = 0; i < nodeInfo.zones.size(); i++) {
                    Zone z = nodeInfo.zones.get(i);
                    if(z.isInsideZone(p)) {
                        inserted = true;
                        z.dhtData.put(keyword, value);
                        System.out.println("INSERT SUCCESS! It was inserted in this node.");
                        break;
                    }
                }
                
                if (!inserted) {
                    CANMessage m = new CANMessage();
                    m.fromIP = nodeInfo.IP_address;
                    m.fromPort = nodeInfo.port;
                    m.fromNodeId = nodeInfo.nodeId;
                    m.messageDirection = "Request";
                    m.messageType = "INSERT";
                    m.text = keyword;
                    m.content = new Object[2];
                    m.content[0] = p;
                    m.content[1] = value;
                    routeMessage(m);
                    
                    timer = new FailureTimer("INSERT Failure for " + keyword);
                    new Thread(timer).start();
                }
            }            
            else {
                System.out.println("Filename required.");
            }
        }
        else if (cmd.equalsIgnoreCase("search")) {
            if(st.hasMoreTokens()) {
                String keyword = st.nextToken();
                Point p = Zone.getMappingPoint(keyword);
                System.out.println(keyword + " is to be searched at point: " + p.x + "  " + p.y);
                boolean found = false;
                for(int i = 0; i < nodeInfo.zones.size(); i++) {
                    Zone z = nodeInfo.zones.get(i);
                    if(z.isInsideZone(p)) {
                        found = true;
                        byte[] value = z.dhtData.get(keyword);
                        if(value != null) {
                            writeToFile(keyword, value);
                            System.out.println("Keyword found at this node!");
                        }
                        else {
                            System.out.println("Search Failure.");
                        }
                        break;
                    }
                }
                
                if (!found) {
                    CANMessage m = new CANMessage();
                    m.fromIP = nodeInfo.IP_address;
                    m.fromPort = nodeInfo.port;
                    m.fromNodeId = nodeInfo.nodeId;
                    m.messageDirection = "Request";
                    m.messageType = "SEARCH";
                    m.text = keyword;
                    m.content = new Object[1];
                    m.content[0] = p;                    
                    routeMessage(m);
                    timer = new FailureTimer("SEARCH Failure for " + keyword);
                    new Thread(timer).start();
                }
            }
            else {
                System.out.println("Filename required.");
            }
        }
        else if (cmd.equalsIgnoreCase("join")) {
            if(st.hasMoreTokens()) {
                String peer = st.nextToken();
                if(peer.equalsIgnoreCase(nodeInfo.nodeId)) {
                    if(!joined) {
                        joinCAN();
                    }
                    else {
                        System.out.println("Node already joined.");
                    }
                }
                else {
                    sendJoinRequest(peer);
                }
            }
            else {
                broadcastJoin();
            }
            
        }
        else {
            System.out.println("Command not recognized.");
        }
    }
    
    public synchronized void processRequestFromNetwork(CANMessage m) {
        if(m.fromNodeId == null) {
            System.out.println("#####################################################");
        }
        try {
            lastHeard.put(m.fromNodeId, new Date());
        } catch (Exception ex) {
            printError(ex, "");
        }
        if(m.messageType.equals("TEXT")) {
            System.out.println();
            System.out.println("Incoming message: " + m.text);
            System.out.print("Enter command: ");
            System.out.flush();
        }
        else if(m.messageType.equals("JOIN")) {            
            if(m.messageDirection.equals("Request")) {
                Point p = (Point)m.content[0];
                for(int i = 0; i < nodeInfo.zones.size(); i++) {
                    if(nodeInfo.zones.get(i).isInsideZone(p.x, p.y)) {
                        String toIP = m.fromIP;
                        int toPort = m.fromPort;
                        
                        Zone zoneToSend = null;
                        if (nodeInfo.zones.size() > 1) {
                            zoneToSend = nodeInfo.zones.remove(i);
                            zoneToSend.ownerIP = toIP;
                            zoneToSend.ownerId = m.fromNodeId;
                            zoneToSend.ownerPort = toPort;
                        } else {
                            Zone[] splitted = nodeInfo.zones.get(i).splitZone();
                            splitted[0].ownerIP = nodeInfo.IP_address;
                            splitted[0].ownerPort = nodeInfo.port;
                            splitted[0].ownerId = nodeInfo.nodeId;
                            splitted[1].ownerIP = toIP;
                            splitted[1].ownerPort = toPort;
                            splitted[1].ownerId = m.fromNodeId;
                            splitted[0].neighbors.add(splitted[1]);
                            splitted[1].neighbors.add(splitted[0]);
                            nodeInfo.zones.remove(i);
                            nodeInfo.zones.add(i, splitted[0]);
                            zoneToSend = splitted[1];
                        }
                        
                        ConcurrentHashMap<String, NodeInfo> removeList = nodeInfo.updateNodeInfoNeighbors(null);
                        sendRemoveNeighbor(removeList, null);
                        System.out.println("####### " + m.fromNodeId + " requested to join.");
                        
                        m.messageDirection = "Reply";
                        m.content = new Object[2];
                        m.content[0] = p;
                        m.content[1] = zoneToSend;
                        m.requestHopsIP.add(nodeInfo.IP_address);
                        m.requestHopsNames.add(nodeInfo.nodeId);                        
                        m.fromIP = nodeInfo.IP_address;
                        m.fromPort = nodeInfo.port;
                        m.fromNodeId = nodeInfo.nodeId;
                        sendMessage(toIP, toPort, m);
                        
                        sendRefresh(zoneToSend.ownerId);
                        viewPeer();
                        return;                    
                    }
                }
                
                // route it
                routeMessage(m);
            }
            else if(m.messageDirection.equals("Reply")) {
                if(joinSent) {
                    timer.cancel();
                    joinSent = false;
                    joined = true;
                    nodeInfo.zones.add((Zone) m.content[1]);                        // problem
                    nodeInfo.updateNodeInfoNeighbors(null);
                    System.out.println("Join successful! - " + m.fromNodeId);
                    System.out.println();
                    sendRefresh(null);
                    
                    //System.out.println("JOIN request succeeded. Alotted zone: "
                    //    + nodeInfo.zones.get(0).x1 + "  " + nodeInfo.zones.get(0).x2 + "  " + nodeInfo.zones.get(0).y1 
                    //         + "  " + nodeInfo.zones.get(0).y2);
                    
                    viewPeer();                    
                }
            }
        }
        else if(m.messageType.equals("INSERT")) {            
            if(m.messageDirection.equals("Request")) {
                Point p = (Point)m.content[0];
                boolean inserted = false;
                for(int i = 0; i < nodeInfo.zones.size(); i++) {
                    Zone z = nodeInfo.zones.get(i);
                    if(z.isInsideZone(p)) {
                        inserted = true;
                        z.dhtData.put(m.text, (byte[])m.content[1]);
                        String toIP = m.fromIP;
                        int toPort = m.fromPort;
                        m.messageDirection = "Reply";                        
                        m.content[0] = p;
                        m.content[1] = nodeInfo.getMemoryFriendlyNodeInfo();
                        m.requestHopsIP.add(nodeInfo.IP_address);
                        m.requestHopsNames.add(nodeInfo.nodeId);                        
                        m.fromIP = nodeInfo.IP_address;
                        m.fromPort = nodeInfo.port;
                        m.fromNodeId = nodeInfo.nodeId;
                        sendMessage(toIP, toPort, m);
                        break;
                    }                
                }
                if(!inserted) {
                    routeMessage(m);
                }
            }
            else if(m.messageDirection.equals("Reply")) {
                timer.cancel();
                System.out.println("INSERT SUCCESS!");
                System.out.println(m.text + " was inserted at " + m.fromNodeId + " (" + m.fromIP + ":" + m.fromPort + ")");
                System.out.println("Route taken:");
                for(int i = 0; i < m.requestHopsNames.size(); i++) {
                    System.out.print(m.requestHopsNames.get(i) + " (" + m.requestHopsIP.get(i) + ")");
                    if(i + 1 < m.requestHopsNames.size()) {
                        System.out.print(" --> ");
                    }
                }
                System.out.println();
            }
        }
        
        else if(m.messageType.equals("SEARCH")) {            
            if(m.messageDirection.equals("Request")) {
                Point p = (Point)m.content[0];
                boolean found = false;
                for(int i = 0; i < nodeInfo.zones.size(); i++) {
                    Zone z = nodeInfo.zones.get(i);
                    if(z.isInsideZone(p)) {
                        found = true;
                        if (z.dhtData.containsKey(m.text)) {
                            byte[] value = z.dhtData.get(m.text);
                            String toIP = m.fromIP;
                            int toPort = m.fromPort;
                            m.messageDirection = "Reply";
                            m.content = new Object[3];
                            m.content[0] = p;
                            m.content[1] = nodeInfo.getMemoryFriendlyNodeInfo();
                            m.content[2] = value;
                            m.requestHopsIP.add(nodeInfo.IP_address);
                            m.requestHopsNames.add(nodeInfo.nodeId);
                            m.fromIP = nodeInfo.IP_address;
                            m.fromPort = nodeInfo.port;
                            m.fromNodeId = nodeInfo.nodeId;
                            sendMessage(toIP, toPort, m);
                        }
                        break;
                    }                
                }
                if(!found) {
                    routeMessage(m);
                }
            }
            else if(m.messageDirection.equals("Reply")) {
                timer.cancel();
                System.out.println("ITEM FOUND!");
                System.out.println(m.text + " was found at " + m.fromNodeId + " (" + m.fromIP + ":" + m.fromPort + ")");
                byte[] value = (byte[])m.content[2];
                writeToFile(m.text, value);
                System.out.println("Route taken:");
                for(int i = 0; i < m.requestHopsNames.size(); i++) {
                    System.out.print(m.requestHopsNames.get(i) + " (" + m.requestHopsIP.get(i) + ")");
                    if(i + 1 < m.requestHopsNames.size()) {
                        System.out.print(" --> ");
                    }
                }
                System.out.println();
            }
        }
        
        else if(m.messageType.equals("IQUIT")) {
            Zone inZone = (Zone)m.content[0];
            System.out.print("###### Leave request received from " + inZone.ownerId + "...");
            for(int i = 0; i < inZone.neighbors.size(); i++) {
                String nId = ((Zone)inZone.neighbors.get(i)).ownerId;
                if(nId.equalsIgnoreCase(nodeInfo.nodeId) || nId.equalsIgnoreCase(inZone.ownerId)) {
                    inZone.neighbors.remove(i);
                    i--;
                    continue;
                }
                String toIP = ((Zone)inZone.neighbors.get(i)).ownerIP;
                int toPort = ((Zone)inZone.neighbors.get(i)).ownerPort;
                CANMessage msg = new CANMessage();
                msg.fromIP = nodeInfo.IP_address;
                msg.fromPort = nodeInfo.port;
                msg.fromNodeId = nodeInfo.nodeId;
                msg.messageType = "NodeIsDown";
                msg.text = inZone.ownerId;
                sendMessage(toIP, toPort, msg);
            }
            
            boolean merged = false;
            for(int i = 0; i < nodeInfo.zones.size(); i++) {
                Zone z = nodeInfo.zones.get(i);
                if(z.isMergeable(inZone)) {
                    merged = true;
                    Zone newZone = Zone.mergeZones(inZone, z);
                    newZone.ownerId = nodeInfo.nodeId;
                    newZone.ownerIP = nodeInfo.IP_address;
                    newZone.ownerPort = nodeInfo.port;
                    nodeInfo.zones.remove(i);
                    nodeInfo.zones.add(i, newZone);
                    break;
                }
            }
            if(!merged) {
                inZone.ownerIP = nodeInfo.IP_address;
                inZone.ownerPort = nodeInfo.port;
                inZone.ownerId = nodeInfo.nodeId;
                nodeInfo.zones.add(inZone);
            }
            ConcurrentHashMap<String, NodeInfo> removeList = nodeInfo.updateNodeInfoNeighbors(m.fromNodeId);
            sendRemoveNeighbor(removeList, m.fromNodeId);
            nodeInfo.neighbors.remove(m.fromNodeId);
            System.out.println("  ...and it left.");
            sendRefresh(m.fromNodeId);
            viewPeer();
        }
        else if(m.messageType.equals("NodeIsDown")) {      
            nodeInfo.neighbors.remove(m.text);
            nodeInfo.updateZones();            
            System.out.println(m.text + " left *********************************");
            viewPeer();
        }
        else if(m.messageType.equals("REFRESH")) {            
            NodeInfo neighbor = (NodeInfo)m.content[0];
            nodeInfo.neighbors.put(neighbor.nodeId, neighbor);
            nodeInfo.updateZones();
            viewPeer();
        }
        else if(m.messageType.equals("REMOVEMYSELF")) {            
            NodeInfo neighbor = (NodeInfo)m.content[0];
            nodeInfo.neighbors.remove(neighbor.nodeId);            
            nodeInfo.updateZones();            
            System.out.println(neighbor.nodeId + " has beed removed from neighbor list.");
            viewPeer();
        }
        else if(m.messageType.equals("VIEWPEER") || m.messageType.equals("VIEWALL")) {
            NodeInfo remoteNode = (NodeInfo)m.content[0];
            viewPeer(remoteNode);
        }
        
    }
    
    private void routeMessage(CANMessage m) {
        
        Point p = (Point)m.content[0];
        String toIP = null;
        int toPort = 0;
        double distance = Double.MAX_VALUE;
        for(int i = 0; i < nodeInfo.zones.size(); i++) {
            Iterator it = nodeInfo.zones.get(i).neighbors.iterator();
            while(it.hasNext()) {
                Zone z = (Zone)it.next();
                if(z.isInsideZone(p)) {
                    toIP = z.ownerIP;
                    toPort = z.ownerPort;
                    break;
                }
                double d = z.getDistance(p.x, p.y);
                if(d < distance) {
                    distance = d;
                    toIP = z.ownerIP;
                    toPort = z.ownerPort;
                }
            }
        }
        
        m.requestHopsIP.add(nodeInfo.IP_address);
        m.requestHopsNames.add(nodeInfo.nodeId);
        sendMessage(toIP, toPort, m);
    }
    
    private void sendRefresh(String except) {
        
        CANMessage m = new CANMessage();
        m.fromIP = nodeInfo.IP_address;
        m.fromPort = nodeInfo.port;
        m.fromNodeId = nodeInfo.nodeId;
        m.messageType = "REFRESH";
        m.messageDirection = "Request";
        
        Iterator it = nodeInfo.neighbors.keySet().iterator();
        while(it.hasNext()) {
            String peer = (String)it.next();
            if(except != null && peer.equals(except)) {
                continue;
            }
            NodeInfo neighbor = nodeInfo.neighbors.get(peer);
            m.toIP = neighbor.IP_address;
            m.toPort = neighbor.port;
            m.content = null;
            m.content = new Object[1];
            m.content[0] = nodeInfo.getMemoryFriendlyNodeInfo();
            sendMessage(neighbor.IP_address, neighbor.port, m);
            System.out.println("REFRESH sent to " + peer);
        }
                
    }
    
    private void sendRemoveNeighbor(ConcurrentHashMap<String, NodeInfo> list, String except) {
        CANMessage m = new CANMessage();
        m.fromIP = nodeInfo.IP_address;
        m.fromPort = nodeInfo.port;
        m.fromNodeId = nodeInfo.nodeId;
        m.messageType = "REMOVEMYSELF";
        m.messageDirection = "Request";
        
        Iterator it = list.keySet().iterator();
        while(it.hasNext()) {
            String peer = (String)it.next();
            if(except != null && peer.equals(except)) {
                continue;
            }
            NodeInfo notNeighbor = list.get(peer);
            m.toIP = notNeighbor.IP_address;
            m.toPort = notNeighbor.port;
            m.content = null;
            m.content = new Object[1];
            m.content[0] = nodeInfo.getMemoryFriendlyNodeInfo();
            sendMessage(notNeighbor.IP_address, notNeighbor.port, m);
            System.out.println("REMOVEMYSELF sent to " + peer);
        }
    }
    
    private synchronized void sendMessage(String IP, int port, CANMessage m) {
        for (int i = 0; i < 2; i++) {
            try {
                Socket socket = new Socket(IP, port);
                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                oos.writeObject(m);
                oos.flush();
                oos.close();
                socket.close();
                break;
            } catch (Exception excp) {
                printError(excp, "");
                System.out.print("Message sending failure at the TCP level");
                if (i == 0) {
                    System.out.println(" on 1st attempt. Trying again.");
                } else if (i == 1) {
                    System.out.println(" on 2nd attempt. Giving up.");
                    break;
                }
                try {
                    Thread.sleep(200);
                }
                catch(Exception ex) {
                    printError(ex, "");
                }
                continue;
            }

        }
    }
    
    private void sendViewRequest(String nodeId) {
        StringBuffer sb = new StringBuffer(nodeInfo.IP_address);
        int from = sb.lastIndexOf(".");        
        String s = sb.substring(0, from + 1);
        String broadcastIP = s + "255";
        CANMessage m = new CANMessage();        
        m.fromIP = nodeInfo.IP_address;
        m.fromPort = nodeInfo.port;
        m.fromNodeId = nodeInfo.nodeId;
        m.messageDirection = "Request";
        m.messageType = "VIEWPEER";
        m.text = nodeId;
        m.content = new Object[2];
        m.content[0] = nodeInfo.IP_address;
        m.content[1] = nodeInfo.port;
        udpHandler.sendUDPData(m, broadcastIP);
    }    
        
    private void broadcastView() {
        StringBuffer sb = new StringBuffer(nodeInfo.IP_address);
        int from = sb.lastIndexOf(".");        
        String s = sb.substring(0, from + 1);
        String broadcastIP = s + "255";
        CANMessage m = new CANMessage();        
        m.fromIP = nodeInfo.IP_address;
        m.fromPort = nodeInfo.port;
        m.fromNodeId = nodeInfo.nodeId;
        m.messageDirection = "Request";
        m.messageType = "VIEWALL";
        m.content = new Object[2];
        m.content[0] = nodeInfo.IP_address;
        m.content[1] = nodeInfo.port;
        udpHandler.sendUDPData(m, broadcastIP);
    }
    
    private void sendJoinRequest(String nodeId) {
        StringBuffer sb = new StringBuffer(nodeInfo.IP_address);
        int from = sb.lastIndexOf(".");        
        String s = sb.substring(0, from + 1);
        String broadcastIP = s + "255";
        CANMessage m = new CANMessage();        
        m.fromIP = nodeInfo.IP_address;
        m.fromPort = nodeInfo.port;
        m.fromNodeId = nodeInfo.nodeId;
        m.messageDirection = "Request";
        m.messageType = "JOINPEER";
        m.text = nodeId;
        m.content = new Object[2];
        m.content[0] = bootstrap_IP;
        m.content[1] = bootstrap_port;
        udpHandler.sendUDPData(m, broadcastIP);
    }
    
    private void broadcastJoin() {
        StringBuffer sb = new StringBuffer(nodeInfo.IP_address);
        int from = sb.lastIndexOf(".");        
        String s = sb.substring(0, from + 1);
        String broadcastIP = s + "255";
        CANMessage m = new CANMessage();        
        m.fromIP = nodeInfo.IP_address;
        m.fromPort = nodeInfo.port;
        m.fromNodeId = nodeInfo.nodeId;
        m.messageDirection = "Request";
        m.messageType = "JOINALL";
        m.content = new Object[2];
        m.content[0] = bootstrap_IP;
        m.content[1] = bootstrap_port;
        udpHandler.sendUDPData(m, broadcastIP);
    }
    
    public static void main(String[] args) {
        //unitTesting();
        if(args == null || args.length == 2) {
            try {
                String name = args[0];
                int port = Integer.parseInt(args[1]);
                NodeCAN nc = new NodeCAN(name, port);
            } catch (NumberFormatException ex) {
                System.out.println("Could not parse the port number. It should be an integer.");
                printArgumentError();
            }
            
        }
        else if(args == null || args.length == 4) {
            try {
                String name = args[0];
                int port = Integer.parseInt(args[1]);
                String b_IP = args[2];
                int b_port = Integer.parseInt(args[3]);
                NodeCAN nc = new NodeCAN(name, port, b_IP, b_port);
            } catch (NumberFormatException ex) {
                System.out.println("Could not parse the port number. It should be an integer.");
                printArgumentError();
            }
        }
        else {
            printArgumentError();
        }
    }
    
    private static void printArgumentError() {
        System.out.println("Format error! Correct format is:");
        System.out.println("NodeCAN.java node_id port [bootstrap_IP bootstrap_port]");
    }
    
    public static void printError(Exception excp, String msg) {
        if(DEBUGMODE) {
            System.out.println(excp.getMessage());
            excp.printStackTrace();
        }
        else {
            System.out.println(msg);
        }
    }
    
    class ListenFromKeyboard implements Runnable {
    
        public ListenFromKeyboard() {}
        
        public void run() {
            while (true) {
                Scanner keyboard = new Scanner(System.in);
                System.out.print("Enter command: ");
                String input = keyboard.nextLine();
                processKeyboardInput(input);
            }
        }
    }
    
    class ProcessIncomingRequest implements Runnable {
    
        Socket socket;
        public ProcessIncomingRequest(Socket s) {
            socket = s;
        }
        
        public void run() {
            ObjectInputStream ois;
            CANMessage m = null;
            try {
                ois = new ObjectInputStream(socket.getInputStream());
                m = (CANMessage) ois.readObject();
                ois.close();
                socket.close();
                processRequestFromNetwork(m);
            } catch (Exception ex) {
                printError(ex, "");
            }

        }
    }
    
    class GeneratePeriodicUpdate implements Runnable {
            
        public GeneratePeriodicUpdate() {
            
        }
        
        public void run() {
            while (true) {
                try {
                    Thread.sleep(INTERVAL * 1000);
                } catch (Exception ex) {
                    printError(ex, "");
                    continue;
                }

                try {

                    Iterator it = nodeInfo.neighbors.keySet().iterator();
                    while (it.hasNext()) {
                        String peer = (String) it.next();
                        try {
                            Thread t1 = new Thread(new GeneratePeriodicUpdateWorker(peer));
                            t1.start();
                        } catch (Exception excp) {
                            printError(excp, "");
                            continue;
                        }
                    }

                } catch (Exception excp) {
                    printError(excp, "");
                    continue;
                }

            }

        }
        
        class GeneratePeriodicUpdateWorker implements Runnable {
            
            private String peer = null;
        
            public GeneratePeriodicUpdateWorker(String p) {
                peer = p;
            }
        
            public void run() {
                Date rightNow = new Date();
                if (!lastHeard.containsKey(peer)) {
                    lastHeard.put(peer, new Date());
                } else {
                    Date lhd = lastHeard.get(peer);

                    //System.out.println("Right now: " + rightNow.getTime()
                    //    + "  last heard from " + peer + ": " + lhd.getTime() 
                    //        + "  Difference: " + (rightNow.getTime() - lhd.getTime()));
                    if ((rightNow.getTime() - lhd.getTime()) > (3 * INTERVAL * 1000)) {
                        nodeInfo.neighbors.remove(peer);
                        lastHeard.remove(peer);
                        nodeInfo.updateZones();
                        System.out.println("Peer: " + peer + " is considered to be absent.");
                    } else {

                        CANMessage m = new CANMessage();
                        m.fromIP = nodeInfo.IP_address;
                        m.fromPort = nodeInfo.port;
                        m.fromNodeId = nodeInfo.nodeId;
                        m.messageType = "REFRESH";
                        m.messageDirection = "Request";
                        NodeInfo neighbor = nodeInfo.neighbors.get(peer);
                        m.toIP = neighbor.IP_address;
                        m.toPort = neighbor.port;
                        m.content = null;
                        m.content = new Object[1];
                        m.content[0] = nodeInfo.getMemoryFriendlyNodeInfo();

                        try {
                            Socket socket = new Socket(neighbor.IP_address, neighbor.port);
                            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                            oos.writeObject(m);
                            oos.flush();
                            oos.close();
                            socket.close();
                            System.out.println("PERIODIC REFRESH sent to " + peer);

                        } catch (Exception excp) {
                            printError(excp, "");
                            try {
                                Thread.sleep(200);
                            } catch (Exception ex) {
                                printError(ex, "");

                            }

                        }

                    }
                }
            }
        }        
    }
    class FailureTimer implements Runnable {

        private String message = null;
        private boolean canceled = false;

        public FailureTimer(String msg) {
            message = msg;
        }

        public void run() {
            try {
                Thread.sleep(2000);
            } catch (Exception ex) {
                printError(ex, "");
            }
            if (!canceled) {
                System.out.println(message);
                if(message.equalsIgnoreCase("Failure to JOIN.")) {
                    joined = false;
                    joinSent = false;
                }
            }
        }

        public void cancel() {
            canceled = true;
        }
    }
    
    class ListenToUDP implements Runnable {

        private DatagramSocket serverSocket = null;

        public ListenToUDP() {
            int port = 22222;
            for (int i = 0; i < 10; i++) {
                try {
                    serverSocket = new DatagramSocket(port + i);
                    System.out.println("UDP listener running at port: " + (port + i));
                    break;
                } catch (Exception ex) {
                    continue;
                }
            }

        }

        public void run() {
            while (true) {
                CANMessage m = receiveUDPData();
                //System.out.println("##########################################################");
                if(m == null) {
                    continue;
                }
                if (m.messageType.equals("VIEWALL")) {
                    m.fromIP = nodeInfo.IP_address;
                    m.fromPort = nodeInfo.port;
                    m.fromNodeId = nodeInfo.nodeId;
                    m.messageDirection = "Reply";
                    m.messageType = "VIEWALL";
                    m.text = nodeInfo.nodeId;
                    String toIP = (String)m.content[0];
                    int toPort = (int)m.content[1];
                    m.content = new Object[1];
                    m.content[0] = nodeInfo;
                    sendMessage(toIP, toPort, m);
                }
                else if (m.messageType.equals("VIEWPEER")) {
                    if (m.text.equalsIgnoreCase(nodeInfo.nodeId)) {
                        m.fromIP = nodeInfo.IP_address;
                        m.fromPort = nodeInfo.port;
                        m.fromNodeId = nodeInfo.nodeId;
                        m.messageDirection = "Reply";
                        m.messageType = "VIEWPEER";
                        m.text = nodeInfo.nodeId;
                        String toIP = (String) m.content[0];
                        int toPort = (int) m.content[1];
                        m.content = new Object[1];
                        m.content[0] = nodeInfo;
                        sendMessage(toIP, toPort, m);
                    }
                }
                else if(m.messageType.equals("JOINALL")) {
                    if(!joined) {
                        joinCAN();
                    }
                }
                else if(m.messageType.equals("JOINPEER")) {
                    if(m.text.equalsIgnoreCase(nodeInfo.nodeId)) {
                        if(!joined) {
                            joinCAN();
                        }
                    }
                }
            }
        }

        private CANMessage receiveUDPData() {
            byte[] receiveData = new byte[8192];
            DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
            try {
                serverSocket.receive(receivePacket);
            } catch (Exception excp) {
                System.err.println(excp.getMessage());
            }
            SocketAddress address = receivePacket.getSocketAddress();

            ObjectInputStream in = null;
            CANMessage m = null;
            try {
                in = new ObjectInputStream(new ByteArrayInputStream(receiveData));
                in.close();
                try {
                    m = (CANMessage) in.readObject();
                } catch (ClassNotFoundException ex) {
                    System.out.println("Class not found. " + ex.getMessage());
                    return null;
                }
            } catch (IOException ex) {
                System.out.println("Could not receive data. " + ex.getMessage());
                return null;
            }

            return m;
        }
        
        private void sendUDPData(CANMessage m, String broadcastIP) {
            
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream out = null;
            try {
                out = new ObjectOutputStream(baos);
            } catch (IOException ex) {
                System.out.println("Could not create output stream. " + ex.getMessage());
                return;
            }

            try {
                out.writeObject(m);
                out.flush();
                out.close();
            } catch (IOException ex) {
                System.out.println("Could not write to output stream. " + ex.getMessage());
                return;
            }

            byte[] buf = baos.toByteArray();
            try {
                int port = 22222;
                for(int i = 0; i < 10; i++) {
                    SocketAddress address = new InetSocketAddress(broadcastIP, (port + i));
                    DatagramPacket sendPacket = new DatagramPacket(buf, buf.length, address);
                    serverSocket.send(sendPacket);
                }
                //return true;
            } catch (IOException excp) {
                System.err.println("Could not send message to user.");
                return;
            }
        }
    }
}
