/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;
/**
 *
 * @author Abhishek
 */
public class NodeInfo implements Serializable {
    
    String nodeId;
    String IP_address;
    int port;
    ArrayList<Zone> zones;
    ConcurrentHashMap<String, NodeInfo> neighbors;
    
    public NodeInfo() {
        zones = new ArrayList();
        neighbors = new ConcurrentHashMap<String, NodeInfo>();
    }
    
    public synchronized ConcurrentHashMap<String, NodeInfo> updateNodeInfoNeighbors(String deadNode) {
        if(zones == null) {
            return null;
        }
        ConcurrentHashMap<String, NodeInfo> tempNeighbors = getACopy(neighbors);
        neighbors.clear();
        for(int i = 0; i < zones.size(); i++) {
            Iterator it = zones.get(i).neighbors.iterator();
            while(it.hasNext()) {
                Zone z = (Zone) it.next();
                if(z.ownerId.equalsIgnoreCase(nodeId)) {
                    continue;
                }
                
                NodeInfo neighbor = neighbors.get(z.ownerId);
                if(neighbor == null) {
                    neighbor = new NodeInfo();
                    neighbor.nodeId = z.ownerId;
                    neighbor.IP_address = z.ownerIP;
                    neighbor.port = z.ownerPort;                    
                }
                
                for(int a = 0; a < neighbor.zones.size(); a++) {
                    if(neighbor.zones.get(a).equals(z)) {
                        neighbor.zones.remove(a);
                        a--;
                    }
                }
                neighbor.zones.add(z);
                if(deadNode != null && deadNode.equals(neighbor.nodeId)) {                    
                    
                }
                else {
                    neighbors.put(neighbor.nodeId, neighbor);
                }
            }
        }
        System.out.print("Neighbors updated for " + this.nodeId + ":    ");
        Iterator it = neighbors.keySet().iterator();
        while(it.hasNext()) {
            String n = (String)it.next();
            System.out.print(n + "  ");
            tempNeighbors.remove(n);
        }
        System.out.println();
        
        return tempNeighbors;
        
    }
    private ConcurrentHashMap<String, NodeInfo> getACopy(ConcurrentHashMap<String, NodeInfo> in)  {
        ConcurrentHashMap<String, NodeInfo> out = new ConcurrentHashMap<String, NodeInfo>();
        Iterator it = in.keySet().iterator();
        while(it.hasNext()) {
            String key = (String)it.next();
            NodeInfo value = in.get(key);
            out.put(key, value);
        }
        return out;
    }
    public void updateZones() {
        for(int i = 0; i < zones.size(); i++) {
            Zone z = zones.remove(i);
            
            z.neighbors.clear();
            Iterator it = neighbors.keySet().iterator();
            while(it.hasNext()) {
                NodeInfo neighborNode = neighbors.get((String)it.next());
                for(int j = 0; j < neighborNode.zones.size(); j++) {
                    Zone neighborZone = neighborNode.zones.get(j);
                    if(z.isNeighbor(neighborZone)) {
                        z.neighbors.add(neighborZone);
                    }
                }
            }
            
            zones.add(i, z);
        }
    }
    
    public NodeInfo getMemoryFriendlyNodeInfo() {
        NodeInfo retVal = new NodeInfo();
        retVal.IP_address = this.IP_address;
        retVal.port = this.port;
        retVal.nodeId = this.nodeId;
        
        retVal.neighbors = null;
        
        for(int i = 0; i < this.zones.size(); i++) {
            Zone z = this.zones.get(i);
            Zone nz = new Zone(IP_address, port, 
                z.x1, z.x2, z.y1, z.y2);
            nz.ownerId = this.nodeId;
            nz.neighbors = null;
            retVal.zones.add(nz);
        }
        
        
        return retVal;
    }
}
