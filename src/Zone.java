
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Abhishek
 */
public class Zone implements Serializable {
    public static final double MAX_COOR = 10;
    public static final double THRESHOLD = 0.0001;
    public String ownerIP;
    public int ownerPort;
    public String ownerId;
    public ArrayList neighbors;
    public double x1;
    public double x2;
    public double y1;
    public double y2;
    
    public ConcurrentHashMap<String, byte[]> dhtData = null;
    public ConcurrentHashMap<String, NodeInfo> dhtNode = null;
    
    public Zone(String IP, int port) {
        neighbors = new ArrayList<Zone>();
        ownerIP = IP;
        ownerPort = port;
        x1 = 0;
        x2 = 10;
        y1 = 0;
        y2 = 10;
        dhtData = new ConcurrentHashMap<String, byte[]>();
        dhtNode = new ConcurrentHashMap<String, NodeInfo>();
    }
    
    public Zone(String IP, int port, double x1, double x2, double y1, double y2) {
        neighbors = new ArrayList<Zone>();
        ownerIP = IP;
        ownerPort = port;
        this.x1 = (x1 > MAX_COOR) ? x1 - MAX_COOR : x1;
        this.x2 = (x2 > MAX_COOR) ? x2 - MAX_COOR : x2;
        this.y1 = (y1 > MAX_COOR) ? y1 - MAX_COOR : y1;
        this.y2 = (y2 > MAX_COOR) ? y2 - MAX_COOR : y2;
        
        if(Math.abs(this.x1 - this.x2) < 0.00001) {
            this.x1 = 0;
            this.x2 = 10;
        }
        if(Math.abs(this.y1 - this.y2) < 0.00001) {
            this.y1 = 0;
            this.y2 = 10;
        }
        dhtData = new ConcurrentHashMap<String, byte[]>();
        dhtNode = new ConcurrentHashMap<String, NodeInfo>();
    }
    
    private Zone(double x1, double x2, double y1, double y2) {
        neighbors = new ArrayList<Zone>();
        this.x1 = (x1 > MAX_COOR) ? x1 - MAX_COOR : x1;
        this.x2 = (x2 > MAX_COOR) ? x2 - MAX_COOR : x2;
        this.y1 = (y1 > MAX_COOR) ? y1 - MAX_COOR : y1;
        this.y2 = (y2 > MAX_COOR) ? y2 - MAX_COOR : y2;
        
        if(Math.abs(this.x1 - this.x2) < 0.00001) {
            this.x1 = 0;
            this.x2 = 10;
        }
        if(Math.abs(this.y1 - this.y2) < 0.00001) {
            this.y1 = 0;
            this.y2 = 10;
        }
        dhtData = new ConcurrentHashMap<String, byte[]>();
        dhtNode = new ConcurrentHashMap<String, NodeInfo>();
    }
    
    public boolean isNeighbor(Zone z) {
        Zone z1 = null, z2 = null;
        try {
        z1 = (Zone)this.clone();
        z2 = (Zone)z.clone();
        }
        catch (Exception excp) {NodeCAN.printError(excp, "");}
        
        z1.x1 = z1.x1 + THRESHOLD;
        z1.x2 = z1.x2 - THRESHOLD;
        z1.y1 = z1.y1 + THRESHOLD;
        z1.y2 = z1.y2 - THRESHOLD;
        
        z2.x1 = z2.x1 + THRESHOLD;
        z2.x2 = z2.x2 - THRESHOLD;
        z2.y1 = z2.y1 + THRESHOLD;
        z2.y2 = z2.y2 - THRESHOLD;
        
        
        if(checkNeighbor(z1, z2)) {
            return true;
        }
        else if(checkNeighbor(z2, z1)) {
            return true;
        }
        return false;
    }
    
    public boolean isInsideZone(Point p) {
        return isInsideZone(p.x, p.y);
    }
    
    public boolean isInsideZone(double pointX, double pointY) {
        
        double zx1 = x1;
        double zx2 = x2;
        double zy1 = y1;
        double zy2 = y2;

        if (zx1 >= zx2) {
            zx2 += MAX_COOR;
        }
        if (zy1 >= zy2) {
            zy2 += MAX_COOR;
        }
        
        if((pointX > zx1 && pointX <= zx2) || (pointX + MAX_COOR > zx1 && pointX + MAX_COOR <= zx2)) {
            if((pointY > zy1 && pointY <= zy2) || (pointY + MAX_COOR > zy1 && pointY + MAX_COOR <= zy2)) {
                return true;
            }
        }
        
        return false;
    }
    
    public double getDistance(double pointX, double pointY) {
        Point midPoint = findMidPoint(this);
        
        double xd = Math.abs(pointX - midPoint.x);
        double c1 = Math.min(xd, Math.abs(xd - MAX_COOR));
        
        double yd = Math.abs(pointY - midPoint.y);
        double c2 = Math.min(yd, Math.abs(yd - MAX_COOR));
        
        double sum = c1 * c1 + c2 * c2;
        return Math.sqrt(sum);
    }
    
    public Zone[] splitZone() {
        Zone[] splitted = new Zone[2];
        
        double side1, side2;
        
        Point midPoint = findMidPoint(this);
        if(x1 >= x2) {
            side1 = x2 + MAX_COOR - x1;
        }
        else {
            side1 = x2 - x1;
        }
        
        if(y1 >= y2) {
            side2 = y2 + MAX_COOR - y1;
        }
        else {
            side2 = y2 - y1;
        }
        //System.out.println("Side1: " + side1 + "   Side2: " + side2);
        if(Math.abs(side1 - side2) < 0.00001) {
            // the zone is a square, split vertically
            splitted[0] = new Zone(x1, midPoint.x, y1, y2);
            splitted[1] = new Zone(midPoint.x, x2, y1, y2);
        }
        else {
            // the zone is a rectangle, split horizontally
            if(side1 < side2){
                splitted[0] = new Zone(x1, x2, y1, midPoint.y);
                splitted[1] = new Zone(x1, x2, midPoint.y, y2);
            }
            else {
                splitted[0] = new Zone(x1, midPoint.x, y1, y2);
                splitted[1] = new Zone(midPoint.x, x2, y1, y2);
            }
        }
        
        // set up neighbors accordingly        
        Iterator i = neighbors.iterator();
        while(i.hasNext()) {
            Zone z = (Zone)i.next();
            if(splitted[0].isNeighbor(z)) {
                splitted[0].neighbors.add(z);
            }
            if(splitted[1].isNeighbor(z)) {
                splitted[1].neighbors.add(z);
            }
        }
        
        // distribute the hashed info
        Iterator it = dhtData.keySet().iterator();
        while(it.hasNext()) {
            String keyword = (String)it.next();
            byte[] value = dhtData.get(keyword);
            
            if(splitted[0].isInsideZone(getMappingPoint(keyword))) {
                splitted[0].dhtData.put(keyword, value);
            }
            else {
                splitted[1].dhtData.put(keyword, value);
            }
        }
        
        it = null;
        it = dhtNode.keySet().iterator();
        while(it.hasNext()) {
            String keyword = (String)it.next();
            NodeInfo value = dhtNode.get(keyword);
            
            if(splitted[0].isInsideZone(getMappingPoint(keyword))) {
                splitted[0].dhtNode.put(keyword, value);
            }
            else {
                splitted[1].dhtNode.put(keyword, value);
            }
        }
        
        return splitted;
    }
    
    public boolean isMergeable(Zone z) {
        Zone z1 = null, z2 = null;
        try {
        z1 = (Zone)this.clone();
        z2 = (Zone)z.clone();
        }
        catch (Exception excp) {NodeCAN.printError(excp, "");}
        
        if(checkMergeable(z1, z2)) {
            return true;
        }
        else if(checkMergeable(z2, z1)) {
            return true;
        }
        return false;
    }
    
    public static Zone mergeZones(Zone z1, Zone z2) {
        Zone z = null;
        if(checkMergeable(z1, z2)) {
            z = merge(z1, z2);
        }
        else if(checkMergeable(z2, z1)) {
            z = merge(z2, z1);
        }
        if(z != null) {
            //set up z's new neighbors
            for(int i = 0; i < z1.neighbors.size(); i++) {
                Zone neighbor = (Zone)z1.neighbors.get(i);
                if(!neighbor.equals(z2)) {
                    z.neighbors.add(neighbor);
                }
            }
            for(int i = 0; i < z2.neighbors.size(); i++) {
                Zone neighbor = (Zone)z2.neighbors.get(i);
                if(!neighbor.equals(z1)) {
                    z.neighbors.add(neighbor);
                }
            }
        }
        
        // merge DHT
        Iterator it = z1.dhtData.keySet().iterator();
        while(it.hasNext()) {
            String keyword = (String)it.next();
            byte[] value = z1.dhtData.get(keyword);
            z.dhtData.put(keyword, value);
        }
        it = null;
        it = z2.dhtData.keySet().iterator();
        while(it.hasNext()) {
            String keyword = (String)it.next();
            byte[] value = z2.dhtData.get(keyword);
            z.dhtData.put(keyword, value);
        }
        
        it = z1.dhtNode.keySet().iterator();
        while(it.hasNext()) {
            String keyword = (String)it.next();
            NodeInfo value = z1.dhtNode.get(keyword);
            z.dhtNode.put(keyword, value);
        }
        it = null;
        it = z2.dhtNode.keySet().iterator();
        while(it.hasNext()) {
            String keyword = (String)it.next();
            NodeInfo value = z2.dhtNode.get(keyword);
            z.dhtNode.put(keyword, value);
        }
        
        return z;
    }
    
    private static Zone merge(Zone z1, Zone z2) {
        
        // check if they align along x-axis
        double zx1 = z2.x1;
        double zx2 = z2.x2;
        double zy1 = z2.y1;
        double zy2 = z2.y2;

        if (zx1 >= zx2) {
            zx2 += MAX_COOR;
        }
        if (zy1 >= zy2) {
            zy2 += MAX_COOR;
        }
                       
                
        if (((Math.abs(z1.x1 - zx1) < 0.01) && (Math.abs(z1.x2 - zx2) < 0.01))
                || ( (Math.abs(z1.x1 + MAX_COOR - zx1) < 0.01)   && (Math.abs(z1.x2 + MAX_COOR - zx2) < 0.01))) {
            // aligns
            
            if((Math.abs(z1.y1 - zy2) < 0.01) || (Math.abs(Math.abs(z1.y1 - zy2) - 10) < 0.01)) {
                return new Zone(zx1, zx2, zy1, z1.y2);            
            }
            else if ( (Math.abs(z1.y2 - zy1) < 0.01)  ||  (Math.abs(Math.abs(z1.y2 - zy1) - 10) < 0.01) ) {
                return new Zone(zx1, zx2, z1.y1, zy2);
                
            }
        }

        // check if they align along y-axis
        zx1 = z2.x1;
        zx2 = z2.x2;
        zy1 = z2.y1;
        zy2 = z2.y2;
        if (zx1 >= zx2) {
            zx2 += MAX_COOR;
        }
        if (zy1 >= zy2) {
            zy2 += MAX_COOR;
        }
        
        if ((Math.abs(z1.y1 - zy1) < 0.01 && Math.abs(z1.y2 - zy2) < 0.01)
                || (Math.abs(z1.y1 + MAX_COOR - zy1) < 0.01 && Math.abs(z1.y2 + MAX_COOR - zy2) < 0.01)) {
            // aligns
            if((Math.abs(z1.x1 - zx2) < 0.01) || (Math.abs(Math.abs(z1.x1 - zx2) - 10) < 0.01)) {
                return new Zone(zx1, z1.x2, zy1, zy2);
            }
            else if ( (Math.abs(z1.x2 - zx1) < 0.01) || (Math.abs(Math.abs(z1.x2 - zx1) - 10) < 0.01) ) {
                return new Zone(z1.x1, zx2, zy1, zy2);
            }
        }

        return null;
    }
    
    private static boolean checkMergeable(Zone z1, Zone z2) {
        // check if they align along x-axis
        double zx1 = z2.x1;
        double zx2 = z2.x2;
        double zy1 = z2.y1;
        double zy2 = z2.y2;
        double z1x1 = z1.x1;
        double z1x2 = z1.x2;
        double z1y1 = z1.y1;
        double z1y2 = z1.y2;

        if (zx1 >= zx2) {
            zx2 += MAX_COOR;
        }
        if (z1x1 >= z1x2) {
            z1x2 += MAX_COOR;
        }
        
                       
                
        if (((Math.abs(z1x1 - zx1) < 0.01) && (Math.abs(z1x2 - zx2) < 0.01))) {
            // aligns
            if (((Math.abs(z1.y1 - zy2) < 0.01) || (Math.abs(z1.y2 - zy1) < 0.01))
                    || ((Math.abs(Math.abs(z1.y1 - zy2) - 10) < 0.01) || (Math.abs(Math.abs(z1.y2 - zy1) - 10) < 0.01))) {

                return true;
            }
        }

        // check if they align along y-axis
        zx1 = z2.x1;
        zx2 = z2.x2;
        zy1 = z2.y1;
        zy2 = z2.y2;
        z1x1 = z1.x1;
        z1x2 = z1.x2;
        z1y1 = z1.y1;
        z1y2 = z1.y2;
        if (zy1 >= zy2) {
            zy2 += MAX_COOR;
        }
        if (z1y1 >= z1y2) {
            z1y2 += MAX_COOR;
        }
        
        if ((Math.abs(z1y1 - zy1) < 0.01 && Math.abs(z1y2 - zy2) < 0.01)) {
            // aligns
            if (((Math.abs(z1.x1 - zx2) < 0.01) || (Math.abs(z1.x2 - zx1) < 0.01))
                    || ((Math.abs(Math.abs(z1.x1 - zx2) - 10) < 0.01) || (Math.abs(Math.abs(z1.x2 - zx1) - 10) < 0.01))) {
                return true;
            }
        }

        return false;
    }
    
    
    private static boolean checkNeighbor(Zone z1, Zone z2) {
        // check if they overlap along x-axis
        double zx1 = z2.x1;
        double zx2 = z2.x2;
        double zy1 = z2.y1;
        double zy2 = z2.y2;
        
        if(zx1 >= zx2) {
            zx2 += MAX_COOR;
        }
        
        if(((z1.x1 >= zx1 && z1.x1 <= zx2) || (z1.x2 >= zx1 && z1.x2 <= zx2)) || 
                ((z1.x1 + MAX_COOR >= zx1 && z1.x1 + MAX_COOR <= zx2) || (z1.x2 + MAX_COOR >= zx1 && z1.x2 + MAX_COOR <= zx2))) {
            // overlaps
            if((Math.abs(z1.y1 - zy2) < 0.01) || (Math.abs(z1.y2 - zy1) < 0.01)
                    || (Math.abs(Math.abs(z1.y1 - zy2) - 10) < 0.01) || (Math.abs(Math.abs(z1.y2 - zy1) - 10) < 0.01)) {
                
                return true;
            }
        }
        
        
        // check if they overlap along y-axis
        zx1 = z2.x1;
        zx2 = z2.x2;
        zy1 = z2.y1;
        zy2 = z2.y2;
        if(zy1 >= zy2) {
            zy2 += MAX_COOR;
        }
        
        if(((z1.y1 >= zy1 && z1.y1 <= zy2) || (z1.y2 >= zy1 && z1.y2 <= zy2)) || 
                ((z1.y1 + MAX_COOR >= zy1 && z1.y1 + MAX_COOR <= zy2) || (z1.y2 + MAX_COOR >= zy1 && z1.y2 + MAX_COOR <= zy2))) {
            // overlaps
            if((Math.abs(z1.x1 - zx2) < 0.01) || (Math.abs(z1.x2 - zx1) < 0.01)
                    || (Math.abs(Math.abs(z1.x1 - zx2) - 10) < 0.01) || (Math.abs(Math.abs(z1.x2 - zx1) - 10) < 0.01)) {
                return true;
            }
        }
        
        
        return false;
    }
    
    private Point findMidPoint(Zone z) {
        double x, y;
        if(z.x1 >= z.x2) {
            x = (z.x1 + z.x2 + MAX_COOR) / 2;
            if(x >= MAX_COOR) {
                x = x - MAX_COOR;
            }
        }
        else {
            x = (z.x1 + z.x2) / 2;
        }
        
        if(z.y1 >= z.y2) {
            y = (z.y1 + z.y2 + MAX_COOR) / 2;
            if(y >= MAX_COOR) {
                y = y - MAX_COOR;
            }
        }
        else {
            y = (z.y1 + z.y2) / 2;
        }
        
        return new Point(x, y);
    }
    
    protected Zone clone() {
        Zone z = new Zone(ownerIP, ownerPort, x1, x2, y1, y2);
        return z;
    }
    
    public boolean equals(Zone z) {
        if( Math.abs(this.x1 - z.x1) < 0.01 && Math.abs(this.x2 - z.x2) < 0.01
                && Math.abs(this.y1 - z.y1) < 0.01 && Math.abs(this.y2 - z.y2) < 0.01) {
            return true;
        }
        return false;
    }
    
    public double area() {
        double side1, side2;
        
        if(x1 >= x2) {
            side1 = x2 + MAX_COOR - x1;
        }
        else {
            side1 = x2 - x1;
        }
        
        if(y1 >= y2) {
            side2 = y2 + MAX_COOR - y1;
        }
        else {
            side2 = y2 - y1;
        }
        return side1 * side2;
    }
    
    public static Point getMappingPoint(String keyword) {
        byte[] bArray = keyword.getBytes();
        int xcoor = 0;
        int ycoor = 0;
        
        for(int i = 0; i < bArray.length; i++) {
            if(i % 2 == 0) {
                //even
                ycoor = ycoor + (int)bArray[i];
            }
            else {
                //odd
                xcoor = xcoor + (int)bArray[i];
            }
        }
        xcoor = xcoor % 10;
        ycoor = ycoor % 10;
        return new Point(xcoor, ycoor);
    }
}
