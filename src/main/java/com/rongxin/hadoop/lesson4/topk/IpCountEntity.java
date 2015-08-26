package com.rongxin.hadoop.lesson4.topk;

import java.awt.color.ICC_Profile;

/**
 * Created by guoxian1 on 15/4/2.
 *
 */
public class IpCountEntity implements Comparable<IpCountEntity>{

    private long pv;

    private String ip;

    public String getIp() {
        return ip;
    }

    public void setPv(long pv) {
        this.pv = pv;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public long getPv() {
        return pv;
    }

    public int hashCode(){
         return ip.hashCode();
     }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;

        if (getClass() != obj.getClass())
            return false;

        IpCountEntity other = (IpCountEntity) obj;
        if (pv != other.pv)
            return false;

        if (ip == null) {
            if (other.ip != null)
                return false;
        } else if (!ip.equals(other.ip))
            return false;

        return true;
    }

    public IpCountEntity(){

    }

    public  IpCountEntity(long pv, String ip){
        this.pv = pv;
        this.ip = ip;
    }


    @Override
    public int compareTo(IpCountEntity o) {
         if (o instanceof IpCountEntity) {
             long cmp = (pv - ((IpCountEntity) o).getPv());
             if (cmp == 0){
                 if(o.getIp().equals(ip)){
                     return 0;
                 }else{
                     return ip.compareTo(o.getIp());
                 }
             }
             return (int) cmp;
         }
        throw new ClassCastException("Cannot compare IpCountEntity with" + o.getClass().getName());
    }
}