package com.calabar.flinkDemo.model;

import lombok.Data;

import java.sql.Date;


/**
 * @author wqkenqing
 * @emai wqkenqingto@163.com
 * @time 2020/8/3
 * @desc
 */
@Data
public class Toll {
    private Integer fId;
    private String passType;
    private Date modifyTime;
    private Date passTime ;
    private String plateNo;
    private Integer RoadId;
    private String userName;
    private String feeType;
    private Integer VKindNo;
    private String plateColor;

    @Override
    public String toString() {
        return "Toll{" +
                "fId=" + fId +
                ", passType='" + passType + '\'' +
                ", modifyTime=" + modifyTime +
                ", passTime=" + passTime +
                ", plateNo=" + plateNo +
                ", RoadId=" + RoadId +
                ", userName='" + userName + '\'' +
                ", feeType='" + feeType + '\'' +
                ", VKindNo=" + VKindNo +
                ", plateColor='" + plateColor + '\'' +
                '}';
    }
}
