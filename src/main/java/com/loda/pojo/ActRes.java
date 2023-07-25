package com.loda.pojo;

import java.util.Objects;

/**
 * @Author loda
 * @Date 2023/7/10 11:16
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class ActRes {
    Integer actId;
    Integer userCnt;
    Integer amountCnt;

    public ActRes(Integer actId, Integer userCnt, Integer amountCnt) {
        this.actId = actId;
        this.userCnt = userCnt;
        this.amountCnt = amountCnt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ActRes)) return false;
        ActRes actRes = (ActRes) o;
        return Objects.equals(actId, actRes.actId) && Objects.equals(userCnt, actRes.userCnt) && Objects.equals(amountCnt, actRes.amountCnt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(actId, userCnt, amountCnt);
    }

    @Override
    public String toString() {
        return "ActRes{" +
                "actId=" + actId +
                ", userCnt=" + userCnt +
                ", amountCnt=" + amountCnt +
                '}';
    }

    public Integer getActId() {
        return actId;
    }

    public void setActId(Integer actId) {
        this.actId = actId;
    }

    public Integer getUserCnt() {
        return userCnt;
    }

    public void setUserCnt(Integer userCnt) {
        this.userCnt = userCnt;
    }

    public Integer getAmountCnt() {
        return amountCnt;
    }

    public void setAmountCnt(Integer amountCnt) {
        this.amountCnt = amountCnt;
    }

    public ActRes() {
    }
}
