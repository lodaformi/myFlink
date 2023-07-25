package com.loda.pojo;

import java.util.Objects;

/**
 * @Author loda
 * @Date 2023/7/10 8:51
 * @Description TODO(一句话描述该类的功能)
 * @Version 1.0
 */
public class Activity {
    Integer actId;
    Integer userId;
    Integer amount;

    public Activity(Integer actId) {
        this.actId = actId;
    }

    public Activity(Integer actId, Integer userId, Integer amount) {
        this.actId = actId;
        this.userId = userId;
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "Activity{" +
                "actId=" + actId +
                ", userId=" + userId +
                ", amount=" + amount +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Activity)) return false;
        Activity activity = (Activity) o;
        return Objects.equals(actId, activity.actId) && Objects.equals(userId, activity.userId) && Objects.equals(amount, activity.amount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(actId, userId, amount);
    }

    public Integer getActId() {
        return actId;
    }

    public void setActId(Integer actId) {
        this.actId = actId;
    }

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    public Integer getAmount() {
        return amount;
    }

    public void setAmount(Integer amount) {
        this.amount = amount;
    }
}
