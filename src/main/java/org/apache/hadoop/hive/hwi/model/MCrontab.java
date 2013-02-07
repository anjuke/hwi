/**
 * Copyright (C) [2013] [Anjuke Inc]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.hwi.model;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

@PersistenceCapable
public class MCrontab {

    public static enum Status {
        RUNNING, PAUSED, DELETED
    };
    
    @PrimaryKey
    @Persistent(valueStrategy=IdGeneratorStrategy.IDENTITY)
    private Integer id;

    private String name;

    private String query;

    private String callback;

    private String userId;

    private String groupId;

    private String crontab;

    private Date created;

    private Date updated;

    private Status status;

    public MCrontab(String name, String query, String callback, String crontab,
            String userId, String groupId) {
        this.name = name;
        this.query = query;
        this.callback = callback;
        this.crontab = crontab;
        this.userId = userId;
        this.groupId = groupId;
        this.created = Calendar.getInstance(TimeZone.getDefault()).getTime();
        this.updated = this.created;
        this.setStatus(Status.PAUSED);
    }

    public void copy(MCrontab mcrontab) {
        name = mcrontab.name;
        query = mcrontab.query;
        callback = mcrontab.callback;
        crontab = mcrontab.crontab;
        created = mcrontab.created;
        updated = mcrontab.updated;
        status = mcrontab.status;
        userId = mcrontab.userId;
        groupId = mcrontab.groupId;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getCallback() {
        return callback;
    }

    public void setCallback(String callback) {
        this.callback = callback;
    }

    public String getCrontab() {
        return crontab;
    }

    public void setCrontab(String crontab) {
        this.crontab = crontab;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public Date getCreated() {
        return created;
    }

    public void setCreated(Date created) {
        this.created = created;
    }

    public Date getUpdated() {
        return updated;
    }

    public void setUpdated(Date updated) {
        this.updated = updated;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

}
