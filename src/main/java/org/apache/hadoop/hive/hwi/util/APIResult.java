package org.apache.hadoop.hive.hwi.util;

import com.google.gson.Gson;
import java.util.HashMap;

public class APIResult {
	
    private String result;
	
    private String msg;
    
    private Integer id;
	
    public static final String ERR = "error";
	
    public static final String OK = "ok";
	
    public APIResult (String ret, String message) {
        result = ret;
        msg = message;
    }

    public APIResult (String ret, String message, Integer id) {
        result = ret;
        msg = message;
        this.id = id;
    }
    
    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
	
    public String toJson() {
        HashMap<String, Object> map = new HashMap<String, Object>();
        map.put("result", this.result);
        map.put("msg", this.msg);
        if (this.result.equals(OK)) {
            map.put("id", this.id);
        }
        Gson gson = new Gson();
        String json = gson.toJson(map);	
        return json;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

}
