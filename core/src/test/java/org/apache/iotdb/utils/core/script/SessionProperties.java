package org.apache.iotdb.utils.core.script;

import java.util.ResourceBundle;

public class SessionProperties {

    public String host = "127.0.0.1";
    public int port = 6667;
    public String username = "root";
    public String password = "root";

    public SessionProperties(){
        ResourceBundle resourceBundle = ResourceBundle.getBundle("sessionConfig");
        host = resourceBundle.getString("iotdbutils.core.session.host");
        port = Integer.parseInt(resourceBundle.getString("iotdbutils.core.session.port"));
        username = resourceBundle.getString("iotdbutils.core.session.username");
        password = resourceBundle.getString("iotdbutils.core.session.password");
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
