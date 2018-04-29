package edu.buffalo.cse.cse486586.simpledynamo.model;

import static edu.buffalo.cse.cse486586.simpledynamo.model.MessageType.DELETE;
import static edu.buffalo.cse.cse486586.simpledynamo.model.MessageType.DELETE_ALL;
import static edu.buffalo.cse.cse486586.simpledynamo.model.MessageType.INSERT;
import static edu.buffalo.cse.cse486586.simpledynamo.model.MessageType.QUERY;
import static edu.buffalo.cse.cse486586.simpledynamo.model.MessageType.QUERY_ALL;
import static edu.buffalo.cse.cse486586.simpledynamo.model.MessageType.RECOVER;

/**
 * Created by priyankanaik on 03/03/2018.
 *
 *
 * This class is used to store the messages using the Message type and other fields
 */
public class Message {

    //Used to separate fields of the message
    public static final String DELIMITER = ":";

    //Used to store the message type
    private MessageType type;

    //Used to store the message key
    private String key;

    //Used to store the message value
    private String value;

    //Used to store the port where the message is stored
    private String port;

    //This constructor is used for JOIN, QUERY, DELETE, DELETE_ALL, QUERY_ALL type message
    public Message(MessageType type) {
        this.type = type;
    }

    //This constructor is used for INSERT message type
    public Message(MessageType type, String key, String value) {
        this.type = type;
        this.key = key;
        this.value = value;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setPort(String port) {
        this.port = port;
    }

    /*
     * Overrided this method by separating required field by DELIMITER
     * The message string is decided by the type of the message
     */
    @Override
    public String toString() {
        if(type == RECOVER || type == DELETE_ALL || type == QUERY_ALL) {
            return type.toString();
        } else if(type == INSERT) {
            return type + DELIMITER + key + DELIMITER + value;
        } else if(type == DELETE || type == QUERY) {
            return type + DELIMITER + key;
        }
        return type + ", " + key + ", " + value + ", " + port;
    }
}
