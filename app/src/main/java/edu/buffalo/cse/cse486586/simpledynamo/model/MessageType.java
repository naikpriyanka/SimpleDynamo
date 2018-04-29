package edu.buffalo.cse.cse486586.simpledynamo.model;

/**
 * Created by priyankanaik on 04/03/2018.
 *
 * Used to store the message type
 */
public enum MessageType {

    RECOVER("RECOVER"),
    INSERT("INSERT"),
    DELETE("DELETE"),
    DELETE_ALL("DELETE_ALL"),
    QUERY("QUERY"),
    QUERY_ALL("QUERY_ALL");

    private final String type;

    MessageType(String type) {
        this.type = type;
    }

    //Used to get the enum values from the string
    public static MessageType getEnumBy(String type){
        for(MessageType m : MessageType.values()){
            if(m.type.equals(type)) return m;
        }
        return null;
    }

}
