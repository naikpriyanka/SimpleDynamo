package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;

import edu.buffalo.cse.cse486586.simpledynamo.data.SimpleDynamoDbHelper;
import edu.buffalo.cse.cse486586.simpledynamo.model.Message;
import edu.buffalo.cse.cse486586.simpledynamo.model.MessageType;

import static android.content.Context.TELEPHONY_SERVICE;
import static edu.buffalo.cse.cse486586.simpledynamo.data.SimpleDynamoContract.BASE_CONTENT_URI;
import static edu.buffalo.cse.cse486586.simpledynamo.data.SimpleDynamoContract.SimpleDhtEntry.KEY_FIELD;
import static edu.buffalo.cse.cse486586.simpledynamo.data.SimpleDynamoContract.SimpleDhtEntry.TABLE_NAME;
import static edu.buffalo.cse.cse486586.simpledynamo.data.SimpleDynamoContract.SimpleDhtEntry.VALUE_FIELD;
import static edu.buffalo.cse.cse486586.simpledynamo.model.Message.DELIMITER;
import static edu.buffalo.cse.cse486586.simpledynamo.model.MessageType.DELETE;
import static edu.buffalo.cse.cse486586.simpledynamo.model.MessageType.DELETE_ALL;
import static edu.buffalo.cse.cse486586.simpledynamo.model.MessageType.INSERT;
import static edu.buffalo.cse.cse486586.simpledynamo.model.MessageType.QUERY;
import static edu.buffalo.cse.cse486586.simpledynamo.model.MessageType.QUERY_ALL;
import static edu.buffalo.cse.cse486586.simpledynamo.model.MessageType.RECOVER;
import static edu.buffalo.cse.cse486586.simpledynamo.model.MessageType.getEnumBy;

public class SimpleDynamoProvider extends ContentProvider {

    public static final String LOG_TAG = SimpleDynamoProvider.class.getSimpleName();

    private SimpleDynamoDbHelper mDbHelper;

    private ContentResolver mContentResolver;

    private static final int SERVER_PORT = 10000;
    private static final String LDUMP = "@"; //Specific AVD
    private static final String GDUMP = "*"; //All AVDs
    private String selfPort = null; //Used to store the port number of the AVD
    private String[] portTable = {"5562", "5556", "5554", "5558", "5560"}; //Storing ports in order
    private String[] hashTable; //Storing hash values of port

    //Make two columns for key and value
    private static final String COLUMN_NAMES[] = new String[]{KEY_FIELD, VALUE_FIELD};

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        int rowsDeleted = 0;

        // Get writeable database
        SQLiteDatabase database = mDbHelper.getWritableDatabase();
        if (LDUMP.equals(selection)) {
            rowsDeleted = database.delete(TABLE_NAME, null, null);
        } else if (GDUMP.equals(selection)) {
            //Delete all the msgs from each node, one at a time by connecting remotely
            Message msg = new Message(DELETE_ALL);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString());
        } else {
            try {
                String hashedkey = genHash(selection);
                String currentPort = getSuccessorFrom(hashedkey);
                if (selfPort.equals(currentPort)) {
                    deleteFromDB(selection, selectionArgs);
                }
                Message msg = new Message(DELETE);
                msg.setKey(selection);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString(), currentPort);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        return rowsDeleted;
    }

    @Override
    public String getType(Uri uri) {
        // COMPLETED Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // Check that the key is not null
        String key = values.getAsString(KEY_FIELD);
        if (key == null) {
            throw new IllegalArgumentException("Message requires a key");
        }

        // Check that the value is not null
        String value = values.getAsString(VALUE_FIELD);
        if (value == null) {
            throw new IllegalArgumentException("Message requires a value");
        }
        try {
            String hashedKey = genHash(key);
            String currentPort = getSuccessorFrom(hashedKey);
            Message msg = new Message(INSERT, key, value);
            if(selfPort.equals(currentPort)) {
                insertInDB(uri, values);
            }
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString(), currentPort);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return null;
    }

    private Uri insertInDB(Uri uri, ContentValues values) {
        /*
         * Reference: https://developer.android.com/reference/android/database/sqlite/SQLiteDatabase.html#insertWithOnConflict(java.lang.String,%20java.lang.String,%20android.content.ContentValues,%20int)
         */
        // Get writable database
        SQLiteDatabase database = mDbHelper.getWritableDatabase();

        long id = database.insertWithOnConflict(TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);

        // If the ID is -1, then the insertion failed. Log an error and return null.
        if (id == -1) {
            Log.e(LOG_TAG, "Failed to insert row for " + uri);
            return null;
        }
        // Notify all listeners that the data has changed for the message content URI
        mContentResolver.notifyChange(uri, null);
        // Return the new URI with the ID (of the newly inserted row) appended at the end
        return ContentUris.withAppendedId(uri, id);
    }

    private int deleteFromDB(String selection, String[] selectionArgs) {
        // Get writable database
        SQLiteDatabase database = mDbHelper.getWritableDatabase();
        selectionArgs = new String[]{selection};
        selection = KEY_FIELD + "=?";
        return database.delete(TABLE_NAME, selection, selectionArgs);
    }

    @Override
    public boolean onCreate() {
        Context context = this.getContext();
        mContentResolver = context.getContentResolver();
        mDbHelper = new SimpleDynamoDbHelper(context);
        initHashTable();

        //Calculate the port number that this AVD listens on
        TelephonyManager tel = (TelephonyManager) context.getSystemService(TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        //Store the current port in self port for future references
        selfPort = portStr;
        System.out.println("Self Port " + selfPort);

        try {
            /*
             * Create a server socket as well as a thread (AsyncTask) that listens on the server
             * port.
             * ServerSocket is a socket which servers can use to listen and accept requests from clients
             * AsyncTask is a simplified thread construct that Android provides.
             */
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(LOG_TAG, "Can't create a ServerSocket");
        }

//        SQLiteDatabase database = mDbHelper.getWritableDatabase();
//        int rowsDeleted = database.delete(TABLE_NAME, null, null);
//        database.close();
//        if (rowsDeleted != 0) {
//            Message msg = new Message(RECOVER);
//            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString(), portStr);
//        }
        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        // Get readable database
        SQLiteDatabase database = mDbHelper.getReadableDatabase();
        Cursor resultCursor;
        if (LDUMP.equals(selection)) {
            resultCursor = database.query(TABLE_NAME, null, null, null, null, null, null);
            return resultCursor;
        } else if (GDUMP.equals(selection)) {
            StringBuilder output = new StringBuilder();
            for (String remotePort : portTable) {
                try {
                    //Get the socket with the given port number
                    Socket socket = getSocket(getPortFromLineNumber(remotePort));
                    try {
                        //Create an output data stream to send QUERY message to all the nodes
                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        //Create a QUERY_ALL message to be sent to the nodes
                        Message msgToSend = new Message(QUERY_ALL);
                        //Write the message on the output stream
                        out.writeUTF(msgToSend.toString());
                        //Flush the output stream
                        out.flush();
                    } catch (IOException e) {
                        Log.e(LOG_TAG, "Error writing data to output stream in QUERY_ALL for port " + remotePort);
                    }
                    try {
                        //Create an input data stream to read messages from all the nodes
                        DataInputStream in = new DataInputStream(socket.getInputStream());
                        //Read the message received
                        String msgReceived = in.readUTF();
                        //Append it to the output
                        output.append(msgReceived);
                    } catch (IOException e) {
                        Log.e(LOG_TAG, "Error reading data from input stream in QUERY_ALL for port " + remotePort);
                    }
                    socket.close();
                } catch (IOException e) {
                    Log.e(LOG_TAG, "Error in socket creation " + remotePort);
                }
            }
            //Get messages as map of key and value from the output returned from all the nodes
            Map<String, String> allMessages = getAllMessages(output.toString());
            /*
             * Reference: https://developer.android.com/reference/android/database/MatrixCursor.html
             *
             * Create Matrix Cursor for placing all the messages
             */
            MatrixCursor matrixCursor = new MatrixCursor(COLUMN_NAMES);
            //Iterate over all the messages and add row for each entry
            for (Map.Entry<String, String> entry : allMessages.entrySet()) {
                //Create key value pair
                String keyValue[] = new String[]{entry.getKey(), entry.getValue()};
                //Add that pair to the matrix cursor
                matrixCursor.addRow(keyValue);
            }
            return matrixCursor;
        } else {
            try {
                String hashedkey = genHash(selection);
                String currentPort = getSuccessorFrom(hashedkey);
                if (selfPort.equals(currentPort)) {
                    // Query selection key from the database
                    selectionArgs = new String[]{selection};
                    selection = KEY_FIELD + "=?";
                    resultCursor = database.query(TABLE_NAME, projection, selection, selectionArgs, null, null, sortOrder);
                    resultCursor.setNotificationUri(mContentResolver, uri);
                    return resultCursor;
                } else {
                    try {
                        //Get the socket with the given port number
                        Socket socket = getSocket(getPortFromLineNumber(currentPort));
                        try {
                            //Create an output data stream to send QUERY message to a particular node
                            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                            //Create a QUERY message to be sent to that node
                            Message msgToSend = new Message(QUERY);
                            //Set the selection as key in the msg
                            msgToSend.setKey(selection);
                            //Write the message on the output stream
                            out.writeUTF(msgToSend.toString());
                            //Flush the output stream
                            out.flush();
                        } catch (IOException e) {
                            Log.e(LOG_TAG, "Error writing data to output stream in QUERY for port " + currentPort);
                        }

                        String succ[] = getSuccessorOf(currentPort);
                        String msgReceived;
                        try {
                            //Create an input data stream to read messages from a particular node
                            DataInputStream in = new DataInputStream(socket.getInputStream());
                            try {
                                //Read the message received
                                msgReceived = in.readUTF();
                                if (msgReceived.isEmpty()) {
                                    msgReceived = queryReplicas(succ[0], selection, succ[1]);
                                }
                            } catch (Exception e) {
                                msgReceived = queryReplicas(succ[0], selection, currentPort);
                            }
                            //Create Matrix Cursor for placing all the messages from that node
                            MatrixCursor matrixCursor = new MatrixCursor(COLUMN_NAMES);
                            //Create key value pair
                            String keyValue[] = {selection, msgReceived};
                            //Add that pair to the matrix cursor
                            matrixCursor.addRow(keyValue);
                            return matrixCursor;
                        } catch (IOException e) {
                            Log.e(LOG_TAG, "Error reading data from input stream in QUERY for port " + currentPort);
                        }
                        socket.close();
                    } catch (IOException e) {
                        Log.e(LOG_TAG, "Error in socket creation" + currentPort);
                    }
                }
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // COMPLETED Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            /*
             * an iterative server that can service multiple clients, though, one at a time.
             */
            while (true) {
                try {
                    if (serverSocket != null) {
                        Socket server = serverSocket.accept();
                        DataInputStream in = new DataInputStream(new BufferedInputStream(server.getInputStream()));
                        //Get message from the input stream
                        String msgReceived = in.readUTF();
                        String[] msgPacket = msgReceived.split(DELIMITER);
                        //Get the message type from the message string
                        MessageType msgType = getEnumBy(msgPacket[0]);
                        if (msgType != null) {
                            switch (msgType) {
                                case INSERT:
                                    System.out.println("INSERT " + msgReceived);
                                    //Insert the key and the value in the database
                                    ContentValues values = new ContentValues();
                                    values.put(KEY_FIELD, msgPacket[1]);
                                    values.put(VALUE_FIELD, msgPacket[2]);
                                    insertInDB(BASE_CONTENT_URI, values);
//                                    mContentResolver.insert(BASE_CONTENT_URI, values);
                                    break;

                                case QUERY:
                                    SQLiteDatabase database = mDbHelper.getReadableDatabase();
                                    String selection = msgPacket[1];
                                    //Create an output data stream to send retrieved QUERY message back to the querying node
                                    DataOutputStream out = new DataOutputStream(server.getOutputStream());
                                    //Query the database with the selection key
                                    String[] selectionArgs = new String[]{selection};
                                    selection = KEY_FIELD + "=?";
                                    Cursor cursor = database.query(TABLE_NAME, null, selection, selectionArgs, null, null, null);
                                    String value = null;
                                    /*
                                     * Move the cursor to the first record
                                     *
                                     * Reference: https://stackoverflow.com/questions/10723770/whats-the-best-way-to-iterate-an-android-cursor
                                     */
                                    if (cursor.moveToFirst()) {
                                        //Get the value from the column
                                        value = cursor.getString(cursor.getColumnIndex(VALUE_FIELD));
                                    }
                                    out.writeUTF(value);
                                    out.flush();
                                    cursor.close();
                                    database.close();
                                    break;

                                case QUERY_ALL:
                                    SQLiteDatabase sqlDatabase = mDbHelper.getReadableDatabase();
                                    //Create an output data stream to send retrieved QUERY_ALL message back to the querying node
                                    DataOutputStream out1 = new DataOutputStream(server.getOutputStream());
                                    //Query the database to get all the results on that node
                                    Cursor resultCursor = sqlDatabase.query(TABLE_NAME, null, null, null, null, null, null);
                                    /*
                                     * Reference: https://developer.android.com/reference/android/database/MatrixCursor.html#getCount()
                                     *
                                     * Get column count
                                     */
                                    int numColumns = resultCursor.getColumnCount();
                                    StringBuilder output = new StringBuilder();
                                    /*
                                     * Move the cursor to the -1 position
                                     * In order to iterate over the result cursor returned from the sql database query
                                     * I move the cursor with -1 position so that I can directly use the function moveToNext
                                     * in the while loop
                                     *
                                     * Reference: https://developer.android.com/reference/android/database/Cursor.html
                                     * Reference: https://stackoverflow.com/questions/10723770/whats-the-best-way-to-iterate-an-android-cursor
                                     */
                                    resultCursor.moveToPosition(-1);
                                    //Iterate over all the values in the cursor
                                    while (resultCursor.moveToNext()) {
                                        for (int i = 0; i < numColumns; i++) {
                                            output.append(resultCursor.getString(i)).append(DELIMITER);
                                        }
                                    }
                                    out1.writeUTF(output.toString());
                                    out1.flush();
                                    resultCursor.close();
                                    sqlDatabase.close();
                                    break;

                                case DELETE:
                                    deleteFromDB(msgPacket[1], null);
//                                    mContentResolver.delete(BASE_CONTENT_URI, msgPacket[1], null);
                                    break;

                                case DELETE_ALL:
                                    deleteFromDB(null, null);
//                                    mContentResolver.delete(BASE_CONTENT_URI, null, null);
                                    break;

                                default:
                                    throw new IllegalArgumentException("Unknown Message type" + msgType);
                            }
                        } else {
                            throw new IllegalArgumentException("Message type is null");
                        }
                    } else {
                        Log.e(LOG_TAG, "The server socket is null");
                    }
                } catch (IOException e) {
                    Log.e(LOG_TAG, "Error accepting socket" + e);
                }
            }
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            String[] msgPacket = msgs[0].split(DELIMITER);
            MessageType msgType = getEnumBy(msgPacket[0]);
            if (msgType != null) {
                switch (msgType) {
                    case RECOVER:
                        recover(msgs[1]);
                        break;

                    case INSERT:
                        String remotePort = msgs[1];
                        String[] succ = getSuccessorOf(remotePort);
                        System.out.println("INSERT " + msgs[0] + " " + remotePort);
                        if(!selfPort.equals(remotePort)) {
                            insertTo(msgPacket, remotePort);
                        }
                        for (String succPort : succ) {
                            insertTo(msgPacket, succPort);
                        }
                        break;

                    case DELETE:
                        String remotePort1 = msgs[1];
                        String[] succ1 = getSuccessorOf(remotePort1);
                        if(!selfPort.equals(remotePort1)) {
                            deletionFrom(msgPacket[1], remotePort1);
                        }
                        for (String succPort : succ1) {
                            deletionFrom(msgPacket[1], succPort);
                        }
                        break;

                    case DELETE_ALL:
                        for (String remotePort2 : portTable) {
                            deleteAllFrom(remotePort2);
                        }
                        break;

                    default:
                        throw new IllegalArgumentException("Unknown Message type" + msgType);

                }
            } else {
                throw new IllegalArgumentException("Message type is null");
            }
            return null;
        }
    }

    private void deleteAllFrom(String remotePort2) {
        try {
            //Get the socket with the given port number
            Socket socket = getSocket(getPortFromLineNumber(remotePort2));
            //Create an output data stream to send DELETE_ALL message to a particular node
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            //Create DELETE msg with key to delete all the message from all the nodes node
            Message msgToSend = new Message(DELETE_ALL);
            //Write the msg on the output stream
            out.writeUTF(msgToSend.toString());
            //Flush the output stream
            out.flush();
            socket.close();
        } catch (IOException e) {
            Log.e(LOG_TAG, "Error in writing DELETE_ALL on port " + remotePort2);
        }
    }

    private void insertTo(String[] msgPacket, String remotePort) {
        try {
            //Get the socket with the given port number
            Socket socket = getSocket(getPortFromLineNumber(remotePort));
            //Create an output data stream to send INSERT message to a particular node
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            //Create INSERT msg with key and value
            Message msgToSend = new Message(INSERT, msgPacket[1], msgPacket[2]);
            //Write msg on the output stream
            out.writeUTF(msgToSend.toString());
            //Flush the output stream
            out.flush();
            socket.close();
        } catch (IOException e) {
            Log.e(LOG_TAG, "Error in writing INSERT on port " + remotePort + e);
        }
    }

    private void deletionFrom(String key, String succPort) {
        try {
            //Get the socket with the given port number
            Socket socket = getSocket(getPortFromLineNumber(succPort));
            //Create an output data stream to send DELETE message to a particular node
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            //Create DELETE msg with key to delete all the message from that node
            Message msgToSend = new Message(DELETE);
            msgToSend.setKey(key);
            //Write the msg on the output stream
            out.writeUTF(msgToSend.toString());
            //Flush the output stream
            out.flush();
            socket.close();
        } catch (IOException e) {
            Log.e(LOG_TAG, "Error in writing DELETE on port " + succPort);
        }
    }

    private synchronized void recover(String port) {
        String succ[] = getSuccessorOf(port);
        String pred[] = getPredecessorOf(port);
        String[] recoveryPorts = {succ[0], pred[0], pred[1]};
        Map<String, String> map = new HashMap<String, String>();
        for (String remotePort : recoveryPorts) {
            try {
                Socket socket = getSocket(getPortFromLineNumber(remotePort));
                Message msgToSend = new Message(QUERY_ALL);
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                out.writeUTF(msgToSend.toString());
                out.flush();

                DataInputStream in = new DataInputStream(socket.getInputStream());
                String op = in.readUTF();
                map.putAll(getAllMessages(op));
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        for (Map.Entry<String, String> m : map.entrySet()) {
            String key = m.getKey();
            String value = m.getValue();
            try {
                String hashedkey = genHash(key);
                String tport = getSuccessorFrom(hashedkey);
                if (tport.equals(port) || tport.equals(pred[0]) || tport.equals(pred[1])) {
                    ContentValues values = new ContentValues();
                    values.put(KEY_FIELD, key);
                    values.put(VALUE_FIELD, value);
                    mContentResolver.insert(BASE_CONTENT_URI, values);
                }
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
    }

    private void display(Map<String, String> smap) {
        for (Map.Entry<String, String> m : smap.entrySet()) {
            System.out.println("Map values : " + m.getKey() + " " + m.getValue());
        }
        System.out.println("----------------------------------------------------------------");
    }

    //Method to create socket for the given port
    private Socket getSocket(String remotePort) throws IOException {
        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
        socket.setTcpNoDelay(true);
        socket.setSoTimeout(1000);
        return socket;
    }

    //Method to get the TCP port from the Line Number
    private String getPortFromLineNumber(String portStr) {
        return String.valueOf(Integer.parseInt(portStr) * 2);
    }

    private void initHashTable() {
        hashTable = new String[portTable.length];
        for (int i = 0; i < portTable.length; i++) {
            try {
                hashTable[i] = genHash(portTable[i]);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
    }

    private String getSuccessorFrom(String hashKey) {
        for (int i = 0; i < portTable.length; i++) {
            if (hashTable[i].compareTo(hashKey) > 0) { //Compare the hash values to get the successor port
                return portTable[i];
            }
        }
        /*
         * If the value greater than the hash is not found
         * then either the key hash greater than hash of last node in the ring
         * or the key hash less than hash of first node in the ring
         */
        return portTable[0];
    }

    //Method to separate out key and value from the message
    private Map<String, String> getAllMessages(String s) {
        System.out.println("Get All Messages " + s);
        Map<String, String> messages = new HashMap<String, String>();
        String[] msgPackets = s.split(DELIMITER);
        if (msgPackets.length >= 2) {
            for (int i = 0; i < msgPackets.length; i += 2) {
                messages.put(msgPackets[i], msgPackets[i + 1]);
            }
        }
        return messages;
    }

    private String[] getSuccessorOf(String port) {
        int index = search(port);
        String succ[] = new String[2];
        if (index != -1) {
            succ[0] = portTable[(index + 1) % portTable.length];
            succ[1] = portTable[(index + 2) % portTable.length];
        }
        return succ;
    }

    private int search(String port) {
        for (int i = 0; i < portTable.length; i++) {
            if (portTable[i].equals(port)) {
                return i;
            }
        }
        return -1;
    }

    private String[] getPredecessorOf(String port) {
        String pred[] = new String[2];
        int index = search(port);
        if (index != -1) {
            pred[0] = portTable[(index + portTable.length - 1) % portTable.length];
            pred[1] = portTable[(index + portTable.length - 2) % portTable.length];
        }
        return pred;
    }

    private String queryReplicas(String replicaPort1, String selection, String replicaPort2) {
        String output = null;
        try {
            Socket socket = getSocket(getPortFromLineNumber(replicaPort1));
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            Message msgToSend = new Message(QUERY);
            msgToSend.setKey(selection);
            out.writeUTF(msgToSend.toString());
            out.flush();

            DataInputStream in = new DataInputStream(socket.getInputStream());
            try {
                //Read the message received
                output = in.readUTF();
                if (output.isEmpty()) {
                    output = queryReplicas(replicaPort2, selection, replicaPort2);
                }
            } catch (Exception e) {
                output = queryReplicas(replicaPort2, selection, replicaPort2);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return output;
    }
}

