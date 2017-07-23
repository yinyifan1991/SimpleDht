package edu.buffalo.cse.cse486586.simpledht;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final String[] port = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
    static final int SERVER_PORT = 10000;
    static final String JOIN_PORT = "11108";
    static final String key = "key";
    static final String valueCol = "value";

    static String nodeId;
    static String mPredId;
    static String mSuccId;
    static String mPredPort;
    static String mSuccPort;
    static String myPort;

    private static final int JOIN = 0;
    private static final int ACCEPT = 1;
    private static final int CHANGE = 2;
    private static final int INSERT = 3;
    private static final int DELETE = 4;
    private static final int QUERY = 5;
    private static final int RETURNQUERY = 6;
    private static final int ALLEND = 7;

    private  static boolean wait;
    private static String returnKey = "";
    private static String returnValue = "";

    Map<String, String> storage = new HashMap<String, String>();
    Map<String, String> all = new HashMap<String, String>();

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        delete(selection, myPort);
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        insert(values.getAsString(key), values.getAsString(valueCol));
        return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));


        try {
            nodeId = genHash(portStr);
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "can't generate hash id for this node");
            e.printStackTrace();
        }

        mPredId = nodeId;
        mSuccId = nodeId;
        mPredPort = myPort;
        mSuccPort = myPort;

        wait = false;

        try {
            ServerSocket serverS = new ServerSocket(SERVER_PORT);
            serverThread(serverS);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Log.v("myPort", myPort);
        if (!myPort.equals(JOIN_PORT)) {
            Log.v("clientThread invoke1", "1");
            clientThread(JOIN_PORT, nodeId, myPort, null, null, null, JOIN, null);
            Log.v("JOIN sent", myPort + "_" + nodeId);

        }

        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub
        Log.v("start query key = ", selection);
        Cursor cursor = query(selection, null, myPort);
        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
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

    private void serverThread(final ServerSocket socket) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                ServerSocket server = socket;
                Log.v("serverThread start", "");
                try {
                    while (true) {
                        Socket clientSocket = server.accept();
                        ObjectInputStream objIS = new ObjectInputStream(clientSocket.getInputStream());
                        Message message = (Message)objIS.readObject();
                        if(message.type == JOIN) {
                            Log.v("receive JOIN", message.nodeId);
                            join(message);
                        }
                        else if(message.type == ACCEPT) {
                            Log.v("receive ACCEPT", "PredId = " + mPredId + " " + "SuccId = " + mSuccId);
                            mPredId = message.predId;
                            mSuccId = message.succId;
                            mPredPort = message.predPort;
                            mSuccPort = message.succPort;
                            Log.v("after accept", mPredId + "_" + nodeId + "_" + mSuccId);

                        }
                        else if(message.type == CHANGE) {
                            Log.v("receive CHANGE", "SuccId = " + mSuccId + " " + "SuccPort = " + mSuccPort);
                            mSuccId = message.succId;
                            mSuccPort = message.succPort;
                            Log.v("after change", mPredId + "_" + nodeId + "_" + mSuccId);
                        }
                        else if(message.type == INSERT) {
                            Log.v("receive INSERT", message.nodeId);
                            insert(message.nodeId, message.predPort);
                        }
                        else if(message.type == DELETE) {
                            Log.v("receive DELETE", message.nodeId);
                            delete(message.nodeId, message.predPort);
                        }
                        else if(message.type == QUERY) {
                            if(message.nodeId.equals("*")) {
                                all.clear();
                                all.putAll(message.map);
                            }
                            Log.v("receive QUERY", "5");
                            query(message.nodeId, null, message.succPort);
                        }
                        else if(message.type == RETURNQUERY) {

                            returnKey = message.nodeId;
                            returnValue = message.predPort;
                            Log.v("receive RETURNQUERY", returnKey + "_" + returnValue);
                            wait = false;
                        }
                        else if(message.type == ALLEND) {
                            Log.v("receive ALLEND", "7");
                            all.clear();
                            all.putAll(message.map);
                            wait = false;
                        }
                        clientSocket.close();
                    }
                } catch (IOException e) {
                    Log.e(TAG, "ServerSocket IOException");
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    private void clientThread(final String port, final String nodeId, final String predPort, final String succPort, final String predId, final String succId, final int type, final Map<String, String> all) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    String remotePort = port;
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));
                    Message message = new Message(predPort, nodeId, predPort, succPort, predId, succId, type, all);
                    ObjectOutputStream objOS = new ObjectOutputStream(socket.getOutputStream());
                    objOS.writeObject(message);
                    Log.v("client write", message.nodeId + "_" + predPort + "_" + type);
                    Thread.sleep(10);
                    socket.close();
                } catch (IOException e) {
                    if(type == QUERY)
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    private boolean blToThisNode(String id) {
        BigInteger nid = new BigInteger(id, 16);
        BigInteger mpi = new BigInteger(mPredId, 16);
        BigInteger mid = new BigInteger(nodeId, 16);
/*
        int nid = Integer.parseInt(id, 16);
        int mpi = Integer.parseInt(mPredId, 16);
        int mid = Integer.parseInt(nodeId, 16);
*/
        Log.v("blToThisNode", nid.toString() + "_" + mpi.toString() + "_" + mid.toString());
        if(mpi.compareTo(mid)!= 0) {
            if (nid.compareTo(mpi) == 1 && nid.compareTo(mid) != 1) {
                Log.v("blToThisNode", "btween true");
                return true;
            }
            else if(mid.compareTo(mpi) == -1 && (nid.compareTo(mpi) == 1 || nid.compareTo(mid) == -1)) {
                Log.v("blToThisNode", "btween -1 true");
                return true;
            }
        }
        else {
            Log.v("blToThisNode", "mpi = mid true");
            return true;
        }
        Log.v("blToThisNode", "false");
        return false;
    }

    private void join(Message message) {
        if(blToThisNode(message.nodeId)) {
            Log.v("clientThread invoke2", "2");
            clientThread(message.port, message.nodeId, mPredPort, myPort, mPredId, nodeId, ACCEPT, null);

            if(!nodeId.equals(mPredId)) {
                Log.v("clientThread invoke3", "3");
                clientThread(mPredPort, message.nodeId, null, message.port, null, message.nodeId, CHANGE, null);


            }
            else {
                mSuccId = message.nodeId;
                mSuccPort = message.port;
            }
            mPredId = message.nodeId;
            mPredPort = message.port;
            Log.v("after join", mPredId + "_" + nodeId + "_" + mSuccId);
        }
        else {
            Log.v("clientThread invoke4", "4");
            Log.v("SuccPort", mSuccPort);
            clientThread(mSuccPort, message.nodeId, message.port, null, null, null, JOIN, null);

        }
    }

    private void insert(String key, String value) {
        try {
            String hashKey = genHash(key);
            Log.v("insert hashKey", hashKey);
            if(blToThisNode(hashKey)) {
                Log.v("insert to origin node", key + "_" + value);
                storage.put(key, value);
            }
            else if(!mSuccPort.equals(myPort)) {
                Log.v("clientThread invoke5", "5");
                clientThread(mSuccPort, key, value, null, null, null, INSERT, null);

                Log.v("insert", "find succ to insert");
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    private void delete(String selection, String sourcePort) {
        if (selection.equals("*")) {
            Log.v("delete *", "");
            if(!storage.isEmpty())
                storage.clear();
            if(mSuccPort != sourcePort) {
                Log.v("delete *", "have succ to delete");
                Log.v("clientThread invoke6", "6");
                clientThread(mSuccPort, selection, sourcePort, null, null, null, DELETE, null);

            }
        }
        else if(selection.equals("@") && !storage.isEmpty()) {
            Log.v("delete @", "");
            storage.clear();
        }
        else {

            Log.v("normal delete", "");
            try {
                String hashDelete = genHash(selection);
                if(blToThisNode(hashDelete)) {
                    if(storage.containsKey(selection)) {
                        Log.v("normal delete remove", selection);
                        storage.remove(selection);
                    }
                    else
                        Log.v("normal delete", "blong but nothing to delete");
                }
                else if(!mSuccPort.equals(myPort)){
                    Log.v("find node to delete", "nomal");
                    Log.v("clientThread invoke7", "7");
                    clientThread(mSuccPort, selection, sourcePort, null, null, null, DELETE, null);

                }
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
    }

    private  Cursor query(String selection, String value, String sourcePort) {
        MatrixCursor cur = new MatrixCursor(new String[]{key, valueCol});

        if (selection.equals("*")) {
            if(myPort.equals(sourcePort)) {
                Log.v("* query", "sourcePort");
                all.putAll(storage);
                if(!mSuccPort.equals(myPort)) {
                    Log.v("clientThread invoke8", "8");
                    clientThread(mSuccPort, selection, null, sourcePort, null, null, QUERY, all);

                    wait = true;
                    while (wait) {}
                    Log.v("* query", "wait canceled");

                }
                for(Map.Entry<String, String> entry: all.entrySet()) {
                    cur.addRow(new String[]{entry.getKey(), entry.getValue()});
                    Log.v("all end query", entry.getKey() + "_" + entry.getValue());
                }
            }
            else if(!mSuccPort.equals(sourcePort)) {
                Log.v("* query", "meddle Port");
                all.putAll(storage);
                Log.v("clientThread invoke9", "9");
                clientThread(mSuccPort, selection, null, sourcePort, null, null, QUERY, all);

            }
            else if(mSuccPort.equals(sourcePort)) {
                Log.v("* query", "final Port");
                all.putAll(storage);
                Log.v("clientThread invoke10", "10");
                clientThread(mSuccPort, selection, null, sourcePort, null, null, ALLEND, all);

            }

        } else if (selection.equals("@")) {
            if(sourcePort.equals(myPort)) {
                Log.v("@ query", "sourcePort");
                for (Map.Entry<String, String> entry : storage.entrySet()) {
                    cur.addRow(new String[]{entry.getKey(), entry.getValue()});
                }
            }
        } else {
            Log.v("normal query", selection);
            try {
                String hashQuery = genHash(selection);
                if (blToThisNode(hashQuery)) {
                    if(sourcePort.equals(myPort) && storage.containsKey(selection)) {
                        Log.v("normal query in source", selection);
                        Log.v("query value", storage.get(selection));
                        cur.addRow(new String[]{selection, storage.get(selection)});
                    }
                    else if(storage.containsKey(selection)) {
                        Log.v("query not in source", "");
                        Log.v("clientThread invoke11", "11");
                        clientThread(sourcePort, selection, storage.get(selection), sourcePort, null, null, RETURNQUERY, null);
                        Log.v("RETURNQUERY", selection + "_" + storage.get(selection));

                    }
                } else if(!mSuccPort.equals(myPort)){
                    Log.v("find node to query", selection);
                    Log.v("clientThread invoke12", "12");
                    clientThread(mSuccPort, selection, null, sourcePort, null, null, QUERY, null);

                    if(myPort.equals(sourcePort)) {
                        Log.v("find node to query", "source wait");
                        wait = true;
                        while (wait) {}
                        cur.addRow((new String[]{returnKey, returnValue}));
                    }
                }
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }

        return cur;
    }
}
